//go:build windows
// +build windows

package fsbroker

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func (b *FSBroker) resolveAndHandle(eventQueue *EventQueue, tickerLock *sync.Mutex) {
	// We only want one instance of this method to run at a time, so we lock it
	if !tickerLock.TryLock() {
		return
	}
	defer tickerLock.Unlock()

	// Process grouped events, detecting related Create and Rename events
	processedPaths := make(map[string]bool)

	// temporary list of events to process
	eventList := eventQueue.List()

	if len(eventList) == 0 {
		return
	}

	//Now we process the events remaining in the event queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		// Ignore already processed paths
		if processedPaths[action.Signature()] {
			continue
		}

		switch action.Type {
		case Remove:
			// Check if there's any earlier event for the same path within the queue, ignore it and only raise the remove event
			for _, relatedAction := range eventList {
				if relatedAction.Path == action.Path && relatedAction.Timestamp.Before(action.Timestamp) {
					processedPaths[relatedAction.Signature()] = true
				}
			}

			// On Windows, a rename causes a remove event (with the old name) followed by a create event (with the new name)
			// So we have to check our watchmap to get infomation about the file that was deleted
			// Then we compare the Identifier of the deleted file with identifiers of created files in the eventList
			// If we find a match, we ignore the remove and create events, and only raise the Rename event
			// Otherwise, we raise the remove event

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			// Note: deletedFile might be nil if the remove event is for a file
			// that was created and removed within the same processing window before
			// it was added to the watchmap by its Create event handler.

			var isRename bool = false
			var idMismatchSuspected bool = false // Flag for potential ID mismatch

			if runtime.GOOS == "windows" {
				// Check if there's a create event for the same file identifier within queue
				for _, relatedAction := range eventList {
					if relatedAction.Type == Create && relatedAction.Timestamp.After(action.Timestamp) {
						stat, err := os.Stat(relatedAction.Path)
						if err != nil {
							continue // Can't stat the potential new file
						}
						relatedFile := FromOSInfo(relatedAction.Path, stat)
						// Check if deletedFile or relatedFile info is missing OR if IDs mismatch
						if relatedFile == nil || deletedFile == nil {
							// Can't compare if info is missing, might be intermediate state
							continue
						}

						if relatedFile.Id == deletedFile.Id { // ID match!
							// We found the new name, ignore the remove event and only raise the Rename event
							result := NewFSEvent(Rename, relatedAction.Path, relatedAction.Timestamp) // Use NEW path/time for Rename event
							result.Properties["OldPath"] = action.Path                                // Store OLD path
							result.EnrichFromInfo(relatedFile)
							b.emitEvent(result)
							processedPaths[action.Signature()] = true
							processedPaths[relatedAction.Signature()] = true
							isRename = true
							idMismatchSuspected = false // Confirmed rename, reset flag

							// Update the watchmap with the new file information (i.e. after the rename)
							b.watchmap.Delete(action.Path)
							b.watchmap.Set(relatedAction.Path, relatedFile)

							break // Found the match, exit inner loop
						} else {
							// Found a Create event temporally related, but ID mismatch. Flag it.
							idMismatchSuspected = true
						}
					}
				}
			}

			if !isRename {
				// If it wasn't a confirmed rename, check if we suspected an ID mismatch
				if idMismatchSuspected {
					// Send an error indicating potential issue
					select { // Non-blocking send
					case b.errors <- fmt.Errorf("potential windows rename detected for remove %s but ID mismatch with create events", action.Path):
					default: // Avoid blocking if error channel is full/unbuffered
					}
				}

				// Process the Remove event normally (hard or soft delete)
				// Check if the file the deleted item was a directory, and if so, remove the watch
				if deletedFile != nil && os.FileMode(deletedFile.Mode).IsDir() { // Check deletedFile is not nil
					b.RemoveWatch(action.Path)
					// No need to delete from watchmap here, RemoveWatch handles prefix deletion
				} else {
					// Ensure regular files are also removed from map on remove, only if deletedFile was found
					if deletedFile != nil {
						b.watchmap.Delete(action.Path)
					}
				}

				if deletedFile != nil { // Only enrich if we have info
					action.EnrichFromInfo(deletedFile)
				}
				// TODO: Differentiate Hard vs Soft remove on Windows if possible/needed
				if runtime.GOOS != "windows" {
					action.Properties["Type"] = "Hard" // Assuming hard for now
				}

				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

		case Create:
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longet exists. Remove it from the watchmap if it exists
					b.watchmap.Delete(action.Path)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				// We failed to get the file info (maybe permission error or transient issue), enrich with what we have
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			var isRenameOrMove bool = false // Reset or ensure it's defined here

			// Check for preceding Remove event (Windows Rename/Move pattern)
			if runtime.GOOS == "windows" {
				for _, relatedAction := range eventList {
					// Look for a REMOVE event *before* this CREATE event
					if relatedAction.Type == Remove && relatedAction.Timestamp.Before(action.Timestamp) {
						// Get the info for the removed file *from the watchmap*
						removedFileInfo := b.watchmap.Get(relatedAction.Path)
						if removedFileInfo != nil && info.Id == removedFileInfo.Id {
							// ID Match! This looks like the second part of a Windows Rename/Move
							result := NewFSEvent(Rename, action.Path, action.Timestamp) // Use NEW path/time
							result.Properties["OldPath"] = removedFileInfo.Path         // Store OLD path
							result.EnrichFromInfo(info)                                 // Enrich with NEW info
							b.emitEvent(result)

							// Mark both original events as processed
							processedPaths[action.Signature()] = true
							processedPaths[relatedAction.Signature()] = true
							isRenameOrMove = true

							// Update watchmap
							// The REMOVE handler might have already deleted old path, but be sure
							b.watchmap.Delete(removedFileInfo.Path)
							b.watchmap.Set(action.Path, info) // Add new path

							break // Found match, exit inner loop
						}
						// We don't set idMismatchSuspected here, the REMOVE handler does that
					}
				}
			}

			// check for following Rename event (Unix Rename/Move pattern)
			// Only run this if the previous check didn't find a match
			if !isRenameOrMove {
				for _, relatedAction := range eventList {
					// Look for RENAME *after* this CREATE
					if relatedAction.Type == Rename && relatedAction.Timestamp.After(action.Timestamp) { // Check time relationship if needed
						potentialRename := b.watchmap.Get(relatedAction.Path)
						// Check potentialRename is not nil before accessing Id
						if potentialRename != nil && potentialRename.Id == info.Id {
							// We found the rename event, ignore the create event and only raise the Rename event
							result := NewFSEvent(Rename, action.Path, action.Timestamp) // Use NEW path/time
							result.Properties["OldPath"] = potentialRename.Path         // Store OLD path
							result.EnrichFromInfo(info)                                 // Enrich with NEW info
							b.emitEvent(result)
							processedPaths[action.Signature()] = true
							processedPaths[relatedAction.Signature()] = true
							isRenameOrMove = true

							// Update watchmap
							b.watchmap.Delete(potentialRename.Path)
							b.watchmap.Set(action.Path, info)

							break
						}
					}
				}
			}

			// Now that we're sure it is not a rename or move. We need to check if it is non-empty file creation.
			// If it is, there should be a corresponding write event which we need to ignore.
			// We also need to check if it is a directory creation. If it is, we need to add a watch for it. Otherwise, we add the file to the watchmap.
			// TODO: Make this optional. Maybe users want to also get write events for non-empty files?
			if !isRenameOrMove {
				if b.watchrecursive && stat.IsDir() {
					// Make sure AddWatch doesn't error due to existing map entry if called twice
					_ = b.AddWatch(action.Path) // Add watch if not already present
				} else {
					// Update watchmap only if it's not a directory being handled by AddWatch
					if !stat.IsDir() {
						b.watchmap.Set(action.Path, info)
					}
				}

				if !stat.IsDir() {
					// Check if there's a corresponding write event for the same path
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							processedPaths[relatedAction.Signature()] = true
							// We found the write event, so we can stop checking.
							// If there are multiple write events, the next one should
							// be considered a nomral write event that we happened to capture
							// in the same queue frame.
							break
						}
					}
				}

				// Now we raise the create event normally
				action.EnrichFromInfo(info) // Enrich with info we already have
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

		case Rename:
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases:
			// - When a file or directory is renamed. In such case there should also be a corresponding Create event
			// - When a file or directory is moved between or within watched directories. Also emits a Create event.
			// - When a file or directory is moved outside the bounds of watched directories, or moved to a trash directory (i.e, soft deleted)
			//   This happens only on POSIX systems (i.e. Linux, MacOS). Windows correctly emits Remove events in cases of soft deletion.

			// So, we need to check if there's a corresponding Create event for the same file identifier
			// If there is one, we ignore the Create event and only raise the Rename event
			// Otherwise, we ignore the Rename event and raise a Remove event

			// Get the file info from the watchmap
			renamedFileInfo := b.watchmap.Get(action.Path)
			if renamedFileInfo == nil {
				// If the file is not in the watchmap, it means it's not being watched, so we raise the rename event
				// This shouldn't happen if our watchmap is up to date.
				// But if it does, we'll raise the rename event because we have nothing to compare to
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			isRenameOrMove := false
			// Check if there's a corresponding Create event for the same file identifier
			// Unfortunately this is a little expensive, because the create event could be before or after the rename event
			// So we'll have to stat all paths in Create events in the eventList
			for _, relatedCreate := range eventList {
				if relatedCreate.Type == Create {
					createdStat, err := os.Stat(relatedCreate.Path)
					if err != nil {
						continue
					}
					createdFileInfo := FromOSInfo(relatedCreate.Path, createdStat)
					if createdFileInfo != nil && createdFileInfo.Id == renamedFileInfo.Id {
						// We found the create event, ignore it and only raise the enriched Rename event
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = renamedFileInfo.Path
						result.EnrichFromInfo(createdFileInfo)
						b.emitEvent(result)
						b.watchmap.Delete(renamedFileInfo.Path)
						b.watchmap.Set(createdFileInfo.Path, createdFileInfo)
						processedPaths[action.Signature()] = true
						processedPaths[relatedCreate.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			if !isRenameOrMove {
				// No corresponding create event found, raise a Remove event
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				b.emitEvent(result)
				processedPaths[action.Signature()] = true
			}

		case Write:
			// First, dedup modify events
			// Check if there are multiple save events for the same path within the queue, treat them as a single Modify event
			latestModify := action
			for _, relatedModify := range eventList {
				if relatedModify.Path == action.Path && relatedModify.Type == Write && relatedModify.Timestamp.After(latestModify.Timestamp) {
					processedPaths[latestModify.Signature()] = true
					latestModify = relatedModify
				}
			}

			// Check if the file exists on disk
			stat, err := os.Stat(latestModify.Path)
			if err != nil {
				// If the file doesn't exist on disk, remove it from the watchmap and ignore the event as there must be a corresponding Remove event
				if os.IsNotExist(err) {
					b.watchmap.Delete(latestModify.Path)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Windows emits write events on directories when contents are changed. This is irrelevant for us.
			// Check if we're on Windows AND for event is for a directory? If true, ignore the event
			if runtime.GOOS == "windows" && stat.IsDir() {
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Then check if there is a Create event for the same path, ignore the Write event and raise the Create event instead
			// This handles the case where any OS emits a Create event followed by a Write event for a new non-empty file or a copy or move from unwatched directory a watched one
			foundCreated := false
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// We found the Create event, ignore the Write event and raise the Create event instead
					// But first we need to check if this specific create event was already processed
					if !processedPaths[relatedCreate.Signature()] {
						relatedCreate.EnrichFromInfo(FromOSInfo(latestModify.Path, stat))
						b.emitEvent(relatedCreate)
						processedPaths[relatedCreate.Signature()] = true
					}
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}

			// Otherwise, Process the latest Write event
			if !foundCreated {
				info := FromOSInfo(latestModify.Path, stat)
				if info != nil {
					b.watchmap.Set(latestModify.Path, info)
					latestModify.EnrichFromInfo(info)
				}
				b.emitEvent(latestModify)
				processedPaths[latestModify.Signature()] = true
			}

		case Chmod:
			// Emit the chmod event if configured
			if b.config.EmitChmod {
				b.emitEvent(action)
			}

			// Mark the action as processed anyway, because it's a chmod event
			processedPaths[action.Signature()] = true

			// Handle case where writing empty file in macOS results in no modify event, but only in chmod event
			if runtime.GOOS == "darwin" {
				stat, err := os.Stat(action.Path)
				if err != nil {
					continue
				}
				potentialModify := b.watchmap.Get(action.Path)
				if potentialModify != nil {
					if stat.Size() == 0 && potentialModify.Size != 0 {
						// Raise a write event
						modified := NewFSEvent(Write, action.Path, action.Timestamp)
						modified.EnrichFromInfo(FromOSInfo(action.Path, stat))
						b.emitEvent(modified)
					}

					// Update the watchmap with the new file information (i.e. after the chmod)
					// No need to do this for other operating systems because it will be eventually caught by the write or create events
					b.watchmap.Set(action.Path, FromOSInfo(action.Path, stat))
				}
			}

		default:
			// Unreachable
		}
	}
}

// isSystemFile checks if the file is a common Windows system or metadata file.
func isSystemFile(name string) bool {
	// Base filenames commonly generated by Windows
	base := strings.ToLower(filepath.Base(name))
	switch base {
	case "desktop.ini", "thumbs.db", "$recycle.bin", "system volume information":
		return true
	}

	return false
}

// isHiddenFile checks if a file is hidden on Windows.
func isHiddenFile(path string) (bool, error) {
	// Get the absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}

	// Get file pointer
	pointer, err := syscall.UTF16PtrFromString(absPath)
	if err != nil {
		return false, err
	}

	// Get file attributes
	attributes, err := syscall.GetFileAttributes(pointer)
	if err != nil {
		return false, err
	}

	// Check if the hidden attribute is set
	// FILE_ATTRIBUTE_HIDDEN = 2 in Windows
	return attributes&syscall.FILE_ATTRIBUTE_HIDDEN != 0, nil
}

func FromOSInfo(path string, fileinfo os.FileInfo) *Info {
	id, err := getFileID(path)
	if err != nil {
		return nil
	}

	return &Info{
		Id:   id,
		Path: path,
		Mode: uint32(fileinfo.Mode()),
	}
}

type BY_HANDLE_FILE_INFORMATION struct {
	FileAttributes     uint32
	CreationTime       syscall.Filetime
	LastAccessTime     syscall.Filetime
	LastWriteTime      syscall.Filetime
	VolumeSerialNumber uint32
	FileSizeHigh       uint32
	FileSizeLow        uint32
	NumberOfLinks      uint32
	FileIndexHigh      uint32
	FileIndexLow       uint32
}

func getFileID(path string) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	handle := windows.Handle(file.Fd())

	var fileInfo BY_HANDLE_FILE_INFORMATION
	err = windows.GetFileInformationByHandle(handle, (*windows.ByHandleFileInformation)(unsafe.Pointer(&fileInfo)))
	if err != nil {
		return 0, err
	}

	fileID := (uint64(fileInfo.FileIndexHigh) << 32) | uint64(fileInfo.FileIndexLow)
	return fileID, nil
}
