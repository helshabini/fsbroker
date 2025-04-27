//go:build windows
// +build windows

package fsbroker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func (b *FSBroker) resolveAndHandle(eventQueue *EventQueue, tickerLock *sync.Mutex) {
	fmt.Printf("[DEBUG] resolveAndHandle triggered. Trying to acquire lock...\n")
	// We only want one instance of this method to run at a time, so we lock it
	if !tickerLock.TryLock() {
		fmt.Printf("[DEBUG] resolveAndHandle could not acquire lock, returning.\n")
		return
	}
	fmt.Printf("[DEBUG] resolveAndHandle acquired lock.\n")
	defer tickerLock.Unlock()
	defer fmt.Printf("[DEBUG] resolveAndHandle releasing lock.\n")

	// Get a snapshot of all events received in this tick
	eventList := eventQueue.List()

	if len(eventList) == 0 {
		fmt.Printf("[DEBUG] resolveAndHandle found event list empty, returning.\n")
		return
	}

	fmt.Printf("[DEBUG] resolveAndHandle Processing event list (size %d): %v\n", len(eventList), eventList)
	processedPaths := make(map[string]bool)

	// --- Pass 1: Detect and synthesize Rename/Move events based on File ID ---
	fmt.Printf("[DEBUG] === Pass 1: Detecting Renames/Moves ===\n")
	for i, createAction := range eventList {
		if createAction.Type != Create || processedPaths[createAction.Signature()] {
			continue // Only look at unprocessed Create events
		}
		fmt.Printf("[DEBUG - Pass 1] Checking CREATE event: %s\n", createAction.Signature())

		// Get info (including ID) of the created file
		createdStat, err := os.Stat(createAction.Path)
		if err != nil {
			fmt.Printf("[DEBUG - Pass 1]   -> Error stating created file %s: %v. Cannot use for pairing.\n", createAction.Path, err)
			continue
		}
		createdFileInfo := FromOSInfo(createAction.Path, createdStat)
		if createdFileInfo == nil {
			fmt.Printf("[DEBUG - Pass 1]   -> Failed to get OS info for created file %s. Cannot use for pairing.\n", createAction.Path)
			continue
		}
		fmt.Printf("[DEBUG - Pass 1]   -> Created file info: ID=%d, Path=%s\n", createdFileInfo.Id, createdFileInfo.Path)

		// Look for a preceding REMOVE or RENAME in the *same batch* with matching ID
		foundPartner := false
		for j, partnerAction := range eventList {
			if i == j || processedPaths[partnerAction.Signature()] {
				continue // Don't compare to self or already processed events
			}

			if partnerAction.Type == Remove || partnerAction.Type == Rename {
				fmt.Printf("[DEBUG - Pass 1]     -> Comparing with potential partner: %s\n", partnerAction.Signature())
				// Get the File ID of the partner *before* it was removed/renamed (from watchmap)
				partnerInfoFromMap := b.watchmap.Get(partnerAction.Path)
				if partnerInfoFromMap == nil {
					fmt.Printf("[DEBUG - Pass 1]       -> No info in watchmap for partner %s, cannot compare ID.\n", partnerAction.Path)
					continue
				}
				fmt.Printf("[DEBUG - Pass 1]       -> Partner info from map: ID=%d, Path=%s\n", partnerInfoFromMap.Id, partnerInfoFromMap.Path)

				// Compare File IDs
				if createdFileInfo.Id == partnerInfoFromMap.Id {
					fmt.Printf("[DEBUG - Pass 1]       -> ID MATCH! Synthesizing RENAME from %s and %s.\n", partnerAction.Signature(), createAction.Signature())
					// Synthesize the Rename event
					// Use the CREATE event's path/time as the definitive state
					renameEvent := NewFSEvent(Rename, createAction.Path, createAction.Timestamp)
					renameEvent.Properties["OldPath"] = partnerAction.Path // Use REMOVE/RENAME event's path as OldPath
					renameEvent.EnrichFromInfo(createdFileInfo)            // Enrich with *new* file info

					fmt.Printf("[DEBUG - Pass 1] Emitting synthesized RENAME: Type=%s, Path=%s, OldPath=%s, Props=%v\n", renameEvent.Type.String(), renameEvent.Path, renameEvent.Properties["OldPath"], renameEvent.Properties)
					b.emitEvent(renameEvent)

					// Mark *both* original events as processed
					fmt.Printf("[DEBUG - Pass 1] Marking %s as processed.\n", partnerAction.Signature())
					processedPaths[partnerAction.Signature()] = true
					fmt.Printf("[DEBUG - Pass 1] Marking %s as processed.\n", createAction.Signature())
					processedPaths[createAction.Signature()] = true

					// Update the watchmap
					fmt.Printf("[DEBUG - Pass 1] Updating watchmap: Deleting %s, Setting %s\n", partnerAction.Path, createAction.Path)
					b.watchmap.Delete(partnerAction.Path)              // Remove old path
					b.watchmap.Set(createAction.Path, createdFileInfo) // Add new path

					foundPartner = true
					break // Found the matching partner for this Create, move to next Create
				} else {
					fmt.Printf("[DEBUG - Pass 1]       -> ID Mismatch (Partner: %d, Create: %d)\n", partnerInfoFromMap.Id, createdFileInfo.Id)
				}
			}
		}
		if foundPartner {
			fmt.Printf("[DEBUG - Pass 1] Finished processing partners for CREATE %s (found rename/move).\n", createAction.Signature())
		} else {
			fmt.Printf("[DEBUG - Pass 1] Finished processing partners for CREATE %s (no rename/move detected).\n", createAction.Signature())
		}
	}
	fmt.Printf("[DEBUG] === Pass 1 Complete ===\n")

	// --- Pass 2: Process remaining events sequentially ---
	fmt.Printf("[DEBUG] === Pass 2: Processing Remaining Events ===\n")
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		fmt.Printf("[DEBUG - Pass 2] Popped event: Type=%s, Path=%s, Sig=%s\n", action.Type.String(), action.Path, action.Signature())
		// Ignore already processed paths (e.g., part of a synthesized Rename/Move)
		if processedPaths[action.Signature()] {
			fmt.Printf("[DEBUG - Pass 2]   -> Event %s already processed (likely part of rename/move), skipping.\n", action.Signature())
			continue
		}

		switch action.Type {
		case Remove:
			fmt.Printf("[DEBUG - Pass 2 - Remove] Processing REMOVE for: %s\n", action.Path)
			// Get file info *before* deletion from watchmap (for enrichment)
			deletedFile := b.watchmap.Get(action.Path)
			fmt.Printf("[DEBUG - Pass 2 - Remove] deletedFile info from map: %v\n", deletedFile)

			// Perform cleanup (remove watch for dirs, delete from map)
			if deletedFile != nil { // Only cleanup if we had info
				if os.FileMode(deletedFile.Mode).IsDir() {
					fmt.Printf("[DEBUG - Pass 2 - Remove]   -> Removing watch for directory: %s\n", action.Path)
					b.RemoveWatch(action.Path) // Removes watch and entries with this prefix from map
				} else {
					fmt.Printf("[DEBUG - Pass 2 - Remove]   -> Deleting file from watchmap: %s\n", action.Path)
					b.watchmap.Delete(action.Path) // Remove file entry from map
				}
				// Enrich the REMOVE event with the deleted file's info
				fmt.Printf("[DEBUG - Pass 2 - Remove]   -> Enriching REMOVE event with info: %v\n", deletedFile)
				action.EnrichFromInfo(deletedFile)
			} else {
				fmt.Printf("[DEBUG - Pass 2 - Remove]   -> No deletedFile info in map for %s, cannot enrich or cleanup map precisely.\n", action.Path)
			}

			// Emit the standard REMOVE event
			fmt.Printf("[DEBUG - Pass 2 - Remove] Emitting standard REMOVE event now: Type=%s, Path=%s, Props=%v\n", action.Type.String(), action.Path, action.Properties)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true // Mark processed (mostly for clarity, already skipped if true)

		case Create:
			fmt.Printf("[DEBUG - Pass 2 - Create] Processing CREATE for: %s\n", action.Path)
			// Since renames were handled in Pass 1, this must be a true Create.
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Printf("[DEBUG - Pass 2 - Create]   -> File %s does not exist anymore (likely deleted quickly). Deleting from map if present.\n", action.Path)
					b.watchmap.Delete(action.Path)
				} else {
					fmt.Printf("[DEBUG - Pass 2 - Create]   -> Error stating file %s: %v\n", action.Path, err)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				fmt.Printf("[DEBUG - Pass 2 - Create]   -> Failed to get OS info for %s. Enriching with basic stat.\n", action.Path)
				action.EnrichFromStat(stat) // Fallback enrichment
			} else {
				fmt.Printf("[DEBUG - Pass 2 - Create] Got file info: %v\n", info)
				action.EnrichFromInfo(info)
				// Handle directory creation and watching / Add file to watchmap
				if b.watchrecursive && stat.IsDir() {
					fmt.Printf("[DEBUG - Pass 2 - Create]   -> Adding watch for new directory: %s\n", action.Path)
					_ = b.AddWatch(action.Path) // Add watch, also adds dir to map
				} else if !stat.IsDir() {
					fmt.Printf("[DEBUG - Pass 2 - Create]   -> Adding file to watchmap: %s\n", action.Path)
					b.watchmap.Set(action.Path, info) // Add file to map
				}
			}

			// Emit the create event
			fmt.Printf("[DEBUG - Pass 2 - Create] Emitting standard CREATE event: Type=%s, Path=%s, Props=%v\n", action.Type.String(), action.Path, action.Properties)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Rename:
			// Original Rename events from fsnotify are ignored in Pass 2 on Windows,
			// as true renames are synthesized in Pass 1.
			fmt.Printf("[DEBUG - Pass 2 - Rename] Ignoring original RENAME event %s (handled in Pass 1).\n", action.Signature())
			processedPaths[action.Signature()] = true

		case Write:
			fmt.Printf("[DEBUG - Pass 2 - Write] Processing WRITE for: %s\n", action.Path)

			// Check if this Write immediately follows a Create for the same path in this batch (using eventList snapshot)
			foundPrecedingCreate := false
			for _, prevAction := range eventList {
				// Look for a Create event for the same path within this batch, occurring before or AT THE SAME TIME as the Write
				// We ignore processedPaths here because the Create might have been processed earlier *in this same batch*.
				if prevAction.Path == action.Path && prevAction.Type == Create && !action.Timestamp.Before(prevAction.Timestamp) { // Allow Create <= Write timestamp
					fmt.Printf("[DEBUG - Pass 2 - Write]   -> Found preceding/concurrent CREATE %s in same batch. Ignoring this WRITE.\n", prevAction.Signature())
					foundPrecedingCreate = true
					break
				}
			}
			if foundPrecedingCreate {
				fmt.Printf("[DEBUG - Pass 2 - Write] Marking WRITE %s as processed (part of create).\n", action.Signature())
				processedPaths[action.Signature()] = true
				continue // Ignore this write
			}

			// Deduplicate writes: Check if a LATER write for the same path exists in eventList
			isLatestWrite := true
			for _, laterAction := range eventList {
				if laterAction.Path == action.Path && laterAction.Type == Write && laterAction.Timestamp.After(action.Timestamp) {
					fmt.Printf("[DEBUG - Pass 2 - Write]   -> Found later WRITE %s in same batch. Ignoring this earlier WRITE.\n", laterAction.Signature())
					isLatestWrite = false
					break
				}
			}
			if !isLatestWrite {
				fmt.Printf("[DEBUG - Pass 2 - Write] Marking WRITE %s as processed (superseded by later write).\n", action.Signature())
				processedPaths[action.Signature()] = true
				continue // Ignore this write
			}

			// If we got here, this is the latest Write event for this path in this batch,
			// and it's not immediately following a Create for the same path.
			fmt.Printf("[DEBUG - Pass 2 - Write] Processing as latest relevant WRITE.\n")

			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Printf("[DEBUG - Pass 2 - Write]   -> File %s does not exist. Deleting from map.\n", action.Path)
					b.watchmap.Delete(action.Path)
				} else {
					fmt.Printf("[DEBUG - Pass 2 - Write]   -> Error stating file %s: %v\n", action.Path, err)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			// Ignore WRITE events on directories (common noise on Windows)
			if stat.IsDir() {
				fmt.Printf("[DEBUG - Pass 2 - Write]   -> Ignoring WRITE on directory.\n")
				processedPaths[action.Signature()] = true
				continue
			}

			// Emit the Write event
			info := FromOSInfo(action.Path, stat)
			if info != nil {
				fmt.Printf("[DEBUG - Pass 2 - Write]   -> Updating watchmap with info: %v\n", info)
				b.watchmap.Set(action.Path, info) // Update map with potentially changed info
				action.EnrichFromInfo(info)
			} else {
				fmt.Printf("[DEBUG - Pass 2 - Write]   -> Failed to get OS info. Enriching with basic stat.\n")
				action.EnrichFromStat(stat)
			}
			fmt.Printf("[DEBUG - Pass 2 - Write] Emitting standard WRITE event: Type=%s, Path=%s, Props=%v\n", action.Type.String(), action.Path, action.Properties)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Chmod:
			fmt.Printf("[DEBUG - Pass 2 - Chmod] Processing CHMOD for: %s\n", action.Path)
			if b.config.EmitChmod {
				fmt.Printf("[DEBUG - Pass 2 - Chmod] Emitting CHMOD event.\n")
				b.emitEvent(action)
			} else {
				fmt.Printf("[DEBUG - Pass 2 - Chmod] Ignoring CHMOD event (EmitChmod=false).\n")
			}
			processedPaths[action.Signature()] = true

		default:
			fmt.Printf("[DEBUG - Pass 2] Unknown event type %d encountered for path %s!\n", action.Type, action.Path)
			processedPaths[action.Signature()] = true // Mark processed to avoid loops
		}
	}
	fmt.Printf("[DEBUG] === Pass 2 Complete ===\n")

	fmt.Printf("[DEBUG] resolveAndHandle releasing lock.\n")
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

	// Enrich with size and modtime for Windows, as they are generally reliable
	return &Info{
		Id:      id,
		Path:    path,
		Size:    uint64(fileinfo.Size()),
		ModTime: fileinfo.ModTime(),
		Mode:    uint32(fileinfo.Mode()),
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
