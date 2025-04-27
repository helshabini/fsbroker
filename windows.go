//go:build windows
// +build windows

package fsbroker

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func (b *FSBroker) resolveAndHandle(eventQueue *EventQueue, tickerLock *sync.Mutex) {
	logDebug("resolveAndHandle triggered. Trying to acquire lock...")
	// We only want one instance of this method to run at a time, so we lock it
	if !tickerLock.TryLock() {
		logDebug("resolveAndHandle could not acquire lock, returning.")
		return
	}
	logDebug("resolveAndHandle acquired lock.")
	defer tickerLock.Unlock()
	defer logDebug("resolveAndHandle releasing lock.")

	// Get a snapshot of all events received in this tick
	eventList := eventQueue.List()

	if len(eventList) == 0 {
		logDebug("resolveAndHandle found event list empty, returning.")
		return
	}

	// Log the initial snapshot if debug is enabled
	if slog.Default().Enabled(nil, slog.LevelDebug) {
		signatures := make([]string, len(eventList))
		for i, evt := range eventList {
			signatures[i] = evt.Signature() // Assuming signature is enough context here
		}
		logDebug("resolveAndHandle Processing event list", "size", len(eventList), "signatures", signatures)
	}
	processedPaths := make(map[string]bool)

	// --- Pass 1: Detect and synthesize Rename/Move events based on File ID ---
	logDebug("=== Pass 1: Detecting Renames/Moves ===")
	for i, createAction := range eventList {
		if createAction.Type != Create || processedPaths[createAction.Signature()] {
			continue // Only look at unprocessed Create events
		}
		logDebug("Pass 1: Checking CREATE event", "signature", createAction.Signature())

		// Get info (including ID) of the created file
		createdStat, err := os.Stat(createAction.Path)
		if err != nil {
			logDebug("Pass 1: Error stating created file, cannot use for pairing.", "path", createAction.Path, "error", err)
			continue
		}
		createdFileInfo := FromOSInfo(createAction.Path, createdStat)
		if createdFileInfo == nil {
			logDebug("Pass 1: Failed to get OS info for created file, cannot use for pairing.", "path", createAction.Path)
			continue
		}
		logDebug("Pass 1: Created file info", "id", createdFileInfo.Id, "path", createdFileInfo.Path)

		// Look for a preceding REMOVE or RENAME in the *same batch* with matching ID
		foundPartner := false
		for j, partnerAction := range eventList {
			if i == j || processedPaths[partnerAction.Signature()] {
				continue // Don't compare to self or already processed events
			}

			if partnerAction.Type == Remove || partnerAction.Type == Rename {
				logDebug("Pass 1: Comparing with potential partner", "createSig", createAction.Signature(), "partnerSig", partnerAction.Signature())
				// Get the File ID of the partner *before* it was removed/renamed (from watchmap)
				partnerInfoFromMap := b.watchmap.Get(partnerAction.Path)
				if partnerInfoFromMap == nil {
					logDebug("Pass 1: No info in watchmap for partner, cannot compare ID.", "partnerPath", partnerAction.Path)
					continue
				}
				logDebug("Pass 1: Partner info from map", "id", partnerInfoFromMap.Id, "path", partnerInfoFromMap.Path)

				// Compare File IDs
				if createdFileInfo.Id == partnerInfoFromMap.Id {
					logDebug("Pass 1: ID MATCH! Synthesizing RENAME.", "partnerSig", partnerAction.Signature(), "createSig", createAction.Signature())
					// Synthesize the Rename event
					// Use the CREATE event's path/time as the definitive state
					renameEvent := NewFSEvent(Rename, createAction.Path, createAction.Timestamp)
					renameEvent.Properties["OldPath"] = partnerAction.Path // Use REMOVE/RENAME event's path as OldPath
					renameEvent.EnrichFromInfo(createdFileInfo)            // Enrich with *new* file info

					logDebug("Pass 1: Emitting synthesized RENAME", "event", renameEvent)
					b.emitEvent(renameEvent)

					// Mark *both* original events as processed
					logDebug("Pass 1: Marking original events processed", "partnerSig", partnerAction.Signature(), "createSig", createAction.Signature())
					processedPaths[partnerAction.Signature()] = true
					processedPaths[createAction.Signature()] = true

					// Update the watchmap
					logDebug("Pass 1: Updating watchmap for rename", "deletePath", partnerAction.Path, "setPath", createAction.Path)
					b.watchmap.Delete(partnerAction.Path)              // Remove old path
					b.watchmap.Set(createAction.Path, createdFileInfo) // Add new path

					foundPartner = true
					break // Found the matching partner for this Create, move to next Create
				} else {
					logDebug("Pass 1: ID Mismatch", "partnerId", partnerInfoFromMap.Id, "createId", createdFileInfo.Id)
				}
			}
		}
		if foundPartner {
			logDebug("Pass 1: Finished processing partners (found rename/move)", "createSig", createAction.Signature())
		} else {
			logDebug("Pass 1: Finished processing partners (no rename/move detected)", "createSig", createAction.Signature())
		}
	}
	logDebug("=== Pass 1 Complete ===")

	// --- Pass 2: Process remaining events sequentially ---
	logDebug("=== Pass 2: Processing Remaining Events ===")
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		logDebug("Pass 2: Popped event", "signature", action.Signature(), "type", action.Type.String(), "path", action.Path)
		// Ignore already processed paths (e.g., part of a synthesized Rename/Move)
		if processedPaths[action.Signature()] {
			logDebug("Pass 2: Event already processed (likely part of rename/move), skipping.", "signature", action.Signature())
			continue
		}

		switch action.Type {
		case Remove:
			logDebug("Pass 2: Remove: Processing", "signature", action.Signature(), "path", action.Path)
			// Get file info *before* deletion from watchmap (for enrichment)
			deletedFile := b.watchmap.Get(action.Path)
			logDebug("Pass 2: Remove: Info from watchmap", "signature", action.Signature(), "info", deletedFile)

			// Perform cleanup (remove watch for dirs, delete from map)
			if deletedFile != nil { // Only cleanup if we had info
				if os.FileMode(deletedFile.Mode).IsDir() {
					logDebug("Pass 2: Remove: Removing watch for directory", "signature", action.Signature(), "path", action.Path)
					b.RemoveWatch(action.Path) // Removes watch and entries with this prefix from map
				} else {
					logDebug("Pass 2: Remove: Deleting file from watchmap", "signature", action.Signature(), "path", action.Path)
					b.watchmap.Delete(action.Path) // Remove file entry from map
				}
				// Enrich the REMOVE event with the deleted file's info
				logDebug("Pass 2: Remove: Enriching REMOVE event with info", "signature", action.Signature())
				action.EnrichFromInfo(deletedFile)
			} else {
				logDebug("Pass 2: Remove: No info in map, cannot enrich or cleanup precisely.", "signature", action.Signature(), "path", action.Path)
			}

			// Emit the standard REMOVE event
			logDebug("Pass 2: Remove: Emitting standard REMOVE event", "signature", action.Signature(), "event", action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true // Mark processed (mostly for clarity, already skipped if true)

		case Create:
			logDebug("Pass 2: Create: Processing", "signature", action.Signature(), "path", action.Path)
			// Since renames were handled in Pass 1, this must be a true Create.
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) {
					logDebug("Pass 2: Create: File does not exist anymore (likely deleted quickly). Deleting from map if present.", "signature", action.Signature(), "path", action.Path)
					b.watchmap.Delete(action.Path)
				} else {
					slog.Warn("Pass 2: Create: Error stating file", "signature", action.Signature(), "path", action.Path, "error", err)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				slog.Warn("Pass 2: Create: Failed to get OS info. Enriching with basic stat.", "signature", action.Signature(), "path", action.Path)
				action.EnrichFromStat(stat) // Fallback enrichment
			} else {
				logDebug("Pass 2: Create: Got file info", "signature", action.Signature(), "info", info)
				action.EnrichFromInfo(info)
				// Handle directory creation and watching / Add file to watchmap
				if b.watchrecursive && stat.IsDir() {
					logDebug("Pass 2: Create: Adding watch for new directory", "signature", action.Signature(), "path", action.Path)
					_ = b.AddWatch(action.Path) // Add watch, also adds dir to map
				} else if !stat.IsDir() {
					logDebug("Pass 2: Create: Adding file to watchmap", "signature", action.Signature(), "path", action.Path)
					b.watchmap.Set(action.Path, info) // Add file to map
				}
			}

			// Emit the create event
			logDebug("Pass 2: Create: Emitting standard CREATE event", "signature", action.Signature(), "event", action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Rename:
			// Original Rename events from fsnotify are ignored in Pass 2 on Windows,
			// as true renames are synthesized in Pass 1.
			logDebug("Pass 2: Rename: Ignoring original RENAME event (handled in Pass 1)", "signature", action.Signature())
			processedPaths[action.Signature()] = true

		case Write:
			logDebug("Pass 2: Write: Processing", "signature", action.Signature(), "path", action.Path)

			// Check if this Write immediately follows a Create for the same path in this batch (using eventList snapshot)
			foundPrecedingCreate := false
			for _, prevAction := range eventList {
				// Look for a Create event for the same path within this batch, occurring before or AT THE SAME TIME as the Write
				// We ignore processedPaths here because the Create might have been processed earlier *in this same batch*.
				if prevAction.Path == action.Path && prevAction.Type == Create && !action.Timestamp.Before(prevAction.Timestamp) { // Allow Create <= Write timestamp
					logDebug("Pass 2: Write: Found preceding/concurrent CREATE in same batch. Ignoring this WRITE.", "writeSig", action.Signature(), "createSig", prevAction.Signature())
					foundPrecedingCreate = true
					break
				}
			}
			if foundPrecedingCreate {
				logDebug("Pass 2: Write: Marking WRITE as processed (part of create)", "signature", action.Signature())
				processedPaths[action.Signature()] = true
				continue // Ignore this write
			}

			// Deduplicate writes: Check if a LATER write for the same path exists in eventList
			isLatestWrite := true
			for _, laterAction := range eventList {
				if laterAction.Path == action.Path && laterAction.Type == Write && laterAction.Timestamp.After(action.Timestamp) {
					logDebug("Pass 2: Write: Found later WRITE in same batch. Ignoring this earlier WRITE.", "earlierSig", action.Signature(), "laterSig", laterAction.Signature())
					isLatestWrite = false
					break
				}
			}
			if !isLatestWrite {
				logDebug("Pass 2: Write: Marking WRITE as processed (superseded by later write)", "signature", action.Signature())
				processedPaths[action.Signature()] = true
				continue // Ignore this write
			}

			// If we got here, this is the latest Write event for this path in this batch,
			// and it's not immediately following a Create for the same path.
			logDebug("Pass 2: Write: Processing as latest relevant WRITE", "signature", action.Signature())

			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) {
					logDebug("Pass 2: Write: File does not exist. Deleting from map.", "signature", action.Signature(), "path", action.Path)
					b.watchmap.Delete(action.Path)
				} else {
					slog.Warn("Pass 2: Write: Error stating file", "signature", action.Signature(), "path", action.Path, "error", err)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			// Ignore WRITE events on directories (common noise on Windows)
			if stat.IsDir() {
				logDebug("Pass 2: Write: Ignoring WRITE on directory", "signature", action.Signature())
				processedPaths[action.Signature()] = true
				continue
			}

			// Emit the Write event
			info := FromOSInfo(action.Path, stat)
			if info != nil {
				logDebug("Pass 2: Write: Updating watchmap with info", "signature", action.Signature(), "info", info)
				b.watchmap.Set(action.Path, info) // Update map with potentially changed info
				action.EnrichFromInfo(info)
			} else {
				slog.Warn("Pass 2: Write: Failed to get OS info. Enriching with basic stat.", "signature", action.Signature())
				action.EnrichFromStat(stat)
			}
			logDebug("Pass 2: Write: Emitting standard WRITE event", "signature", action.Signature(), "event", action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Chmod:
			logDebug("Pass 2: Chmod: Processing", "signature", action.Signature(), "path", action.Path)
			if b.config.EmitChmod {
				logDebug("Pass 2: Chmod: Emitting CHMOD event", "signature", action.Signature())
				b.emitEvent(action)
			} else {
				logDebug("Pass 2: Chmod: Ignoring CHMOD event (EmitChmod=false)", "signature", action.Signature())
			}
			processedPaths[action.Signature()] = true

		default:
			slog.Warn("Pass 2: Unknown event type encountered", "typeValue", action.Type, "signature", action.Signature())
			processedPaths[action.Signature()] = true // Mark processed to avoid loops
		}
	}
	logDebug("=== Pass 2 Complete ===")
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
