//go:build darwin
// +build darwin

package fsbroker

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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

	// items to be removed from watchmap
	watchmapItemsToRemove := make(map[string]bool)

	// Log the initial snapshot if debug is enabled
	if slog.Default().Enabled(nil, slog.LevelDebug) {
		signatures := make([]string, len(eventList))
		for i, evt := range eventList {
			signatures[i] = evt.Signature()
		}
		logDebug("Initial event list snapshot", "size", len(eventList), "signatures", signatures)
	}

	if len(eventList) == 0 {
		return
	}

	//Now we process the events remaining in the event queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		// Ignore already processed paths
		logDebug("Loop Start: Popped action", "signature", action.Signature(), "type", action.Type.String(), "path", action.Path, "time", action.Timestamp)
		if processedPaths[action.Signature()] {
			logDebug("Loop Skip: Action already processed", "signature", action.Signature())
			continue
		}

		switch action.Type {
		case Remove:
			// Check if there's any earlier event for the same path within the queue, ignore it and only raise the remove event
			for _, relatedAction := range eventList {
				if relatedAction.Path == action.Path && relatedAction.Timestamp.Before(action.Timestamp) {
					processedPaths[relatedAction.Signature()] = true
					logDebug("Remove: Marked earlier action processed", "removedSig", action.Signature(), "earlierSig", relatedAction.Signature())
				}
			}

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			logDebug("Remove: Info from watchmap", "signature", action.Signature(), "info", deletedFile)

			// Process the Remove event normally (hard delete on macOS)
			// Check if the deleted item was a directory, and if so, remove the watch
			if deletedFile != nil && deletedFile.IsDir() { // Check deletedFile is not nil
				logDebug("Remove: Path is directory, removing watch", "signature", action.Signature(), "path", action.Path)
				b.RemoveWatch(action.Path)
				// No need to delete from watchmap here, RemoveWatch handles prefix deletion
			} else {
				// Ensure regular files are also removed from map on remove, only if deletedFile was found
				if deletedFile != nil {
					logDebug("Remove: Path is file, adding to deferred removal", "signature", action.Signature(), "path", action.Path)
					watchmapItemsToRemove[action.Path] = true
				}
			}

			if deletedFile != nil { // Only enrich if we have info
				action.EnrichFromInfo(deletedFile)
				logDebug("Remove: Enriched event from map info", "signature", action.Signature())
			}
			// Assuming hard delete, because macOS doesn't emit a Remove event for soft deletes
			action.Properties["Type"] = "Hard"

			logDebug("Remove: Emitting Hard Remove", "signature", action.Signature(), "event", action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Create:
			logDebug("Create: Entering handler", "signature", action.Signature())
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longer exists. Remove it from the watchmap if it exists
					logDebug("Create: os.Stat failed (NotExist), adding to deferred removal and skipping", "signature", action.Signature(), "path", action.Path, "error", err)
					watchmapItemsToRemove[action.Path] = true
				} else {
					slog.Warn("Create: os.Stat failed, skipping", "signature", action.Signature(), "path", action.Path, "error", err)
				}
				processedPaths[action.Signature()] = true
				continue
			}
			logDebug("Create: os.Stat successful", "signature", action.Signature(), "path", action.Path)

			info := FromOSInfo(action.Path, stat)
			logDebug("Create: Got info from stat", "signature", action.Signature(), "path", action.Path, "info", info)
			if info == nil {
				// We failed to get the file info (maybe permission error or transient issue), enrich with what we have
				slog.Warn("Create: FromOSInfo returned nil after stat, enriching from stat and emitting raw Create", "signature", action.Signature(), "path", action.Path)
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			var isRenameOrMove bool = false // Reset or ensure it's defined here

			// Check for following Rename event (Unix Rename/Move pattern)
			logDebug("Create: Entering Rename partner check", "signature", action.Signature())
			for _, relatedAction := range eventList {
				// Look for RENAME *after* this CREATE
				if relatedAction.Type == Rename && relatedAction.Timestamp.After(action.Timestamp) { // Check time relationship if needed
					logDebug("Create: Checking potential RENAME partner", "createSig", action.Signature(), "renameSig", relatedAction.Signature())
					// --- Log Watchmap State BEFORE potential rename handling ---
					// b.PrintMap()
					potentialRename := b.watchmap.Get(relatedAction.Path)
					logDebug("Create: Info for potential rename path from watchmap", "createSig", action.Signature(), "renamePath", relatedAction.Path, "renameInfo", potentialRename)
					// Safely log IDs, handling potential nil pointer for potentialRename
					var potentialRenameId uint64
					if potentialRename != nil {
						potentialRenameId = potentialRename.Id
					}
					idsMatch := potentialRename != nil && potentialRename.Id == info.Id
					logDebug("Create: Comparing IDs for rename", "createSig", action.Signature(), "renameSig", relatedAction.Signature(), "renameId", potentialRenameId, "createId", info.Id, "match", idsMatch)
					if idsMatch {
						// We found the rename event, ignore the create event and only raise the Rename event
						logDebug("Create: Found RENAME partner via ID match, synthesizing Rename", "createSig", action.Signature(), "renameSig", relatedAction.Signature(), "id", info.Id)
						result := NewFSEvent(Rename, action.Path, action.Timestamp) // Use NEW path/time
						result.Properties["OldPath"] = potentialRename.Path         // Store OLD path
						result.EnrichFromInfo(info)                                 // Enrich with NEW info
						logDebug("Create: Emitting synthesized RENAME", "createSig", action.Signature(), "event", result)
						b.emitEvent(result)
						processedPaths[action.Signature()] = true
						processedPaths[relatedAction.Signature()] = true
						logDebug("Create: Marked Create and Rename processed", "createSig", action.Signature(), "renameSig", relatedAction.Signature())
						isRenameOrMove = true

						// Update watchmap
						logDebug("Create: Updating watchmap for rename", "createSig", action.Signature(), "deletePath", potentialRename.Path, "setPath", action.Path)
						watchmapItemsToRemove[potentialRename.Path] = true
						b.watchmap.Set(action.Path, info)

						break
					}
				}
			}
			logDebug("Create: Exited Rename partner check", "signature", action.Signature(), "isRenameOrMove", isRenameOrMove)

			// Now that we're sure it is not a rename or move. We need to check if it is non-empty file creation.
			// If it is, there should be a corresponding write event which we need to ignore.
			// Also handle directory creation for recursive watch.
			if !isRenameOrMove {
				logDebug("Create: Processing as normal Create (not rename/move)", "signature", action.Signature())
				if b.watchrecursive && stat.IsDir() {
					// Make sure AddWatch doesn't error due to existing map entry if called twice
					_ = b.AddWatch(action.Path) // Add watch if not already present
					logDebug("Create: Added recursive watch for directory", "signature", action.Signature(), "path", action.Path)
				} else {
					// Update watchmap only if it's not a directory being handled by AddWatch
					if !stat.IsDir() {
						logDebug("Create: Updating watchmap for non-dir create", "signature", action.Signature(), "path", action.Path)
						b.watchmap.Set(action.Path, info)
					}
				}

				if !stat.IsDir() {
					foundWritePartner := false
					// Check if there's a corresponding write event for the same path
					logDebug("Create: Entering Write partner check", "signature", action.Signature())
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							logDebug("Create: Found subsequent WRITE partner, marking Write processed", "createSig", action.Signature(), "writeSig", relatedAction.Signature())
							processedPaths[relatedAction.Signature()] = true
							foundWritePartner = true
							// We found the write event, so we can stop checking.
							break
						}
					}
					logDebug("Create: Exited Write partner check", "signature", action.Signature(), "foundWritePartner", foundWritePartner)
					// Note: Even if foundWritePartner is true, we still emit the Create event.
					// The write event itself is simply marked processed.
				}

				// Now we raise the create event normally
				logDebug("Create: Emitting final CREATE event", "signature", action.Signature(), "event", action)
				action.EnrichFromInfo(info) // Enrich with info we already have
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}
			logDebug("Create: Exiting handler", "signature", action.Signature())

		case Rename:
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases:
			// 1. When a file/directory is renamed or moved within watched areas:
			//    - macOS emits: CREATE (new path, same ID) + RENAME (old path, same ID)
			// 2. When a file/directory is moved outside watched areas (or to Trash):
			//    - macOS emits: RENAME (old path only)

			// So, we need to check if there's a corresponding Create event for the same file identifier
			// If there is one, we ignore the Create event and only raise the Rename event
			// Otherwise, we ignore the Rename event and raise a Remove event

			// Get the file info from the watchmap
			renamedFileInfo := b.watchmap.Get(action.Path)
			logDebug("Rename: Processing. Dumping watchmap BEFORE Get:")
			b.PrintMap()
			if renamedFileInfo == nil {
				logDebug("Rename: Cannot find info for", "path", action.Path)
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
						logDebug("Rename: Found CREATE partner", "createSig", relatedCreate.Signature(), "renameSig", action.Signature())
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = renamedFileInfo.Path
						result.EnrichFromInfo(createdFileInfo)
						logDebug("Rename: Emitting synthesized RENAME", "event", result)
						b.emitEvent(result)
						b.watchmap.Delete(renamedFileInfo.Path)
						b.watchmap.Set(createdFileInfo.Path, createdFileInfo)
						logDebug("Rename: Updated watchmap: Deleted", "path", renamedFileInfo.Path, "Set", createdFileInfo.Path)
						processedPaths[action.Signature()] = true
						processedPaths[relatedCreate.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			if !isRenameOrMove {
				// No corresponding create event found, raise a Remove event
				logDebug("Rename: No CREATE partner found for RENAME", "path", action.Path)
				// --- Log watchmap state before potential delete ---
				logDebug("Rename: Watchmap state before potential delete of", "path", renamedFileInfo.Path)
				b.PrintMap()
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				logDebug("Rename: Emitting synthesized REMOVE", "event", result)
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
					logDebug("Write: Found later Write, marking earlier processed", "earlierSig", latestModify.Signature(), "laterSig", relatedModify.Signature())
					latestModify = relatedModify
				}
			}

			// Check if the file exists on disk
			stat, err := os.Stat(latestModify.Path)
			if err != nil {
				// If the file doesn't exist on disk, remove it from the watchmap and ignore the event
				// as there must be a corresponding Remove/Rename event.
				if os.IsNotExist(err) {
					b.watchmap.Delete(latestModify.Path)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Then check if there is a Create event for the same path, ignore the Write event and raise the Create event instead
			// This handles the case where macOS emits a Create event followed by a Write event for a new non-empty file
			// or a copy/move from an unwatched directory into a watched one.
			foundCreated := false
			logDebug("Write: Entering Create partner check", "signature", latestModify.Signature())
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// We found the Create event, ignore the Write event and raise the Create event instead
					// But first we need to check if this specific create event was already processed
					if !processedPaths[relatedCreate.Signature()] {
						logDebug("Write: Found preceding CREATE partner", "createSig", relatedCreate.Signature(), "writeSig", latestModify.Signature())
						// But first we need to check if this specific create event was already processed
						if processedPaths[relatedCreate.Signature()] {
							logDebug("Write: Preceding CREATE partner was already processed. Skipping Create emit.", "createSig", relatedCreate.Signature())
						} else {
							logDebug("Write: Emitting preceding CREATE event instead of Write", "createSig", relatedCreate.Signature(), "writeSig", latestModify.Signature())
							relatedCreate.EnrichFromInfo(FromOSInfo(latestModify.Path, stat))
							b.emitEvent(relatedCreate)
							processedPaths[relatedCreate.Signature()] = true
						}
						processedPaths[latestModify.Signature()] = true
						foundCreated = true
						break
					}
				}
			}
			logDebug("Write: Exited Create partner check", "signature", latestModify.Signature(), "foundCreated", foundCreated)

			// Otherwise, Process the latest Write event
			if !foundCreated {
				logDebug("Write: Processing as standalone Write", "signature", latestModify.Signature())
				logDebug("Write: Watchmap state before Set/Enrich/Emit:", "signature", latestModify.Signature())
				b.PrintMap()
				info := FromOSInfo(latestModify.Path, stat)
				if info != nil {
					logDebug("Write: Updating watchmap with info", "signature", latestModify.Signature(), "info", info)
					b.watchmap.Set(latestModify.Path, info)
					latestModify.EnrichFromInfo(info)
				}
				logDebug("Write: Emitting WRITE event", "signature", latestModify.Signature(), "event", latestModify)
				b.emitEvent(latestModify)
				processedPaths[latestModify.Signature()] = true
			}

		case Chmod:
			logDebug("Chmod: Entering handler", "signature", action.Signature())
			// Emit the chmod event if configured
			if b.config.EmitChmod {
				logDebug("Chmod: EmitChmod enabled, emitting CHMOD event", "signature", action.Signature(), "event", action)
				b.emitEvent(action)
			}

			// Mark the action as processed anyway, because it's a chmod event
			processedPaths[action.Signature()] = true
			logDebug("Chmod: Marked self as processed", "signature", action.Signature())

			// Handle case where writing empty file in macOS results in no modify event, but only in chmod event
			stat, err := os.Stat(action.Path)
			if err != nil {
				slog.Warn("Chmod: os.Stat failed during empty file check", "signature", action.Signature(), "path", action.Path, "error", err)
				continue
			}
			potentialModify := b.watchmap.Get(action.Path)
			if potentialModify != nil {
				if stat.Size() == 0 && potentialModify.Size != 0 {
					// Raise a write event
					logDebug("Chmod: Detected file cleared via chmod, synthesizing Write", "signature", action.Signature())
					modified := NewFSEvent(Write, action.Path, action.Timestamp)
					modified.EnrichFromInfo(FromOSInfo(action.Path, stat))
					b.emitEvent(modified)
				}

				// Update the watchmap with the new file information (i.e. after the chmod)
				logDebug("Chmod: Updating watchmap with post-chmod info", "signature", action.Signature())
				b.watchmap.Set(action.Path, FromOSInfo(action.Path, stat))
			}

		default:
			slog.Warn("Unknown action type in resolveAndHandle", "typeValue", action.Type, "signature", action.Signature())
			// Unreachable
		}
	}

	// Remove items from watchmap
	logDebug("Applying deferred watchmap removals", "count", len(watchmapItemsToRemove))
	for path := range watchmapItemsToRemove {
		logDebug("Removing item from watchmap", "path", path)
		b.watchmap.Delete(path)
	}

	logDebug("Loop End", "processedCount", len(processedPaths))
}

// isSystemFile checks if the file is a common macOS system or metadata file.
func isSystemFile(name string) bool {
	// Base filenames commonly generated by macOS
	base := strings.ToLower(filepath.Base(name))
	switch base {
	case ".ds_store", // Finder metadata
		".appledouble",    // Stores resource forks and HFS+ metadata
		".spotlight-v100", // Spotlight indexing folder
		// Common Unix/Shell/User files also found on macOS
		".bash_history", ".bash_logout", ".bash_profile", ".bashrc", ".profile",
		".zshrc", ".zhistory",
		".cache", ".config",
		".viminfo",
		// macOS specific files
		".temporaryitems",          // Temporary items cache
		".trashes",                 // User trash folder
		".fseventsd",               // Filesystem event log daemon data
		".volumeicon.icns",         // Custom volume icon
		"icon\r",                   // Custom icon representation (note the escaped )
		".documentrevisions-v100",  // Versions database
		".pkinstallsandboxmanager", // Related to package installations
		".apdisk":                  // Related to APFS snapshots or Time Machine?
		return true
	}

	// Check for common patterns
	// Editor backup/swap files
	if strings.HasSuffix(base, "~") || // Common backup suffix (vim, emacs, etc.)
		(strings.HasPrefix(base, "#") && strings.HasSuffix(base, "#")) || // Emacs autosave
		(strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".swp")) || // Vim swap file
		(strings.HasPrefix(base, ".") && strings.HasSuffix(base, ".swo")) { // Vim swap file (newer)
		return true
	}

	// Python bytecode
	if strings.HasSuffix(base, ".pyc") {
		return true
	}

	// Match patterns for resource fork or metadata files
	return strings.HasPrefix(base, "._") || base == ".com.apple.timemachine.donotpresent"
}

// isHiddenFile checks if a file is hidden on Unix-like systems.
func isHiddenFile(path string) (bool, error) {
	baseName := filepath.Base(path)
	if len(baseName) > 0 && baseName[0] == '.' && baseName != "." && baseName != ".." {
		return true, nil // Files starting with a dot are conventionally hidden
	}

	// On macOS (and other Unix-like systems), the primary convention for hidden files
	// is that their name starts with a dot.
	// Checking the UF_HIDDEN flag via Getattrlist is complex and often requires `unsafe`,
	// so we rely on the standard dot-prefix convention.
	return false, nil
}

func FromOSInfo(path string, fileinfo os.FileInfo) *Info {
	sys := fileinfo.Sys()
	sysstat, ok := sys.(*syscall.Stat_t)
	if !ok {
		return nil
	}

	return &Info{
		Id:      sysstat.Ino,
		Path:    path,
		Size:    uint64(fileinfo.Size()),
		ModTime: fileinfo.ModTime(),
		Mode:    uint32(fileinfo.Mode()),
	}
}
