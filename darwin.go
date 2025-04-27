//go:build darwin
// +build darwin

package fsbroker

import (
	"fmt"
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

	fmt.Printf("[DEBUG] Initial eventList snapshot (Size: %d):\n", len(eventList))
	for _, evt := range eventList {
		fmt.Printf("  - %s\n", evt.Signature())
	}

	if len(eventList) == 0 {
		return
	}

	//Now we process the events remaining in the event queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		// Ignore already processed paths
		fmt.Printf("[DEBUG] Loop Start: Popped action: Type=%s, Path=%s, Sig=%s\n", action.Type.String(), action.Path, action.Signature())
		if processedPaths[action.Signature()] {
			fmt.Printf("[DEBUG] Loop Skip: Action %s already processed.\n", action.Signature())
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

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			// Note: deletedFile might be nil if the remove event is for a file
			// that was created and removed within the same processing window before
			// it was added to the watchmap by its Create event handler.

			// Process the Remove event normally (hard delete on macOS)
			// Check if the deleted item was a directory, and if so, remove the watch
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
			// Assuming hard delete, because macOS doesn't emit a Remove event for soft deletes
			action.Properties["Type"] = "Hard"

			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Create:
			fmt.Printf("[DEBUG - Create %s] Entering Create handler\n", action.Signature())
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longer exists. Remove it from the watchmap if it exists
					fmt.Printf("[DEBUG - Create %s] os.Stat failed (NotExist), removing from watchmap and skipping.\n", action.Signature())
					watchmapItemsToRemove[action.Path] = true
				} else {
					fmt.Printf("[DEBUG - Create %s] os.Stat failed (%v), skipping.\n", action.Signature(), err)
				}
				processedPaths[action.Signature()] = true
				continue
			}
			fmt.Printf("[DEBUG - Create %s] os.Stat successful.\n", action.Signature())

			info := FromOSInfo(action.Path, stat)
			fmt.Printf("[DEBUG - Create %s] Path %s got info: %+v\n", action.Signature(), action.Path, info)
			if info == nil {
				// We failed to get the file info (maybe permission error or transient issue), enrich with what we have
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			var isRenameOrMove bool = false // Reset or ensure it's defined here

			// Check for following Rename event (Unix Rename/Move pattern)

			fmt.Printf("[DEBUG - Create %s] Entering Rename check block. isRenameOrMove=%t\n", action.Signature(), isRenameOrMove)
			for _, relatedAction := range eventList {
				// Look for RENAME *after* this CREATE
				if relatedAction.Type == Rename && relatedAction.Timestamp.After(action.Timestamp) { // Check time relationship if needed
					fmt.Printf("[DEBUG - Create %s] Checking potential RENAME partner: %s\n", action.Signature(), relatedAction.Signature())
					// --- Log Watchmap State BEFORE potential rename handling ---
					b.PrintMap()
					potentialRename := b.watchmap.Get(relatedAction.Path)
					fmt.Printf("[DEBUG - Create %s] Info for potential old path %s from watchmap: %+v\n", action.Signature(), relatedAction.Path, potentialRename)
					// Safely log IDs, handling potential nil pointer for potentialRename
					var potentialRenameId uint64
					if potentialRename != nil {
						potentialRenameId = potentialRename.Id
					}
					idsMatch := potentialRename != nil && potentialRename.Id == info.Id
					fmt.Printf("[DEBUG - Create %s] Comparing IDs: potentialRename.Id (%d) == info.Id (%d) -> %t\n", action.Signature(), potentialRenameId, info.Id, idsMatch)
					if idsMatch {
						// We found the rename event, ignore the create event and only raise the Rename event
						fmt.Printf("[DEBUG - Create] Found RENAME partner %s for CREATE %s via ID match (%d). Synthesizing.\n", relatedAction.Signature(), action.Signature(), info.Id)
						result := NewFSEvent(Rename, action.Path, action.Timestamp) // Use NEW path/time
						result.Properties["OldPath"] = potentialRename.Path         // Store OLD path
						result.EnrichFromInfo(info)                                 // Enrich with NEW info
						fmt.Printf("[DEBUG - Create] Emitting synthesized RENAME: %v\n", result)
						b.emitEvent(result)
						processedPaths[action.Signature()] = true
						processedPaths[relatedAction.Signature()] = true
						fmt.Printf("[DEBUG - Create %s] Updating watchmap: Deleting %s, Setting %s\n", action.Signature(), potentialRename.Path, action.Path)
						isRenameOrMove = true

						// Update watchmap
						b.watchmap.Delete(potentialRename.Path)
						b.watchmap.Set(action.Path, info)

						break
					}
				}
			}

			fmt.Printf("[DEBUG - Create %s] Exited Rename check block. isRenameOrMove=%t\n", action.Signature(), isRenameOrMove)

			// Now that we're sure it is not a rename or move. We need to check if it is non-empty file creation.
			// If it is, there should be a corresponding write event which we need to ignore.
			// We also need to check if it is a directory creation. If it is, we need to add a watch for it. Otherwise, we add the file to the watchmap.
			// TODO: Make this optional. Maybe users want to also get write events for non-empty files?
			if !isRenameOrMove {
				fmt.Printf("[DEBUG - Create %s] Processing as normal Create (not rename/move). isRenameOrMove=%t\n", action.Signature(), isRenameOrMove)
				// if b.watchrecursive && stat.IsDir() {
				// 	// Make sure AddWatch doesn't error due to existing map entry if called twice
				// 	_ = b.AddWatch(action.Path) // Add watch if not already present
				// 	fmt.Printf("[DEBUG - Create %s] Added recursive watch for directory %s\n", action.Signature(), action.Path)
				// } else {
				// 	// Update watchmap only if it's not a directory being handled by AddWatch
				// 	if !stat.IsDir() {
				// 		fmt.Printf("[DEBUG - Create %s] Updating watchmap: Setting %s (not a dir)\n", action.Signature(), action.Path)
				// 		b.watchmap.Set(action.Path, info)
				// 	}
				// }

				if !stat.IsDir() {
					foundWritePartner := false
					// Check if there's a corresponding write event for the same path
					fmt.Printf("[DEBUG - Create %s] Entering Write partner check block.\n", action.Signature())
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							fmt.Printf("[DEBUG - Create %s] Found subsequent WRITE partner %s. Marking WRITE processed.\n", action.Signature(), relatedAction.Signature())
							processedPaths[relatedAction.Signature()] = true
							foundWritePartner = true
							// We found the write event, so we can stop checking.
							// If there are multiple write events, the next one should
							// be considered a nomral write event that we happened to capture
							// in the same queue frame.
							break
						}
					}
					fmt.Printf("[DEBUG - Create %s] Exited Write partner check block. foundWritePartner=%t\n", action.Signature(), foundWritePartner)
					if foundWritePartner {
						fmt.Printf("[DEBUG - Create %s] Found write partner, emitting CREATE anyway (current logic).\n", action.Signature())
					} else {
						fmt.Printf("[DEBUG - Create %s] No write partner found, emitting CREATE.\n", action.Signature())
					}
				}

				// Now we raise the create event normally
				fmt.Printf("[DEBUG - Create %s] Emitting final CREATE event: %v\n", action.Signature(), action)
				action.EnrichFromInfo(info) // Enrich with info we already have
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

			fmt.Printf("[DEBUG - Create %s] Exiting Create handler.\n", action.Signature())

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
			fmt.Printf("[DEBUG - Rename %s] Processing. Dumping watchmap BEFORE Get:\n", action.Signature())
			b.PrintMap()
			if renamedFileInfo == nil {
				fmt.Printf("[DEBUG - Rename] Cannot find info for %s in watchmap. Emitting raw Rename.\n", action.Path)
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
						fmt.Printf("[DEBUG - Rename] Found CREATE partner %s for RENAME %s via ID match. Synthesizing.\n", relatedCreate.Signature(), action.Signature())
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = renamedFileInfo.Path
						result.EnrichFromInfo(createdFileInfo)
						fmt.Printf("[DEBUG - Rename] Emitting synthesized RENAME: %v\n", result)
						b.emitEvent(result)
						b.watchmap.Delete(renamedFileInfo.Path)
						b.watchmap.Set(createdFileInfo.Path, createdFileInfo)
						fmt.Printf("[DEBUG - Rename %s] Updated watchmap: Deleted %s, Set %s\n", action.Signature(), renamedFileInfo.Path, createdFileInfo.Path)
						processedPaths[action.Signature()] = true
						processedPaths[relatedCreate.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			if !isRenameOrMove {
				// No corresponding create event found, raise a Remove event
				fmt.Printf("[DEBUG - Rename] No CREATE partner found for RENAME %s. Synthesizing Remove.\n", action.Signature())
				// --- Log watchmap state before potential delete ---
				fmt.Printf("[DEBUG - Rename %s] Watchmap state before potential delete of %s:\n", action.Signature(), renamedFileInfo.Path)
				b.PrintMap()
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				fmt.Printf("[DEBUG - Rename] Emitting synthesized REMOVE: %v\n", result)
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
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// We found the Create event, ignore the Write event and raise the Create event instead
					// But first we need to check if this specific create event was already processed
					if !processedPaths[relatedCreate.Signature()] {
						fmt.Printf("[DEBUG - Write] Found preceding CREATE partner %s for WRITE %s.\n", relatedCreate.Signature(), latestModify.Signature())
						// But first we need to check if this specific create event was already processed
						if processedPaths[relatedCreate.Signature()] {
							fmt.Printf("[DEBUG - Write] Preceding CREATE %s was already processed. Skipping emit.\n", relatedCreate.Signature())
						} else {
							fmt.Printf("[DEBUG - Write] Emitting preceding CREATE event %s instead of WRITE %s\n", relatedCreate.Signature(), latestModify.Signature())
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

			// Otherwise, Process the latest Write event
			if !foundCreated {
				fmt.Printf("[DEBUG - Write] No preceding Create found or Create already processed. Emitting WRITE event: %v\n", latestModify)
				fmt.Printf("[DEBUG - Write %s] Watchmap state before Set/Enrich/Emit:\n", latestModify.Signature())
				b.PrintMap()
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
				b.watchmap.Set(action.Path, FromOSInfo(action.Path, stat))
			}

		default:
			// Unreachable
		}
	}

	// Remove items from watchmap
	for path := range watchmapItemsToRemove {
		b.watchmap.Delete(path)
	}

	fmt.Printf("[DEBUG] Loop End: Final processed paths: %v\n", processedPaths)
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
