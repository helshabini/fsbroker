//go:build linux
// +build linux

package fsbroker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func (b *FSBroker) resolveAndHandle(eventQueue *EventQueue, tickerLock *sync.Mutex) {
	// We only want one instance of this method to run at a time, so we lock it
	if !tickerLock.TryLock() {
		return
	}
	defer tickerLock.Unlock()

	// Process grouped events, detecting related Create and Rename events
	processedPaths := make(map[string]bool)

	// items to be removed from watchmap
	watchmapItemsToRemove := make(map[string]bool)

	// temporary list of events to process
	eventList := eventQueue.List()

	fmt.Printf("[DEBUG - Linux] Initial eventList snapshot (Size: %d):\n", len(eventList))
	for _, evt := range eventList {
		fmt.Printf("  - %s (Time: %s)\n", evt.Signature(), evt.Timestamp.Format(time.RFC3339Nano))
	}

	if len(eventList) == 0 {
		return
	}

	//Now we process the events remaining in the event queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		fmt.Printf("[DEBUG - Linux] --- Loop Start: Popped action: %s (Time: %s) ---\n", action.Signature(), action.Timestamp.Format(time.RFC3339Nano))
		// Ignore already processed paths
		if processedPaths[action.Signature()] {
			fmt.Printf("[DEBUG - Linux] Loop Skip: Action %s already processed.\n", action.Signature())
			continue
		}

		switch action.Type {
		case Remove:
			fmt.Printf("[DEBUG - Linux - Remove %s] Entering Remove handler.\n", action.Signature())
			// Check if there's any earlier event for the same path within the queue, ignore it and only raise the remove event
			for _, relatedAction := range eventList {
				if relatedAction.Path == action.Path && relatedAction.Timestamp.Before(action.Timestamp) {
					processedPaths[relatedAction.Signature()] = true
					fmt.Printf("[DEBUG - Linux - Remove %s] Marked earlier action %s as processed.\n", action.Signature(), relatedAction.Signature())
				}
			}

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			fmt.Printf("[DEBUG - Linux - Remove %s] Info from watchmap: %+v\n", action.Signature(), deletedFile)

			// Process the Remove event normally (hard delete on Linux)
			// Check if the deleted item was a directory, and if so, remove the watch
			if deletedFile != nil && deletedFile.IsDir() { // Check deletedFile is not nil
				fmt.Printf("[DEBUG - Linux - Remove %s] Path is a directory, calling RemoveWatch.\n", action.Signature())
				b.RemoveWatch(action.Path)
				// No need to delete from watchmap here, RemoveWatch handles prefix deletion
			} else {
				// Ensure regular files are also removed from map on remove, only if deletedFile was found
				if deletedFile != nil {
					fmt.Printf("[DEBUG - Linux - Remove %s] Path is a file, adding to deferred removal list.\n", action.Signature())
					watchmapItemsToRemove[action.Path] = true
				}
			}

			if deletedFile != nil { // Only enrich if we have info
				fmt.Printf("[DEBUG - Linux - Remove %s] Enriching event from watchmap info.\n", action.Signature())
				action.EnrichFromInfo(deletedFile)
			}
			// Remove events on Linux are typically hard deletes
			action.Properties["Type"] = "Hard"

			fmt.Printf("[DEBUG - Linux - Remove %s] Emitting Hard Remove event: %v\n", action.Signature(), action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true
			fmt.Printf("[DEBUG - Linux - Remove %s] Marked self as processed.\n", action.Signature())

		case Create:
			fmt.Printf("[DEBUG - Linux - Create %s] Entering Create handler.\n", action.Signature())
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longer exists. Remove it from the watchmap if it exists
					fmt.Printf("[DEBUG - Linux - Create %s] os.Stat failed (NotExist), adding to deferred removal list and skipping.\n", action.Signature())
					watchmapItemsToRemove[action.Path] = true
				} else {
					fmt.Printf("[DEBUG - Linux - Create %s] os.Stat failed (%v), skipping.\n", action.Signature(), err)
				}
				processedPaths[action.Signature()] = true // Mark processed even on error
				continue
			}
			fmt.Printf("[DEBUG - Linux - Create %s] os.Stat successful.\n", action.Signature())

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				fmt.Printf("[DEBUG - Linux - Create %s] FromOSInfo returned nil after successful stat. Enriching from stat and emitting raw Create.\n", action.Signature())
				// We failed to get the file info (maybe permission error or transient issue), enrich with what we have
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}
			fmt.Printf("[DEBUG - Linux - Create %s] Got Info: %+v\n", action.Signature(), info)

			var isRenameOrMove bool = false // Reset or ensure it's defined here

			// check for preceding Rename event (Linux Rename/Move pattern: RENAME(old) -> CREATE(new))
			fmt.Printf("[DEBUG - Linux - Create %s] Entering Rename check block.\n", action.Signature())
			for _, relatedAction := range eventList {
				// Look for RENAME *before* this CREATE
				if relatedAction.Type == Rename && relatedAction.Timestamp.Before(action.Timestamp) {
					fmt.Printf("[DEBUG - Linux - Create %s] Checking potential RENAME partner: %s\n", action.Signature(), relatedAction.Signature())
					potentialRename := b.watchmap.Get(relatedAction.Path)
					// Check potentialRename is not nil before accessing Id
					if potentialRename != nil && potentialRename.Id == info.Id {
						// We found the matching RENAME event
						fmt.Printf("[DEBUG - Linux - Create %s] Found RENAME partner %s via ID match (%d). Synthesizing RENAME.\n", action.Signature(), relatedAction.Signature(), info.Id)
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = potentialRename.Path
						result.EnrichFromInfo(info)
						fmt.Printf("[DEBUG - Linux - Create %s] Emitting synthesized RENAME: %v\n", action.Signature(), result)
						b.emitEvent(result)
						processedPaths[action.Signature()] = true
						processedPaths[relatedAction.Signature()] = true
						fmt.Printf("[DEBUG - Linux - Create %s] Marked self (%s) and partner (%s) as processed.\n", action.Signature(), action.Signature(), relatedAction.Signature())
						isRenameOrMove = true

						// Update watchmap
						fmt.Printf("[DEBUG - Linux - Create %s] Updating watchmap: Deleting old path %s (deferred), Setting new path %s\n", action.Signature(), potentialRename.Path, action.Path)
						watchmapItemsToRemove[potentialRename.Path] = true
						b.watchmap.Set(action.Path, info)

						break // Found the match
					}
				}
			}
			fmt.Printf("[DEBUG - Linux - Create %s] Exited Rename check block. isRenameOrMove=%t\n", action.Signature(), isRenameOrMove)

			// If it wasn't a rename/move, process as a standard Create
			if !isRenameOrMove {
				fmt.Printf("[DEBUG - Linux - Create %s] Processing as standard Create.\n", action.Signature())
				// Check for subsequent Write events (non-empty file creation)
				if !info.IsDir() {
					fmt.Printf("[DEBUG - Linux - Create %s] Entering Write partner check block.\n", action.Signature())
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							processedPaths[relatedAction.Signature()] = true
							fmt.Printf("[DEBUG - Linux - Create %s] Found subsequent WRITE partner %s. Marked WRITE processed.\n", action.Signature(), relatedAction.Signature())
							// Ignore this write as part of the create
							break
						}
					}
					fmt.Printf("[DEBUG - Linux - Create %s] Exited Write partner check block.\n", action.Signature())
				}

				// Emit the create event
				fmt.Printf("[DEBUG - Linux - Create %s] Enriching event with info.\n", action.Signature())
				action.EnrichFromInfo(info)
				fmt.Printf("[DEBUG - Linux - Create %s] Updating watchmap with final info for path %s.\n", action.Signature(), action.Path)
				b.watchmap.Set(action.Path, info)
				fmt.Printf("[DEBUG - Linux - Create %s] Emitting final CREATE event: %v\n", action.Signature(), action)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				fmt.Printf("[DEBUG - Linux - Create %s] Marked self as processed.\n", action.Signature())
			}

		case Rename:
			fmt.Printf("[DEBUG - Linux - Rename %s] Entering Rename handler.\n", action.Signature())
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases on Linux:
			// 1. Rename/Move within watched area: RENAME(old) + CREATE(new)
			// Get the file info from the watchmap for the old path
			renamedFileInfo := b.watchmap.Get(action.Path)
			if renamedFileInfo == nil {
				// Cannot correlate if not in map. Treat as soft delete.
				fmt.Printf("[DEBUG - Linux - Rename %s] Info not found in watchmap. Synthesizing Soft Remove.\n", action.Signature())
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				fmt.Printf("[DEBUG - Linux - Rename %s] Emitting synthesized Soft Remove: %v\n", action.Signature(), result)
				b.emitEvent(result)
				processedPaths[action.Signature()] = true
				continue
			}
			fmt.Printf("[DEBUG - Linux - Rename %s] Found info in watchmap for old path: %+v\n", action.Signature(), renamedFileInfo)

			isRenameOrMove := false
			// Check for a corresponding Create event *after* this Rename event
			fmt.Printf("[DEBUG - Linux - Rename %s] Entering Create partner check block.\n", action.Signature())
			for _, relatedCreate := range eventList {
				if relatedCreate.Type == Create && relatedCreate.Timestamp.After(action.Timestamp) {
					fmt.Printf("[DEBUG - Linux - Rename %s] Checking potential CREATE partner: %s\n", action.Signature(), relatedCreate.Signature())
					createdStat, err := os.Stat(relatedCreate.Path)
					if err != nil {
						fmt.Printf("[DEBUG - Linux - Rename %s] os.Stat failed for potential partner %s (%v). Skipping partner.\n", action.Signature(), relatedCreate.Path, err)
						continue
					}
					createdFileInfo := FromOSInfo(relatedCreate.Path, createdStat)
					if createdFileInfo != nil && createdFileInfo.Id == renamedFileInfo.Id {
						// Found the CREATE part of the rename/move
						fmt.Printf("[DEBUG - Linux - Rename %s] Found CREATE partner %s via ID match (%d). Marking Rename as processed (Create handler will emit).\n", action.Signature(), relatedCreate.Signature(), renamedFileInfo.Id)
						processedPaths[action.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			fmt.Printf("[DEBUG - Linux - Rename %s] Exited Create partner check block. isRenameOrMove=%t\n", action.Signature(), isRenameOrMove)

			if !isRenameOrMove {
				// No corresponding create event found => Soft Delete / Move Out
				fmt.Printf("[DEBUG - Linux - Rename %s] No CREATE partner found. Synthesizing Soft Remove.\n", action.Signature())
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				fmt.Printf("[DEBUG - Linux - Rename %s] Emitting synthesized Soft Remove: %v\n", action.Signature(), result)
				b.emitEvent(result)
				// Clean up map for the old path
				fmt.Printf("[DEBUG - Linux - Rename %s] Adding old path %s to deferred removal list.\n", action.Signature(), action.Path)
				watchmapItemsToRemove[action.Path] = true
				processedPaths[action.Signature()] = true
				fmt.Printf("[DEBUG - Linux - Rename %s] Marked self as processed.\n", action.Signature())
			}

		case Write:
			fmt.Printf("[DEBUG - Linux - Write %s] Entering Write handler.\n", action.Signature())
			// Deduplicate writes: only process the latest one for a given path in this tick
			latestModify := action
			for _, relatedModify := range eventList {
				if relatedModify.Path == action.Path && relatedModify.Type == Write && relatedModify.Timestamp.After(latestModify.Timestamp) {
					processedPaths[action.Signature()] = true // Mark current action processed
					fmt.Printf("[DEBUG - Linux - Write %s] Found later Write %s. Marking self processed, updating latestModify.\n", action.Signature(), relatedModify.Signature())
					latestModify = relatedModify // Update latestModify to the later one
				}
			}

			// If the latestModify event itself was marked processed (e.g., by an even later write), skip.
			if processedPaths[latestModify.Signature()] {
				// Need to check if latestModify *is* the current action. If action was marked processed by a later write, action != latestModify.
				// If action *is* latestModify, but it's already processed (e.g. by Create+Write handling), then skip.
				if latestModify.Signature() == action.Signature() {
					fmt.Printf("[DEBUG - Linux - Write %s] Skipping: Event was latest write but already marked processed.\n", action.Signature())
				} else {
					// This case should theoretically not be hit because the outer check `if processedPaths[action.Signature()]` would catch it.
					// If it is hit, it means the *original* action was processed, likely because a later write was found.
					fmt.Printf("[DEBUG - Linux - Write %s] Skipping: Event was superseded by later write %s.\n", action.Signature(), latestModify.Signature())
				}
				continue
			}
			fmt.Printf("[DEBUG - Linux - Write %s] Processing as latest write.\n", latestModify.Signature())

			// Check if the file exists on disk
			stat, err := os.Stat(latestModify.Path)
			if err != nil {
				// Ignore write if file doesn't exist (must be related to a remove/rename)
				if os.IsNotExist(err) {
					fmt.Printf("[DEBUG - Linux - Write %s] os.Stat failed (NotExist). Adding to deferred removal and skipping.\n", latestModify.Signature())
					watchmapItemsToRemove[latestModify.Path] = true
				} else {
					fmt.Printf("[DEBUG - Linux - Write %s] os.Stat failed (%v). Skipping.\n", latestModify.Signature(), err)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}
			fmt.Printf("[DEBUG - Linux - Write %s] os.Stat successful.\n", latestModify.Signature())

			// Check if this Write corresponds to a Create (non-empty file creation)
			foundCreated := false
			fmt.Printf("[DEBUG - Linux - Write %s] Entering Create partner check block.\n", latestModify.Signature())
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// Write is part of Create sequence. Ignore the Write; Create handler deals with it.
					fmt.Printf("[DEBUG - Linux - Write %s] Found preceding CREATE partner %s. Marking self processed and skipping.\n", latestModify.Signature(), relatedCreate.Signature())
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}
			fmt.Printf("[DEBUG - Linux - Write %s] Exited Create partner check block. foundCreated=%t\n", latestModify.Signature(), foundCreated)

			// If not part of a Create, emit the Write event
			if !foundCreated {
				fmt.Printf("[DEBUG - Linux - Write %s] Processing as standalone Write.\n", latestModify.Signature())
				info := FromOSInfo(latestModify.Path, stat)
				if info != nil {
					// Update map with potentially changed info (mod time, size)
					fmt.Printf("[DEBUG - Linux - Write %s] Updating watchmap with info: %+v\n", latestModify.Signature(), info)
					b.watchmap.Set(latestModify.Path, info)
					latestModify.EnrichFromInfo(info)
				}
				fmt.Printf("[DEBUG - Linux - Write %s] Emitting WRITE event: %v\n", latestModify.Signature(), latestModify)
				b.emitEvent(latestModify)
				processedPaths[latestModify.Signature()] = true
				fmt.Printf("[DEBUG - Linux - Write %s] Marked self as processed.\n", latestModify.Signature())
			}

		case Chmod:
			fmt.Printf("[DEBUG - Linux - Chmod %s] Entering Chmod handler.\n", action.Signature())
			// Emit the chmod event if configured
			if b.config.EmitChmod {
				fmt.Printf("[DEBUG - Linux - Chmod %s] EmitChmod enabled. Emitting CHMOD event: %v\n", action.Signature(), action)
				b.emitEvent(action)
			}

			// Mark the action as processed anyway, whether emitted or not
			processedPaths[action.Signature()] = true
			fmt.Printf("[DEBUG - Linux - Chmod %s] Marked self as processed.\n", action.Signature())

		default:
			fmt.Printf("[WARN - Linux] Unknown action type in resolveAndHandle: %d\n", action.Type)
			// Unreachable
		}
	}

	// Remove items from watchmap
	fmt.Printf("[DEBUG - Linux] Applying deferred watchmap removals for %d paths.\n", len(watchmapItemsToRemove))
	for path := range watchmapItemsToRemove {
		fmt.Printf("  - Removing %s\n", path)
		b.watchmap.Delete(path)
	}
	fmt.Printf("[DEBUG - Linux] --- Loop End --- Final processed paths count: %d ---\n", len(processedPaths))
}

// isSystemFile checks if the file is a common Linux system or temporary file.
func isSystemFile(name string) bool {
	base := strings.ToLower(filepath.Base(name))

	// Check for exact matches of common config/cache/system files/dirs
	switch base {
	// Shell config/history
	case ".bash_history", ".bash_logout", ".bash_profile", ".bashrc", ".profile",
		".login", ".sudo_as_admin_successful", ".xauthority", ".xsession-errors",
		".viminfo", ".cache", ".config", ".local", ".dbus", ".gvfs",
		".recently-used", ".fontconfig", ".iceauthority":
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

	// Patterns for temporary GNOME/GTK files and trash directories
	if strings.HasPrefix(base, ".goutputstream-") || // GNOME/GTK temp stream files
		strings.HasPrefix(base, ".trash-") { // Files being moved to trash
		return true
	}

	return false
}

// isHiddenFile checks if a file is hidden on Unix-like systems.
func isHiddenFile(path string) (bool, error) {
	baseName := filepath.Base(path)
	if len(baseName) > 0 && baseName[0] == '.' && baseName != "." && baseName != ".." {
		return true, nil // Files starting with a dot are conventionally hidden
	}

	return false, nil
}

func FromOSInfo(path string, fileinfo os.FileInfo) *Info {
	sys := fileinfo.Sys()
	sysstat, ok := sys.(*syscall.Stat_t)
	if !ok {
		return nil
	}

	return &Info{
		Id:   sysstat.Ino,
		Path: path,
		Mode: uint32(fileinfo.Mode()),
	}
}
