//go:build linux
// +build linux

package fsbroker

import (
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

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			// Note: deletedFile might be nil if the remove event is for a file
			// that was created and removed within the same processing window before
			// it was added to the watchmap by its Create event handler.

			// Process the Remove event normally (hard delete on Linux)
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
			// Remove events on Linux are typically hard deletes
			action.Properties["Type"] = "Hard"

			b.emitEvent(action)
			processedPaths[action.Signature()] = true

		case Create:
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longer exists. Remove it from the watchmap if it exists
					b.watchmap.Delete(action.Path)
				}
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

			// check for preceding Rename event (Linux Rename/Move pattern: RENAME(old) -> CREATE(new))
			if !isRenameOrMove {
				for _, relatedAction := range eventList {
					// Look for RENAME *before* this CREATE
					if relatedAction.Type == Rename && relatedAction.Timestamp.Before(action.Timestamp) {
						potentialRename := b.watchmap.Get(relatedAction.Path)
						// Check potentialRename is not nil before accessing Id
						if potentialRename != nil && potentialRename.Id == info.Id {
							// We found the matching RENAME event
							result := NewFSEvent(Rename, action.Path, action.Timestamp)
							result.Properties["OldPath"] = potentialRename.Path
							result.EnrichFromInfo(info)
							b.emitEvent(result)
							processedPaths[action.Signature()] = true
							processedPaths[relatedAction.Signature()] = true
							isRenameOrMove = true

							// Update watchmap
							b.watchmap.Delete(potentialRename.Path)
							b.watchmap.Set(action.Path, info)

							break // Found the match
						}
					}
				}
			}

			// If it wasn't a rename/move, process as a standard Create
			if !isRenameOrMove {
				// Handle directory creation and watching
				if b.watchrecursive && stat.IsDir() {
					_ = b.AddWatch(action.Path)
				} else if !stat.IsDir() {
					// Add file to watchmap
					b.watchmap.Set(action.Path, info)
				}

				// Check for subsequent Write events (non-empty file creation)
				if !stat.IsDir() {
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							processedPaths[relatedAction.Signature()] = true
							// Ignore this write as part of the create
							break
						}
					}
				}

				// Emit the create event
				action.EnrichFromInfo(info)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

		case Rename:
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases on Linux:
			// 1. Rename/Move within watched area: RENAME(old) + CREATE(new)
			// 2. Move out/Soft delete: RENAME(old)

			// Get the file info from the watchmap for the old path
			renamedFileInfo := b.watchmap.Get(action.Path)
			if renamedFileInfo == nil {
				// Cannot correlate if not in map. Treat as soft delete.
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				b.emitEvent(result)
				processedPaths[action.Signature()] = true
				continue
			}

			isRenameOrMove := false
			// Check for a corresponding Create event *after* this Rename event
			for _, relatedCreate := range eventList {
				if relatedCreate.Type == Create && relatedCreate.Timestamp.After(action.Timestamp) {
					createdStat, err := os.Stat(relatedCreate.Path)
					if err != nil {
						continue
					}
					createdFileInfo := FromOSInfo(relatedCreate.Path, createdStat)
					if createdFileInfo != nil && createdFileInfo.Id == renamedFileInfo.Id {
						// Found the CREATE part of the rename/move
						processedPaths[action.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}

			if !isRenameOrMove {
				// No corresponding create event found => Soft Delete / Move Out
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				b.emitEvent(result)
				// Clean up map for the old path
				b.watchmap.Delete(action.Path)
				processedPaths[action.Signature()] = true
			}

		case Write:
			// Deduplicate writes: only process the latest one for a given path in this tick
			latestModify := action
			for _, relatedModify := range eventList {
				if relatedModify.Path == action.Path && relatedModify.Type == Write && relatedModify.Timestamp.After(latestModify.Timestamp) {
					processedPaths[action.Signature()] = true // Mark current action processed
					latestModify = relatedModify              // Update latestModify to the later one
				}
			}

			// If the latestModify event itself was marked processed (e.g., by an even later write), skip.
			if processedPaths[latestModify.Signature()] {
				continue
			}

			// Check if the file exists on disk
			stat, err := os.Stat(latestModify.Path)
			if err != nil {
				// Ignore write if file doesn't exist (must be related to a remove/rename)
				if os.IsNotExist(err) {
					b.watchmap.Delete(latestModify.Path)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Check if this Write corresponds to a Create (non-empty file creation)
			foundCreated := false
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// Write is part of Create sequence. Ignore the Write; Create handler deals with it.
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}

			// If not part of a Create, emit the Write event
			if !foundCreated {
				info := FromOSInfo(latestModify.Path, stat)
				if info != nil {
					// Update map with potentially changed info (mod time, size)
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

			// Mark the action as processed anyway, whether emitted or not
			processedPaths[action.Signature()] = true

		default:
			// Unreachable
		}
	}
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
