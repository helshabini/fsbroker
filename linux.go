//go:build linux
// +build linux

package fsbroker

import (
	"fmt"
	"log/slog"
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

	// Log the initial snapshot if debug is enabled
	if slog.Default().Enabled(nil, slog.LevelDebug) {
		signatures := make([]string, len(eventList))
		for i, evt := range eventList {
			signatures[i] = fmt.Sprintf("%s@%s", evt.Signature(), evt.Timestamp.Format(time.RFC3339Nano))
		}
		logDebug("Initial event list snapshot", "size", len(eventList), "signatures", signatures)
	}

	if len(eventList) == 0 {
		return
	}

	//Now we process the events remaining in the event queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		logDebug("Loop Start: Popped action", "signature", action.Signature(), "type", action.Type.String(), "path", action.Path, "time", action.Timestamp)
		// Ignore already processed paths
		if processedPaths[action.Signature()] {
			logDebug("Loop Skip: Action already processed", "signature", action.Signature())
			continue
		}

		switch action.Type {
		case Remove:
			logDebug("Remove: Entering handler", "signature", action.Signature())
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

			// Process the Remove event normally (hard delete on Linux)
			// Check if the deleted item was a directory, and if so, remove the watch
			if deletedFile != nil && deletedFile.IsDir() { // Check deletedFile is not nil
				logDebug("Remove: Path is directory, calling RemoveWatch", "signature", action.Signature(), "path", action.Path)
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
				logDebug("Remove: Enriching event from watchmap info", "signature", action.Signature())
				action.EnrichFromInfo(deletedFile)
			}
			// Remove events on Linux are typically hard deletes
			action.Properties["Type"] = "Hard"

			logDebug("Remove: Emitting Hard Remove", "signature", action.Signature(), "event", action)
			b.emitEvent(action)
			processedPaths[action.Signature()] = true
			logDebug("Remove: Marked self as processed", "signature", action.Signature())

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
				processedPaths[action.Signature()] = true // Mark processed even on error
				continue
			}
			logDebug("Create: os.Stat successful", "signature", action.Signature(), "path", action.Path)

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				slog.Warn("Create: FromOSInfo returned nil after stat, enriching from stat and emitting raw Create", "signature", action.Signature(), "path", action.Path)
				// We failed to get the file info (maybe permission error or transient issue), enrich with what we have
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}
			logDebug("Create: Got info from stat", "signature", action.Signature(), "info", info)

			var isRenameOrMove bool = false // Reset or ensure it's defined here

			// check for preceding Rename event (Linux Rename/Move pattern: RENAME(old) -> CREATE(new))
			logDebug("Create: Entering Rename partner check", "signature", action.Signature())
			for _, relatedAction := range eventList {
				// Look for RENAME *before* this CREATE
				if relatedAction.Type == Rename && relatedAction.Timestamp.Before(action.Timestamp) {
					logDebug("Create: Checking potential RENAME partner", "createSig", action.Signature(), "renameSig", relatedAction.Signature())
					potentialRename := b.watchmap.Get(relatedAction.Path)
					// Check potentialRename is not nil before accessing Id
					if potentialRename != nil && potentialRename.Id == info.Id {
						// We found the matching RENAME event
						logDebug("Create: Found RENAME partner via ID match, synthesizing Rename", "createSig", action.Signature(), "renameSig", relatedAction.Signature(), "id", info.Id)
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = potentialRename.Path
						result.EnrichFromInfo(info)
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

						break // Found the match
					}
				}
			}
			logDebug("Create: Exited Rename partner check", "signature", action.Signature(), "isRenameOrMove", isRenameOrMove)

			// If it wasn't a rename/move, process as a standard Create
			if !isRenameOrMove {
				logDebug("Create: Processing as standard Create", "signature", action.Signature())
				// Check for subsequent Write events (non-empty file creation)
				if !info.IsDir() {
					logDebug("Create: Entering Write partner check", "signature", action.Signature())
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Path == action.Path && relatedAction.Timestamp.After(action.Timestamp) {
							processedPaths[relatedAction.Signature()] = true
							logDebug("Create: Found subsequent WRITE partner, marked Write processed", "createSig", action.Signature(), "writeSig", relatedAction.Signature())
							// Ignore this write as part of the create
							break
						}
					}
					logDebug("Create: Exited Write partner check", "signature", action.Signature())
				}

				// Emit the create event
				logDebug("Create: Enriching event with info", "signature", action.Signature())
				action.EnrichFromInfo(info)
				// Update map (important for files created and immediately modified/removed within the same tick)
				logDebug("Create: Updating watchmap with final info", "signature", action.Signature(), "path", action.Path)
				b.watchmap.Set(action.Path, info)
				logDebug("Create: Emitting final CREATE event", "signature", action.Signature(), "event", action)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				logDebug("Create: Marked self as processed", "signature", action.Signature())
			}

		case Rename:
			logDebug("Rename: Entering handler", "signature", action.Signature())
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases on Linux:
			// 1. Rename/Move within watched area: RENAME(old) + CREATE(new)
			// Get the file info from the watchmap for the old path
			renamedFileInfo := b.watchmap.Get(action.Path)
			if renamedFileInfo == nil {
				// Cannot correlate if not in map. Treat as soft delete.
				logDebug("Rename: Info not found in watchmap, synthesizing Soft Remove", "signature", action.Signature())
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				logDebug("Rename: Emitting synthesized Soft Remove", "signature", action.Signature(), "event", result)
				b.emitEvent(result)
				processedPaths[action.Signature()] = true
				continue
			}
			logDebug("Rename: Found info in watchmap for old path", "signature", action.Signature(), "info", renamedFileInfo)

			isRenameOrMove := false
			// Check for a corresponding Create event *after* this Rename event
			logDebug("Rename: Entering Create partner check", "signature", action.Signature())
			for _, relatedCreate := range eventList {
				if relatedCreate.Type == Create && relatedCreate.Timestamp.After(action.Timestamp) {
					logDebug("Rename: Checking potential CREATE partner", "renameSig", action.Signature(), "createSig", relatedCreate.Signature())
					createdStat, err := os.Stat(relatedCreate.Path)
					if err != nil {
						logDebug("Rename: os.Stat failed for potential partner, skipping partner", "renameSig", action.Signature(), "createPath", relatedCreate.Path, "error", err)
						continue
					}
					createdFileInfo := FromOSInfo(relatedCreate.Path, createdStat)
					if createdFileInfo != nil && createdFileInfo.Id == renamedFileInfo.Id {
						// Found the CREATE part of the rename/move
						logDebug("Rename: Found CREATE partner via ID match, marking Rename processed", "renameSig", action.Signature(), "createSig", relatedCreate.Signature(), "id", renamedFileInfo.Id)
						processedPaths[action.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			logDebug("Rename: Exited Create partner check", "signature", action.Signature(), "isRenameOrMove", isRenameOrMove)

			if !isRenameOrMove {
				// No corresponding create event found => Soft Delete / Move Out
				logDebug("Rename: No CREATE partner found, synthesizing Soft Remove", "signature", action.Signature())
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
				logDebug("Rename: Emitting synthesized Soft Remove", "signature", action.Signature(), "event", result)
				b.emitEvent(result)
				// Clean up map for the old path
				logDebug("Rename: Adding old path to deferred removal list", "signature", action.Signature(), "path", action.Path)
				watchmapItemsToRemove[action.Path] = true
				processedPaths[action.Signature()] = true
				logDebug("Rename: Marked self as processed", "signature", action.Signature())
			}

		case Write:
			logDebug("Write: Entering handler", "signature", action.Signature())
			// Deduplicate writes: only process the latest one for a given path in this tick
			latestModify := action
			for _, relatedModify := range eventList {
				if relatedModify.Path == action.Path && relatedModify.Type == Write && relatedModify.Timestamp.After(latestModify.Timestamp) {
					processedPaths[latestModify.Signature()] = true // Mark current action processed
					logDebug("Write: Found later Write, marking earlier processed", "earlierSig", latestModify.Signature(), "laterSig", relatedModify.Signature())
					latestModify = relatedModify // Update latestModify to the later one
				}
			}

			// Check if the file exists on disk
			stat, err := os.Stat(latestModify.Path)
			if err != nil {
				// Ignore write if file doesn't exist (must be related to a remove/rename)
				if os.IsNotExist(err) {
					logDebug("Write: os.Stat failed (NotExist), adding to deferred removal and skipping", "signature", latestModify.Signature(), "path", latestModify.Path, "error", err)
					watchmapItemsToRemove[latestModify.Path] = true
				} else {
					slog.Warn("Write: os.Stat failed, skipping", "signature", latestModify.Signature(), "path", latestModify.Path, "error", err)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}
			logDebug("Write: os.Stat successful", "signature", latestModify.Signature())

			// Check if this Write corresponds to a Create (non-empty file creation)
			foundCreated := false
			logDebug("Write: Entering Create partner check", "signature", latestModify.Signature())
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// Write is part of Create sequence. Ignore the Write; Create handler deals with it.
					logDebug("Write: Found preceding CREATE partner, marking self processed and skipping", "writeSig", latestModify.Signature(), "createSig", relatedCreate.Signature())
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}
			logDebug("Write: Exited Create partner check", "signature", latestModify.Signature(), "foundCreated", foundCreated)

			// If not part of a Create, emit the Write event
			if !foundCreated {
				logDebug("Write: Processing as standalone Write", "signature", latestModify.Signature())
				info := FromOSInfo(latestModify.Path, stat)
				if info != nil {
					// Update map with potentially changed info (mod time, size)
					logDebug("Write: Updating watchmap with info", "signature", latestModify.Signature(), "info", info)
					b.watchmap.Set(latestModify.Path, info)
					latestModify.EnrichFromInfo(info)
				}
				logDebug("Write: Emitting WRITE event", "signature", latestModify.Signature(), "event", latestModify)
				b.emitEvent(latestModify)
				processedPaths[latestModify.Signature()] = true
				logDebug("Write: Marked self as processed", "signature", latestModify.Signature())
			}

		case Chmod:
			logDebug("Chmod: Entering handler", "signature", action.Signature())
			// Emit the chmod event if configured
			if b.config.EmitChmod {
				logDebug("Chmod: EmitChmod enabled, emitting CHMOD event", "signature", action.Signature(), "event", action)
				b.emitEvent(action)
			}

			// Mark the action as processed anyway, whether emitted or not
			processedPaths[action.Signature()] = true
			logDebug("Chmod: Marked self as processed", "signature", action.Signature())

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
