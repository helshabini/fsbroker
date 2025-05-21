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

func (b *FSBroker) resolveAndHandle(stack *EventStack, tickerLock *sync.Mutex) {
	// We only want one instance of this method to run at a time, so we lock it
	if !tickerLock.TryLock() {
		return
	}
	defer tickerLock.Unlock()

	if stack.Len() == 0 {
		return
	}

	// Prep: To avoid stating the same path multiple times (since many events may occur for the same file)
	// We pre-stat all unique paths from incoming events into a statmap
	// On linux: we also do rename deduplication here to avoid rename event confusion in Pass 1
	statmap := make(map[string]*FSInfo)
	events := stack.List()

	noop := make([]*FSAction, 0)
	renameDedupMap := make(map[string]*FSEvent, 0)
	for _, event := range events {
		if event.Type == Rename {
			_, found := renameDedupMap[event.Path]
			if !found {
				renameDedupMap[event.Path] = event
				continue
			}
			logDebug("Deduping rename event", "operation", event.Type, "path", event.Path)
			action := FromFSEvent(event)
			action.Properties["Message"] = "Duplicate rename event"
			noop = append(noop, action)
			stack.Delete(event)
		}
		logDebug("Stating event file", "operation", event.Type, "path", event.Path)
		_, found := statmap[event.Path]
		if !found {
			stat, err := os.Stat(event.Path)
			if err != nil {
				if os.IsNotExist(err) {
					logDebug("File not found", "operation", event.Type, "path", event.Path)
				} else {
					logDebug("Failed to stat file", "operation", event.Type, "path", event.Path)
				}
				continue
			}
			info := FromOSInfo(event.Path, stat)
			if info == nil {
				logDebug("Failed to get file info", "operation", event.Type, "path", event.Path)
				continue
			}
			statmap[event.Path] = info
		}
	}

	// Pass 1: transform events into grouped action
	actions := make(map[uint64]*FSAction)
	toRename := make([]*FSInfo, 0)

	for event := stack.Pop(); event != nil; event = stack.Pop() {
		logDebug("Popped event", "operation", event.Type, "path", event.Path)

		switch event.Type {
		case Create:
			info, found := statmap[event.Path]
			if !found {
				logDebug("Create: Since file no longer exists on disk, this event is irrelevant", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				action.Properties["Message"] = "Event irrelevant. File no longer exists"
				noop = append(noop, action)
				logDebug("Create: Added action to noop", "path", event.Path)
				continue
			}
			// Found file on disk, now we query the watchmap by file Id to check whether we already know about that file or not
			watchmapInfo := b.watchmap.GetById(info.Id)

			if watchmapInfo == nil { // Not found: new file // or rename yet to be processed in the same tick
				logDebug("Create: No entry found in watchmap. This is a new path", "path", event.Path)
				action := AppendEvent(actions, event, info.Id)
				action.Subject = info
				logDebug("Create: Adding watch if recursive watch is enabled")
				if b.watchrecursive && info.IsDir() {
					// Make sure AddWatch doesn't error due to existing map entry if called twice
					_ = b.AddWatch(info.Path) // Add watch if not already present, this already adds recursively into watchmap
					logDebug("Create: Added recursive watch for directory", "signature", action.Signature(), "path", info.Path)
				}
				b.watchmap.Set(info)
				logDebug("Create: Added action to actionsmap", "path", event.Path)
				continue
			}

			// Found: rename/move
			logDebug("Create: Entry found in watchmap. This could still be a Create or Rename/Move", "path", event.Path)
			// We check the existing actions map, if it has an existing Create action left by a write event, we're just a create
			createAction, found := actions[info.Id]
			if found && createAction.Type == Create {
				logDebug("Create: Found already created action. Appending this event to it.", "path", event.Path)
				_ = AppendEvent(actions, event, info.Id)
				b.watchmap.Set(info)
				logDebug("Create: Added action to actionsmap", "path", event.Path)
				continue
			}

			logDebug("Create: Entry found in watchmap. This could still be a Create or Rename/Move", "path", event.Path)
			// Otherwise, we're a rename/move
			renameInfo := watchmapInfo.Clone()
			toRename = append(toRename, renameInfo)
			action := AppendEvent(actions, event, info.Id)
			action.Subject = info
			action.Type = Rename
			b.watchmap.Set(info)
			logDebug("Create: Added action to actionsmap", "path", event.Path)

			logDebug("Create: Done", "path", event.Path)
		case Write:
			info, found := statmap[event.Path]
			if !found {
				// File no longer exists. NoOp
				logDebug("Write: File no longer exists", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				action.Properties["Message"] = "Event irrelevant. File no longer exists"
				noop = append(noop, action)
				logDebug("Write: Added action to noop", "path", event.Path)
				continue
			}

			watchmapInfo := b.watchmap.GetById(info.Id)

			if watchmapInfo == nil { // Not found: this is a non-empty file creation
				logDebug("Write: No entry found in watchmap. This is a non-empty file creation", "path", event.Path)
				action := AppendEvent(actions, event, info.Id)
				action.Type = Create
				action.Subject = info
				logDebug("Create: Adding watch if recursive watch is enabled")
				if b.watchrecursive && info.IsDir() {
					// Make sure AddWatch doesn't error due to existing map entry if called twice
					_ = b.AddWatch(info.Path) // Add watch if not already present
					logDebug("Create: Added recursive watch for directory", "signature", action.Signature(), "path", info.Path)
				}
				b.watchmap.Set(info)
				logDebug("Write: Added action to actionsmap", "path", event.Path)
				continue
			}

			// Found: normal write event
			logDebug("Write: Found watchmap entry. This is a normal write", "path", event.Path)
			action := AppendEvent(actions, event, info.Id)
			action.Subject = info
			b.watchmap.Set(info)
			logDebug("Write: Added action to actionsmap", "path", event.Path)

			logDebug("Write: Done", "path", event.Path)

		case Remove:
			watchmapInfo := b.watchmap.GetByPath(event.Path)
			if watchmapInfo == nil {
				logDebug("Remove: No watchmap entry found, file must have been created then removed quickly", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Remove: Added action to noop", "path", event.Path)
				continue
			}

			logDebug("Remove: Found watchmap entry.", "path", event.Path)
			action := AppendEvent(actions, event, watchmapInfo.Id)
			action.Subject = watchmapInfo // Last known info about the deleted file
			logDebug("Remove: Added action to actionsmap", "path", event.Path)
			b.watchmap.DeleteByPath(event.Path)
			logDebug("Remove: Done", "path", event.Path)

		case Rename:
			var renameInfo *FSInfo = nil
			for _, rename := range toRename {
				if event.Path == rename.Path {
					renameInfo = rename
				}
			}

			if renameInfo != nil { // Not found
				logDebug("Rename: Found matching CREATE event. This is a rename/move", "path", event.Path)
				action := AppendEvent(actions, event, renameInfo.Id)
				action.Type = Rename
				action.Properties["OldPath"] = event.Path
				logDebug("Rename: Added action to actionsmap", "path", event.Path)
				continue
			}

			// Otherwise, this must be a normal rename. Handle it as usual
			watchmapInfo := b.watchmap.GetByPath(event.Path)
			if watchmapInfo == nil { // Not found
				logDebug("Rename: No watchmap entry found, file must have been created then renamed quickly", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Rename: Added action to noop", "path", event.Path)
				continue
			}

			logDebug("Rename: Found watchmap entry", "path", event.Path)
			action := AppendEvent(actions, event, watchmapInfo.Id)
			action.Type = Remove
			action.Subject = watchmapInfo
			logDebug("Rename: Added Remove action to actionsmap", "path", event.Path)
			b.watchmap.DeleteByPath(event.Path)
			logDebug("Rename: Done", "path", event.Path)

		case Chmod:
			info, found := statmap[event.Path]
			if !found { // Event irrelevant, file no longer exists
				logDebug("Chmod: Event irrelevant. File no longer exists", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Chmod: Added action to noop", "path", event.Path)
				continue
			}

			// Add event to chmod normally
			if b.config.EmitChmod {
				logDebug("Chmod: Emitting Chmod is enabled. Adding action", "path", event.Path)
				action := AppendEvent(actions, event, info.Id)
				action.Subject = info
				logDebug("Chmod: Added action to actionsmap", "path", event.Path)
			}

			logDebug("Chmod: Done", "path", event.Path)
		}
	}

	// Pass 2: Emitting actions
	logDebug("Pass 2: Emitting actions")
	for key, value := range actions {
		logDebug("Action: ", "id", key, "action", value)
		b.emitAction(value)
	}
	for _, value := range noop {
		logDebug("NoOp: ", "action", value)
		b.emitAction(value)
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

func FromOSInfo(path string, fileinfo os.FileInfo) *FSInfo {
	sys := fileinfo.Sys()
	sysstat, ok := sys.(*syscall.Stat_t)
	if !ok {
		return nil
	}

	return &FSInfo{
		Id:   sysstat.Ino,
		Path: path,
		Mode: uint32(fileinfo.Mode()),
	}
}
