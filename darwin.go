//go:build darwin
// +build darwin

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
	statmap := make(map[string]*FSInfo)
	events := stack.List()

	for _, event := range events {
		_, found := statmap[event.Path]
		if !found {
			logDebug("Stating event file", "operation", event.Type, "path", event.Path)
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
	noop := make([]*FSAction, 0)
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

			var renameInfo *FSInfo = nil
			for _, rename := range toRename {
				if info.Id == rename.Id {
					renameInfo = rename
				}
			}

			if renameInfo == nil { // Not found, nor rename: new file
				logDebug("Create: No entry found in watchmap. This is a new path", "path", event.Path)
				action := AppendEvent(actions, event, info.Id)
				action.Subject = info
				logDebug("Create: Adding watch if recursive watch is enabled")
				if b.watchrecursive && info.IsDir() {
					// Make sure AddWatch doesn't error due to existing map entry if called twice
					_ = b.AddWatch(info.Path) // Add watch if not already present
					logDebug("Create: Added recursive watch for directory", "signature", action.Signature(), "path", info.Path)
				}
				b.watchmap.Set(info)
				logDebug("Create: Added action to actionsmap", "path", event.Path)
				continue
			}

			// Found: rename/move
			logDebug("Create: Entry found in watchmap. This is a rename/move", "path", event.Path)
			oldpath := renameInfo.Path
			if oldpath == info.Path {
				_ = AppendEvent(actions, event, info.Id)
			} else {
				action := AppendEvent(actions, event, info.Id)
				action.Subject = info
				action.Type = Rename
				action.Properties["OldPath"] = oldpath
				b.watchmap.Set(info)
				logDebug("Create: Added action to actionsmap", "path", event.Path)
			}

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
			logDebug("Write: Added action to actionsmap", "path", event.Path)
			b.watchmap.Set(info)
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
			watchmapInfo := b.watchmap.GetByPath(event.Path)
			if watchmapInfo == nil { 
				logDebug("Rename: No watchmap entry found, file must have been created then renamed quickly", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Rename: Added action to noop", "path", event.Path)
				continue
			}

			// Found: we add it renamed map to coalese it later
			logDebug("Rename: Found watchmap entry", "path", event.Path)
			action := AppendEvent(actions, event, watchmapInfo.Id)
			action.Type = Remove
			action.Subject = watchmapInfo
			logDebug("Rename: Added Remove action to actionsmap", "path", event.Path)
			renameInfo := watchmapInfo.Clone()
			toRename = append(toRename, renameInfo)
			b.watchmap.DeleteByPath(event.Path)
			logDebug("Rename: Done", "path", event.Path)

		case Chmod:
			//-----------------------------------------------------------------------------------
			// MacOS special case: clearing file to zero bytes emits no write events
			// We use chmod event instead, and check the file to make sure it is a write
			//	- stat/getId:
			//		- File exists on disk && is zero bytes && wasn't zero bytes (from map by Id) -> add write
			//			- add chmod
			//	- Either way, still emit the chmod event if configured to do so
			//-----------------------------------------------------------------------------------

			// Get stats from statmap
			info, found := statmap[event.Path]
			if !found { // Event irrelevant, file no longer exists
				logDebug("Chmod: Event irrelevant. File no longer exists", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Chmod: Added action to noop", "path", event.Path)
				continue
			}

			// Found statmap entry, check size
			logDebug("Chmod: File found on disk, check it in watchmap", "path", event.Path)
			watchmapInfo := b.watchmap.GetById(info.Id)
			if watchmapInfo != nil { // Found watchmap entry. Compare sizes
				if info.Size == 0 && watchmapInfo.Size > 0 {
					logDebug("Chmod: File is zero size. It wasn't before.", "path", event.Path)
					action := AppendEvent(actions, event, info.Id)
					action.Type = Write
					action.Subject = info
					b.watchmap.Set(info)
					logDebug("Chmod: Added action to actionsmap", "path", event.Path)
				}
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

func FromOSInfo(path string, fileinfo os.FileInfo) *FSInfo {
	sys := fileinfo.Sys()
	sysstat, ok := sys.(*syscall.Stat_t)
	if !ok {
		return nil
	}

	return &FSInfo{
		Id:      sysstat.Ino,
		Path:    path,
		Size:    uint64(fileinfo.Size()),
		ModTime: fileinfo.ModTime(),
		Mode:    uint32(fileinfo.Mode()),
	}
}
