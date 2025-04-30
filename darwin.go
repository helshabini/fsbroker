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
	created := make(map[uint64]*FSAction)
	written := make(map[uint64]*FSAction)
	removed := make(map[uint64]*FSAction)
	renamed := make(map[uint64]*FSAction)
	modded  := make(map[uint64]*FSAction)
	noop    := make([]*FSAction, 0)

	for event := stack.Pop(); event != nil; event = stack.Pop() {
		logDebug("Popped event", "operation", event.Type, "path", event.Path)

		switch event.Type {
		case Create:
			//--------------------------------------------------------------
			//  - this is either a new file or rename/move
			//    - stat/getId:
			//      - File doesn't exist on disk:
			//        - Irrelevant event, file no longer exists -> add noop
			//      - File exists on disk:
			//        - query the map by Id
			//          - Found: rename/move -> add rename (new & old path)
			//          - Not Found: new file -> add create
			//--------------------------------------------------------------

			// We pre-stated all unique paths before entering Pass 1
			// So now we seek the statmap for information about our file on disk
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
			// There is currently now way to efficiently query fsmap by Id. This will not compile for now.
			watchmapInfo := b.watchmap.GetById(info.Id)
			
			if watchmapInfo == nil {	// Not found: new file
				logDebug("Create: No entry found in watchmap. This is a new path", "path", event.Path)
				action := FromFSEvent(event)
				action.Subject = info
				created[info.Id] = action
				logDebug("Create: Added action to createdmap", "path", event.Path)
			}

			// Found: rename/move
			logDebug("Create: Entry found in watchmap. This is a rename/move", "path", event.Path)
			action := FromFSEvent(event)
			action.Subject = info
			action.Type = Rename
			action.Properties["OldPath"] = watchmapInfo.Path
			renamed[info.Id] = action
			logDebug("Create: Added action to renamedmap", "path", event.Path)
			
			logDebug("Create: Done", "path", event.Path)
		case Write:
			//--------------------------------------------------------------------------
			//	- this is either associated with a create (non-empty) or normal write
    	//		- query the map by path
			//			- Not found: non-empty create -> add create
      //			- Found: normal write event -> add write
      //--------------------------------------------------------------------------
			
			// Need to get new stats for the file
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

			watchmapInfo := b.watchmap.GetByPath(event.Path)
			
			if watchmapInfo == nil { // Not found: this is a non-empty file creation
				logDebug("Write: No entry found in watchmap. This is a non-empty file creation", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = Create
				action.Subject = info
				created[info.Id] = action
				logDebug("Write: Added action to createdmap", "path", event.Path)
			}

			// Found: normal write event
			logDebug("Write: Found watchmap entry. This is a normal write", "path", event.Path)
			action := FromFSEvent(event)
			action.Subject = info
			written[info.Id] = action
			logDebug("Write: Added action to writtenmap", "path", event.Path)
			
			logDebug("Write: Done", "path", event.Path)

		case Remove:
			//-----------------------------------------------------------------------------------------
			// - Check if we already have an entry about this file
			// 	- Not found: irrelevant event, file never captured and now is gone -> add noop
			//	- Found: Relevant, we know about the file -> ensure file is actually removed from disk
			//		- Still Exists: irrelevant event, file still exists -> add noop
			//		- Doesn't exist: file actually gone -> add remove
			//-----------------------------------------------------------------------------------------				

			// Seek file info from our watchmap
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
			// Check file stats to make sure it does not exist on disk
			info, found := statmap[event.Path]
			if found { // Found: File should have been deleted but somehow recreated. This event is irrelevant
				logDebug("Remove: Event is irrelevant. File still exists on disk", "path", event.Path)	
				action := FromFSEvent(event)
				action.Subject = info
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Remove: Added action to noop", "path", event.Path)
				continue
			}
			// Not found on disk. Add to removedmap.
			logDebug("Remove: File not found on disk. This is a normal remove", "path", event.Path)
			action := FromFSEvent(event)
			action.Subject = watchmapInfo // Last known info about the deleted file
			removed[watchmapInfo.Id] = action
			logDebug("Remove: Added action to removedmap", "path", event.Path)

			logDebug("Remove: Done", "path", event.Path)
			
		case Rename:
			//-----------------------------------------------------------------------------------
			//	- this is either rename/move or soft delete
			//	- Check our watchmap to see if we know about the file
			// 		- Found: We know about this file, it is now renamed, but we don't know to what -> add rename
			//		- Not found: We know nothing about this file, irrelevant -> add noop
			//-----------------------------------------------------------------------------------
			
			watchmapInfo := b.watchmap.GetByPath(event.Path)
			if watchmapInfo == nil { // Not found
				logDebug("Rename: No watchmap entry found, file must have been created then renamed quickly", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				noop = append(noop, action)
				logDebug("Rename: Added action to noop", "path", event.Path)
				continue
			}
			
			// Found: we add it renamed map to coalese it later
			logDebug("Rename: Found watchmap entry", "path", event.Path)
			action := FromFSEvent(event)
			action.Subject = watchmapInfo // Last known info about this file
			renamed[watchmapInfo.Id] = action
			logDebug("Rename: Added action to renamedmap", "path", event.Path)

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
					action := FromFSEvent(event)
					action.Type = Write
					action.Subject = info
					written[info.Id] = action
					logDebug("Chmod: Added action to writtenmap", "path", event.Path)
				}
			}

			// Add event to chmod normally
			if b.config.EmitChmod {
				logDebug("Chmod: Emitting Chmod is enabled. Adding action", "path", event.Path)
				action := FromFSEvent(event)
				action.Subject = info
				modded[info.Id] = action
				logDebug("Chmod: Added action to moddedmap", "path", event.Path)
			}

			logDebug("Chmod: Done", "path", event.Path)
		}
	}
	
	/*
	// Pass 2: Coalesing events, mapping pairs and deduplication
	for _, create := range created {
		event := NewFSEvent(Create, create.Path, create.ModTime)
		event.EnrichFromInfo(create)
		b.emitAction(event)
		logDebug("Create emitted: ", event)
	}
	for _, write := range written {
		event := NewFSEvent(Write, write.Path, write.ModTime)
		event.EnrichFromInfo(write)
		b.emitAction(event)
		logDebug("Write emitted: ", event)
	}
	for _,rename := range renamed {
		event := NewFSEvent(Rename, rename.Path, rename.ModTime)
		event.EnrichFromInfo(rename)
		b.emitAction(event)
		logDebug("Rename emitted: ", event)
	}
	for _, remove := range removed {
		event := NewFSEvent(Remove, remove.Path, remove.ModTime)
		event.EnrichFromInfo(remove)
		b.emitAction(event)
		logDebug("Remove emitted: ", event)
	}
	*/
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
