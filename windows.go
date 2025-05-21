//go:build windows
// +build windows

package fsbroker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
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
	actions := make(map[uint64]*FSAction)
	noop := make([]*FSAction, 0)
	toRename := make(map[string]*FSInfo, 0)

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

			if watchmapInfo == nil { // Not found: new file
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
			oldpath := watchmapInfo.Path
			if oldpath == info.Path {
				_ = AppendEvent(actions, event, info.Id)
			} else {
				toRename[oldpath] = watchmapInfo.Clone()
				action := AppendEvent(actions, event, info.Id)
				action.Subject = info
				action.Type = Rename
				action.Properties["OldPath"] = oldpath
				b.watchmap.Set(info)
				logDebug("Create: Added action to actionsmap", "path", event.Path)
			}

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

			// If the write event is for a directory, ignore the event (add to noop)
			if info.IsDir() {
				logDebug("Write: event is on a directory. NoOp.", "path", event.Path)
				action := FromFSEvent(event)
				action.Type = NoOp
				action.Properties["Message"] = "Event irrelevant. Event is on a directory"
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
				// First check if we're part of a rename
				renameInfo, found := toRename[event.Path]
				if found {
			  	logDebug("Remove: Found rename entry in toRename map", "path", event.Path)
					_ = AppendEvent(actions, event, renameInfo.Id)
					logDebug("Remove: Appended event to existing Rename action", "path", event.Path)
					continue
				}

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
			if watchmapInfo == nil { // Not found
				// First check if we're part of a rename
				renameInfo, found := toRename[event.Path]
				if found {
			  	logDebug("Rename: Found rename entry in toRename map", "path", event.Path)
					_ = AppendEvent(actions, event, renameInfo.Id)
					logDebug("Rename: Appended event to existing Rename action", "path", event.Path)
					continue
				}

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
		// It is acceptable for these syscalls to fail
		// This is because the file may not exist yet, and we want to ignore that
		var sysErr syscall.Errno
		if errors.As(err, &sysErr) &&
			(sysErr == syscall.Errno(2) || sysErr == syscall.Errno(3)) /*ERROR_FILE_NOT_FOUND or ERROR_PATH_NOT_FOUND*/ {
			logDebug("Ignoring benign 'file/path not found' error during hidden check", "name", path, "errno", int(sysErr))
			return false, nil
		}

		// Any other error (permissions, etc.) OR "file not found" for non-RENAME ops.
		return false, err
	}

	// Check if the hidden attribute is set
	// FILE_ATTRIBUTE_HIDDEN = 2 in Windows
	return attributes&syscall.FILE_ATTRIBUTE_HIDDEN != 0, nil
}

func FromOSInfo(path string, fileinfo os.FileInfo) *FSInfo {
	id, err := getFileID(path)
	if err != nil {
		return nil
	}

	// Enrich with size and modtime for Windows, as they are generally reliable
	return &FSInfo{
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
