package fsbroker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type FSConfig struct {
	Timeout             time.Duration // duration to wait for events to be grouped and processed
	IgnoreSysFiles      bool          // ignore common system files and directories
	IgnoreHiddenFiles   bool          // ignore hidden files
	DarwinChmodAsModify bool          // treat chmod events on empty files as modify events on macOS
	EmitChmod           bool          // emit chmod events
}

func DefaultFSConfig() *FSConfig {
	return &FSConfig{
		Timeout:             300 * time.Millisecond,
		IgnoreSysFiles:      true,
		IgnoreHiddenFiles:   true,
		DarwinChmodAsModify: true,
		EmitChmod:           false,
	}
}

// FSBroker collects fsnotify events, groups them, dedupes them, and processes them as a single event.
type FSBroker struct {
	watcher        *fsnotify.Watcher
	watchrecursive bool          // watch recursively on directories, set by AddRecursiveWatch
	watchmap       *Map          // local map of watched files and directories
	events         chan *FSEvent // internal events channel, processes FSevent for every FSNotify Op
	emitch         chan *FSEvent // emitted events channel, sends FSevent to the user after deduplication, grouping, and processing
	errors         chan error
	quit           chan struct{}
	config         *FSConfig
	Filter         func(*FSEvent) bool
}

// NewFSBroker creates a new FSBroker instance.
// timeout is the duration to wait for events to be grouped and processed.
// ignoreSysFiles will ignore common system files and directories.
func NewFSBroker(config *FSConfig) (*FSBroker, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &FSBroker{
		watcher:        watcher,
		watchmap:       NewMap(),
		watchrecursive: false,
		events:         make(chan *FSEvent, 100),
		emitch:         make(chan *FSEvent),
		errors:         make(chan error),
		quit:           make(chan struct{}),
		config:         config,
	}, nil
}

// Start starts the broker, listening for events and processing them.
func (b *FSBroker) Start() {
	go b.eventloop()

	go func() {
		for {
			select {
			case event := <-b.watcher.Events:
				fmt.Printf("Event: %s, File: %s\n", event.Op, event.Name)
				b.addEvent(event.Op, event.Name)
			case err := <-b.watcher.Errors:
				b.errors <- err
			}
		}
	}()
}

// Next returns the channel to receive events.
func (b *FSBroker) Next() <-chan *FSEvent {
	return b.emitch
}

// Error returns the channel to receive errors.
func (b *FSBroker) Error() <-chan error {
	return b.errors
}

// AddRecursiveWatch adds a watch on a directory and all its subdirectories.
// It will also add watches on all new directories created within the directory.
// Note: If this is called at least once, all newly created directories will be watched automatically, even if they were added using AddWatch and not using AddRecursiveWatch.
func (b *FSBroker) AddRecursiveWatch(path string) error {
	b.watchrecursive = true // enable recursive watch
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if err := b.AddWatch(p); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// AddWatch adds a watch on a file or directory.
func (b *FSBroker) AddWatch(path string) error {
	if b.watchmap.Get(path) != nil {
		return nil
	}

	if err := b.watcher.Add(path); err != nil {
		return err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return errors.New("path is not a directory")
	}

	b.watchmap.Set(path, FromOSInfo(path, stat))

	// List all files in the directory
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// Add the files to the watchmap. No need to add FSNotify watches, because we're already watching the directory.
	for _, file := range files {
		// Skip directories, we'll watch them later if recursive watch is being requested
		if file.IsDir() {
			continue
		}
		stat, err := file.Info()
		if err != nil {
			return err
		}
		b.watchmap.Set(filepath.Join(path, file.Name()), FromOSInfo(filepath.Join(path, file.Name()), stat))
	}

	return nil
}

// RemoveWatch removes a watch on a file or directory.
func (b *FSBroker) RemoveWatch(path string) {
	_ = b.watcher.Remove(path) // Ignore error, fsnotify will remove the watch automatically if it caught the event

	paths := make([]string, 0)

	// Remove all files in the directory from the watchmap
	b.watchmap.Iterate(func(key string, _ *Info) {
		if strings.HasPrefix(key, path) {
			paths = append(paths, key)
		}
	})
	for _, path := range paths {
		b.watchmap.Delete(path)
	}
}

// eventloop starts the broker, grouping and interpreting events as a single action.
func (b *FSBroker) eventloop() {
	eventQueue := NewEventQueue() // queue of events to be processed, gets cleared every tick

	// Ensure timeout is positive
	timeout := b.config.Timeout
	if timeout <= 0 {
		timeout = 300 * time.Millisecond // Default to 300ms if timeout is invalid
	}

	ticker := time.NewTicker(timeout)
	tickerLock := sync.Mutex{}
	defer ticker.Stop()

	for {
		select {
		case event := <-b.events:
			// Add the event to the queue for grouping
			eventQueue.Push(event)

		case <-ticker.C:
			b.resolveAndHandle(eventQueue, &tickerLock)

		case <-b.quit:
			return
		}
	}
}

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

			// On Windows, a rename causes a remove event (with the old name) followed by a create event (with the new name)
			// So we have to check our watchmap to get infomation about the file that was deleted
			// Then we compare the Identifier of the deleted file with identifiers of created files in the eventList
			// If we find a match, we ignore the remove and create events, and only raise the Rename event
			// Otherwise, we raise the remove event

			// Get the file that was deleted from the watchmap
			deletedFile := b.watchmap.Get(action.Path)
			if deletedFile == nil {
				// If the file is not in the watchmap, it means it's not being watched, so we raise the remove event
				// This shouldn't happen if our watchmap is up to date.
				// But if it does, we'll raise the remove event because we have nothing to compare to
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			var isRename bool = false

			if runtime.GOOS == "windows" {
				// Check if there's a create event for the same file identifier within queue
				for _, relatedAction := range eventList {
					if relatedAction.Type == Create && relatedAction.Timestamp.After(action.Timestamp) {
						stat, err := os.Stat(relatedAction.Path)
						if err != nil {
							continue
						}
						relatedFile := FromOSInfo(relatedAction.Path, stat)
						if relatedFile.Id == deletedFile.Id {
							// We found the new name, ignore the remove event and only raise the Rename event
							result := NewFSEvent(Rename, action.Path, action.Timestamp)
							result.Properties["OldPath"] = relatedAction.Path
							result.EnrichFromInfo(relatedFile)
							b.emitEvent(result)
							processedPaths[action.Signature()] = true
							processedPaths[relatedAction.Signature()] = true
							isRename = true

							// Update the watchmap with the new file information (i.e. after the rename)
							b.watchmap.Delete(action.Path)                  // Remove the old file from the watchmap
							b.watchmap.Set(relatedAction.Path, relatedFile) // Add the new file to the watchmap

							break
						}
					}
				}
			}

			if !isRename {
				// Check if the file the deleted item was a directory, and if so, remove the watch
				if os.FileMode(deletedFile.Mode).IsDir() {
					b.RemoveWatch(action.Path)
					b.watchmap.Delete(action.Path)
				}

				// Enrich the event with the deleted file information
				action.EnrichFromInfo(deletedFile)
				action.Properties["Type"] = "Hard"

				// Process the Remove event normally
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

		case Create:
			// Get info about the created file
			stat, err := os.Stat(action.Path)
			if err != nil {
				if os.IsNotExist(err) { // Created item no longet exists. Remove it from the watchmap if it exists
					b.watchmap.Delete(action.Path)
				}
				processedPaths[action.Signature()] = true
				continue
			}

			info := FromOSInfo(action.Path, stat)
			if info == nil {
				// We failed to get the file Id, so we'll just raise the create event
				action.EnrichFromStat(stat)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
				continue
			}

			var isRenameOrMove bool = false
			// Check if there is a corresponding rename event for the same file identifier
			for _, relatedAction := range eventList {
				if relatedAction.Type == Rename {
					potentialRename := b.watchmap.Get(relatedAction.Path)
					if potentialRename != nil && potentialRename.Id == info.Id {
						// We found the rename event, ignore the create event and only raise the Rename event
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = potentialRename.Path
						result.EnrichFromInfo(info)
						b.emitEvent(result)
						processedPaths[action.Signature()] = true
						processedPaths[relatedAction.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}

			// Now that we're sure it is not a rename or move. We need to check if it is non-empty file creation.
			// If it is, there should be a corresponding write event which we need to ignore.
			// We also need to check if it is a directory creation. If it is, we need to add a watch for it. Otherwise, we add the file to the watchmap.
			// TODO: Make this optional. Maybe users want to also get write events for non-empty files?
			if !isRenameOrMove {
				if b.watchrecursive && stat.IsDir() {
					b.AddWatch(action.Path) // This adds the directory to the watchmap and creates a watch for it
				} else {
					b.watchmap.Set(action.Path, info)
				}

				if !stat.IsDir() {
					// Check if there's a corresponding write event for the same path
					for _, relatedAction := range eventList {
						if relatedAction.Type == Write && relatedAction.Timestamp.After(action.Timestamp) {
							processedPaths[relatedAction.Signature()] = true
							// We found the write event, so we can stop checking.
							// If there are multiple write events, the next one should
							// be considered a nomral write event that we happened to capture
							// in the same queue frame.
							break
						}
					}
				}

				// Now we raise the create event normally
				action.EnrichFromInfo(info)
				b.emitEvent(action)
				processedPaths[action.Signature()] = true
			}

		case Rename:
			// A rename event always carries a path that should no longer exist
			// It is emitted in one of the following cases:
			// - When a file or directory is renamed. In such case there should also be a corresponding Create event
			// - When a file or directory is moved between or within watched directories. Also emits a Create event.
			// - When a file or directory is moved outside the bounds of watched directories, or moved to a trash directory (i.e, soft deleted)
			//   This happens only on POSIX systems (i.e. Linux, MacOS). Windows correctly emits Remove events in cases of soft deletion.

			// So, we need to check if there's a corresponding Create event for the same file identifier
			// If there is one, we ignore the Create event and only raise the Rename event
			// Otherwise, we ignore the Rename event and raise a Remove event

			// Get the file info from the watchmap
			renamedFileInfo := b.watchmap.Get(action.Path)
			if renamedFileInfo == nil {
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
						result := NewFSEvent(Rename, action.Path, action.Timestamp)
						result.Properties["OldPath"] = renamedFileInfo.Path
						result.EnrichFromInfo(createdFileInfo)
						b.emitEvent(result)
						b.watchmap.Delete(renamedFileInfo.Path)
						b.watchmap.Set(createdFileInfo.Path, createdFileInfo)
						processedPaths[action.Signature()] = true
						processedPaths[relatedCreate.Signature()] = true
						isRenameOrMove = true
						break
					}
				}
			}
			if !isRenameOrMove {
				// No corresponding create event found, raise a Remove event
				result := NewFSEvent(Remove, action.Path, action.Timestamp)
				result.Properties["Type"] = "Soft"
				result.EnrichFromInfo(renamedFileInfo)
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
				// If the file doesn't exist on disk, remove it from the watchmap and ignore the event as there must be a corresponding Remove event
				if os.IsNotExist(err) {
					b.watchmap.Delete(latestModify.Path)
				}
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Windows emits write events on directories when contents are changed. This is irrelevant for us.
			// Check if we're on Windows AND for event is for a directory? If true, ignore the event
			if runtime.GOOS == "windows" && stat.IsDir() {
				processedPaths[latestModify.Signature()] = true
				continue
			}

			// Then check if there is a Create event for the same path, ignore the Write event and raise the Create event instead
			// This handles the case where any OS emits a Create event followed by a Write event for a new non-empty file or a copy or move from unwatched directory a watched one
			foundCreated := false
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == latestModify.Path && relatedCreate.Type == Create && relatedCreate.Timestamp.Before(latestModify.Timestamp) {
					// We found the Create event, ignore the Write event and raise the Create event instead
					// But first we need to check if this specific create event was already processed
					if !processedPaths[relatedCreate.Signature()] {
						relatedCreate.EnrichFromInfo(FromOSInfo(latestModify.Path, stat))
						b.emitEvent(relatedCreate)
						processedPaths[relatedCreate.Signature()] = true
					}
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}

			// Otherwise, Process the latest Write event
			if !foundCreated {
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
			if runtime.GOOS == "darwin" {
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
					// No need to do this for other operating systems because it will be eventually caught by the write or create events
					b.watchmap.Set(action.Path, FromOSInfo(action.Path, stat))
				}
			}

		default:
			// Unreachable
		}
	}
}

// Stop stops the broker.
func (b *FSBroker) Stop() {
	close(b.quit)
	b.watcher.Close()
}

// AddEvent queues a new file system event into the broker.
func (b *FSBroker) addEvent(op fsnotify.Op, name string) {
	if b.config.IgnoreSysFiles {
		if isSystemFile(name) {
			return
		}
	}

	if b.config.IgnoreHiddenFiles {
		hidden, err := isHiddenFile(name)
		if err != nil {
			return
		}
		if hidden {
			return
		}
	}

	eventType := mapOpToEventType(op)

	event := NewFSEvent(eventType, name, time.Now())

	if b.Filter != nil && b.Filter(event) {
		return
	}

	b.events <- event
}

// mapOpToEventType maps fsnotify.Op to EventType.
func mapOpToEventType(op fsnotify.Op) EventType {
	switch {
	case op&fsnotify.Create == fsnotify.Create:
		return Create
	case op&fsnotify.Write == fsnotify.Write:
		return Write
	case op&fsnotify.Rename == fsnotify.Rename:
		return Rename
	case op&fsnotify.Remove == fsnotify.Remove:
		return Remove
	case op&fsnotify.Chmod == fsnotify.Chmod:
		return Chmod
	default:
		return -1 // Unknown event
	}
}

// emitEvent sends the event to the user after deduplication, grouping, and processing.
func (b *FSBroker) emitEvent(event *FSEvent) {
	b.emitch <- event
}

// utility method to print the watchmap. This is useful for debugging.
func (b *FSBroker) PrintMap() {
	fmt.Println("Watchmap size:", b.watchmap.Size())
	b.watchmap.Iterate(func(key string, value *Info) {
		fmt.Println(key, value)
	})
}
