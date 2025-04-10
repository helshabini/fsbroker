package fsbroker

import (
	"os"
	"path/filepath"
	"runtime"
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
	watched        map[string]bool
	watchermu      sync.Mutex
	watchrecursive bool          // watch recursively on directories, set by AddRecursiveWatch
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
		watched:        make(map[string]bool),
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
	b.watchermu.Lock()
	defer b.watchermu.Unlock()

	if b.watched[path] {
		return nil
	}

	if err := b.watcher.Add(path); err != nil {
		return err
	}
	b.watched[path] = true
	return nil
}

// RemoveWatch removes a watch on a file or directory.
func (b *FSBroker) RemoveWatch(path string) {
	b.watchermu.Lock()
	defer b.watchermu.Unlock()

	if !b.watched[path] {
		return
	}

	b.watcher.Remove(path)
	delete(b.watched, path)
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
	eventList := eventQueue.List()

	if len(eventList) == 0 {
		return
	}

	//Pop event from queue, and stop the loop when it's empty
	for action := eventQueue.Pop(); action != nil; action = eventQueue.Pop() {
		// Ignore already processed paths
		if processedPaths[action.Signature()] {
			continue
		}

		switch action.Type {
		case Remove:
			// TODO: On Windows, a rename causes a remove event followed by a create event.
			// Unfortunately, there is no way we can handle it except two distinct events
			// I have ideas that involve looking at file properties, and try to find remove and create events which are close to each other, but it's not reliable

			// Check if there's any other event for the same path within the queue, ignore it and only raise the remove event
			for _, relatedAction := range eventList {
				if relatedAction.Path == action.Path && relatedAction.Timestamp.Before(action.Timestamp) {
					processedPaths[relatedAction.Signature()] = true
				}
			}
			// If a directory is removed, remove the watch
			b.RemoveWatch(action.Path)
			// Process the Remove event normally
			b.handleEvent(action)
			processedPaths[action.Signature()] = true
		case Create:
			// If a directory is created, add a watch
			if b.watchrecursive {
				info, err := os.Stat(action.Path)
				if err == nil && info.IsDir() {
					b.AddWatch(action.Path)
				}
			}
			// Check if there's a Rename event for the same path within the queue
			isRename := false
			for _, relatedrename := range eventList {
				if filepath.Dir(relatedrename.Path) == filepath.Dir(action.Path) && relatedrename.Type == Rename {
					result := NewFSEvent(Rename, action.Path, action.Timestamp)
					result.Properties["OldPath"] = relatedrename.Path
					b.handleEvent(result)
					processedPaths[action.Signature()] = true
					processedPaths[relatedrename.Signature()] = true
					isRename = true
					break
				}
			}
			if !isRename {
				// Process the Create event normally
				b.handleEvent(action)
				processedPaths[action.Signature()] = true
			}
		case Rename:
			// Rename could be called if the item is moved outside the bounds of watch directories, or moved to a trash directory
			// In both of these cases, we should remove the watch and emit a Remove event
			// We do that by checking if the file actually exists, if it doesn't, we'll emit a Remove event
			// If the item is moved to another directory within watched directories, we'll rely on the associated Create event to detect it
			if _, err := os.Stat(action.Path); os.IsNotExist(err) {
				// If a directory is removed, remove the watch
				b.RemoveWatch(action.Path)
				// Create and process the Remove event normally
				remove := NewFSEvent(Remove, action.Path, action.Timestamp)
				b.handleEvent(remove)
				processedPaths[action.Signature()] = true
				// Then we'll loop on all other preceding captured actions to ignore them, they should be now irrelevant
				for _, relatedAction := range eventList {
					if relatedAction.Path == action.Path && relatedAction.Timestamp.Before(action.Timestamp) {
						processedPaths[relatedAction.Signature()] = true
					}
				}
			}
		case Modify:
			// First, dedup modify events
			// Check if there are multiple save events for the same path within the queue, treat them as a single Modify event
			latestModify := action
			for _, relatedModify := range eventList {
				if relatedModify.Path == action.Path && relatedModify.Type == Modify && relatedModify.Timestamp.After(latestModify.Timestamp) {
					latestModify = relatedModify
				}
			}

			// Then check if there is a Create event for the same path, ignore the Modify event, we'll let the Create event handle it
			// This handles the case where Windows/Linux emits a Create event followed by a Modify event for a new file
			foundCreated := false
			for _, relatedCreate := range eventList {
				if relatedCreate.Path == action.Path && relatedCreate.Type == Create {
					processedPaths[latestModify.Signature()] = true
					foundCreated = true
					break
				}
			}

			// Otherwise, Process the latest Modify event
			if !foundCreated {
				b.handleEvent(latestModify)
				processedPaths[latestModify.Signature()] = true
			}
		case Chmod:
			// Handle case where writing empty file in macOS results in no modify event, but only in chmod event
			if b.config.DarwinChmodAsModify && runtime.GOOS == "darwin" {
				stat, err := os.Stat(action.Path)
				if err == nil && stat.Size() == 0 {
					// Here we are assuming that the file was modified (just because it is empty and had a chmod event)
					modified := NewFSEvent(Modify, action.Path, action.Timestamp)
					b.handleEvent(modified)
					processedPaths[action.Signature()] = true
				} else if b.config.EmitChmod {
					// If the file is not empty or there's an error stating the file, emit the chmod event if configured
					b.handleEvent(action)
					processedPaths[action.Signature()] = true
				}
			} else if b.config.EmitChmod {
				// Not on Darwin or DarwinChmodAsModify is false, emit chmod event if configured
				b.handleEvent(action)
				processedPaths[action.Signature()] = true
			}
		default:
			// Ignore other event types
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
		return Modify
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

// handleEvent sends the event to the user after deduplication, grouping, and processing.
func (b *FSBroker) handleEvent(event *FSEvent) {
	b.emitch <- event
}
