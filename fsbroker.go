package fsbroker

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type FSConfig struct {
	Timeout           time.Duration // duration to wait for events to be grouped and processed
	IgnoreSysFiles    bool          // ignore common system files and directories
	IgnoreHiddenFiles bool          // ignore hidden files
	EmitChmod         bool          // emit chmod events
}

func DefaultFSConfig() *FSConfig {
	return &FSConfig{
		Timeout:           300 * time.Millisecond,
		IgnoreSysFiles:    true,
		IgnoreHiddenFiles: true,
		EmitChmod:         false,
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
				switch event.Op {
				case fsnotify.Create, fsnotify.Write, fsnotify.Remove, fsnotify.Rename, fsnotify.Chmod:
					logDebug("Received fsnotify event", "op", event.Op.String(), "name", event.Name)
					b.addEvent(event.Op, event.Name)
				default:
					b.errors <- errors.New("unknown fsnotify event")
				}
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

	// List all files in the directory
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// Add the files to the watchmap. No need to add FSNotify watches, because we're already watching the directory.
	for _, file := range files {
		// Recursively watch directories if recursive watch is being requested
		if b.watchrecursive && file.IsDir() {
			b.AddWatch(filepath.Join(path, file.Name()))
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
		logDebug("Event loop tick", "queueSize", eventQueue.Len())
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

// Stop stops the broker.
func (b *FSBroker) Stop() {
	close(b.quit)
	b.watcher.Close()
}

// AddEvent queues a new file system event into the broker.
func (b *FSBroker) addEvent(op fsnotify.Op, name string) {
	// This duplicates the log in the Start() goroutine, but keeping for potential direct calls to addEvent if ever added.
	// slog.Debug("addEvent called", "op", op.String(), "name", name)

	// --- System File Check ---
	if b.config.IgnoreSysFiles {
		if isSystemFile(name) {
			logDebug("Ignoring event: System file", "name", name)
			return
		}
	}

	// Map fsnotify Op first to check type before hidden check
	eventType := mapOpToEventType(op)
	if eventType == -1 { // Check for unknown event type
		logDebug("Ignoring event: Unknown event type", "op", op.String(), "name", name)
		return // Don't queue unknown events
	}
	// slog.Debug("Mapped fsnotify op to event type", "op", op.String(), "eventType", eventType.String(), "name", name)

	// --- Hidden File Check ---
	if b.config.IgnoreHiddenFiles {
		hidden, err := isHiddenFile(name) // Use name, not path
		if err != nil {
			// If file doesn't exist when we check hidden status, it's likely due to
			// a rapid Rename/Remove. Log locally but don't treat as error or hidden.
			if os.IsNotExist(err) {
				logDebug("Cannot check hidden status (likely gone), assuming not hidden", "name", name, "error", err)
				// Don't send error to main channel, don't mark as hidden
				hidden = false // Proceed as if not hidden
				err = nil      // Clear error so we don't trigger general error handling
			} else {
				// For other errors, or errors, log to main channel
				b.errors <- fmt.Errorf("error checking if file %s is hidden: %w", name, err)
				slog.Warn("Error checking hidden status, allowing event", "name", name, "error", err)
				// Allow event despite error, but report it
				hidden = false
				err = nil // Clear error as it's been reported
			}
		}
		if hidden {
			logDebug("Ignoring event: Hidden file", "name", name)
			return
		}
	}

	// --- Create FSEvent Object ---
	event := NewFSEvent(eventType, name, time.Now())

	// --- Update Watchmap Immediately for Creates ---
	if eventType == Create {
		logDebug("Create event: Attempting immediate stat & map update", "signature", event.Signature(), "name", name)
		stat, err := os.Stat(name)
		if err != nil {
			// Log error but proceed to queue event. Map won't be updated yet.
			logDebug("Immediate os.Stat failed, map not updated", "signature", event.Signature(), "name", name, "error", err)
		} else {
			info := FromOSInfo(name, stat)
			if info != nil {
				if b.watchrecursive && info.IsDir() {
					b.AddWatch(name)
				}
				b.watchmap.Set(name, info)
				logDebug("Immediate os.Stat successful, watchmap updated", "signature", event.Signature(), "name", name, "id", info.Id)
			} else {
				// Should not happen if stat succeeded, but handle defensively
				slog.Warn("Immediate os.Stat succeeded but FromOSInfo is nil, map not updated", "signature", event.Signature(), "name", name)
			}
		}
	}

	// --- Custom Filter Check ---
	if b.Filter != nil {
		filterResult := b.Filter(event)
		// slog.Debug("Custom filter result", "signature", event.Signature(), "name", name, "exclude", filterResult)
		if filterResult { // Assuming filter returns true to EXCLUDE
			logDebug("Ignoring event: Custom filter excluded", "signature", event.Signature(), "name", name)
			return
		}
	}

	logDebug("Queuing event", "signature", event.Signature(), "type", event.Type.String(), "path", event.Path)
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
