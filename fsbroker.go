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



// FSBroker collects fsnotify events, groups them, dedupes them, and processes them as a single event.
type FSBroker struct {
	watcher        *fsnotify.Watcher
	watchrecursive bool          // watch recursively on directories, set by AddRecursiveWatch
	watchmap       *FSMap          // local map of watched files and directories
	events        chan *FSEvent // internal events channel, processes FSEvent(s) for every FSNotify Op
	emit           chan *FSAction // emitted events channel, sends FSAction(s) to the user after processing
	errors         chan error
	quit           chan struct{}
	config         *FSConfig
	Filter         func(*FSAction) bool
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
		watchmap:       NewFSMap(),
		watchrecursive: false,
		events:         make(chan *FSEvent),
		emit:						make(chan *FSAction),
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
					b.handleEvent(event)
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
func (b *FSBroker) Next() <-chan *FSAction {
	return b.emit
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
	err := filepath.Walk(path, func(p string, stat os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if stat.IsDir() {
			if err := b.addWatchInternal(path, stat); err != nil {
				return err
			}
		}
		b.watchmap.Set(FromOSInfo(p, stat))

		return nil
	})
	return err
}

// AddWatch adds a watch on a file or directory.
func (b *FSBroker) AddWatch(path string) error {
	// Check if the path is already in the watchmap
	if b.watchmap.GetByPath(path) != nil {
		return nil
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	if err := b.watcher.Add(path); err != nil {
		return err
	}

	// Add the files to the watchmap. No need to add FSNotify watches, because we're already watching the directory.
	for _, file := range files {
		stat, err := os.Stat(path)
		if err != nil {
			continue // Ignore error, file may not exist
		}
		b.watchmap.Set(FromOSInfo(filepath.Join(path, file.Name()), stat))
	}

	return nil
}

func (b *FSBroker) addWatchInternal(path string, stat os.FileInfo) error {
	// Check if the path is already in the watchmap
	if b.watchmap.GetByPath(path) != nil {
		return nil
	}

	if stat.IsDir() {
		if err := b.watcher.Add(path); err != nil {
			return err
		}
	}

	return nil
}

// RemoveWatch removes a watch on a file or directory.
func (b *FSBroker) RemoveWatch(path string) {
	_ = b.watcher.Remove(path) // Ignore error, fsnotify will remove the watch automatically if it caught the event

	paths := make([]string, 0)

	// Remove all files in the directory from the watchmap
	b.watchmap.IteratePaths(func(key string, _ *FSInfo) {
		if strings.HasPrefix(key, path) {
			paths = append(paths, key)
		}
	})
	for _, path := range paths {
		b.watchmap.DeleteByPath(path)
	}
}

// eventloop starts the broker, grouping and interpreting events as a single action.
func (b *FSBroker) eventloop() {
	eventStack := NewEventStack() // queue of events to be processed, gets cleared every tick

	// Ensure timeout is positive
	timeout := b.config.Timeout
	if timeout <= 0 {
		timeout = 300 * time.Millisecond // Default to 300ms if timeout is invalid
	}

	ticker := time.NewTicker(timeout)
	tickerLock := sync.Mutex{}
	defer ticker.Stop()

	for {
		logDebug("Event loop tick", "Stack Size", eventStack.Len())
		select {
		case event := <-b.events:
			// Add the event to the queue for grouping
			eventStack.Push(event)

		case <-ticker.C:
			b.resolveAndHandle(eventStack, &tickerLock)

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
func (b *FSBroker) handleEvent(event fsnotify.Event) {
	fsevent := NewFSEvent(&event)

	// --- System File Check ---
	if b.config.IgnoreSysFiles {
		if isSystemFile(event.Name) {
			logDebug("Ignoring event: System file", "name", event.Name)
			return
		}
	}

	// --- Hidden File Check ---
	if b.config.IgnoreHiddenFiles {
		hidden, err := isHiddenFile(event.Name) // Use name, not path
		if err != nil {
			// If file doesn't exist when we check hidden status, it's likely due to
			// a rapid Rename/Remove. Log locally but don't treat as error or hidden.
			if os.IsNotExist(err) {
				logDebug("Cannot check hidden status (likely gone), assuming not hidden", "name", event.Name, "error", err)
				// Don't send error to main channel, don't mark as hidden
				hidden = false // Proceed as if not hidden
				err = nil      // Clear error so we don't trigger general error handling
			} else {
				// For other errors, or errors, log to main channel
				b.errors <- fmt.Errorf("error checking if file %s is hidden: %w", event.Name, err)
				slog.Warn("Error checking hidden status, allowing event", "name", event.Name, "error", err)
				// Allow event despite error, but report it
				hidden = false
				err = nil // Clear error as it's been reported
			}
		}
		if hidden {
			logDebug("Ignoring event: Hidden file", "name", event.Name)
			return
		}
	}

	logDebug("Queuing action", "type", fsevent.Event.Op)
	b.events <- fsevent
}

// emitAction sends the event to the user after deduplication, grouping, and processing.
func (b *FSBroker) emitAction(action *FSAction) {
	b.emit <- action
}

