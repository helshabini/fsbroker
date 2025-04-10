package fsbroker

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// TestNewFSBroker ensures the broker initializes correctly.
func TestNewFSBroker(t *testing.T) {
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()
}

// TestAddRecursiveWatch verifies recursive watch on directories.
func TestAddRecursiveWatch(t *testing.T) {
	// Create a temp directory and subdirectory to watch
	rootDir := t.TempDir()
	subDir := filepath.Join(rootDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()

	// Add recursive watch
	if err := broker.AddRecursiveWatch(rootDir); err != nil {
		t.Fatalf("Failed to add recursive watch: %v", err)
	}

	if _, ok := broker.watched[rootDir]; !ok {
		t.Errorf("Expected root directory to be watched, but it's not")
	}
	if _, ok := broker.watched[subDir]; !ok {
		t.Errorf("Expected sub directory to be watched, but it's not")
	}
}

// TestFilterSysFiles verifies filtering system files based on OS type.
func TestFilterSysFiles(t *testing.T) {
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()

	// Define a filter function that only allows Create events.
	broker.Filter = func(event *FSEvent) bool {
		return event.Type != Create
	}

	// Test filtering non-Create events
	event := NewFSEvent(Remove, "/some/path", time.Now())
	broker.events <- event

	select {
	case <-broker.Next():
		t.Error("Expected event to be filtered, but it was not")
	case <-time.After(100 * time.Millisecond):
		// Success if we get here without any event being passed on
	}
}

// TestEventLoop verifies event deduplication and handling
func TestEventLoop(t *testing.T) {
	config := DefaultFSConfig()
	config.Timeout = 100 * time.Millisecond // Set a valid timeout value
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()

	// Start the broker
	broker.Start()

	event1 := NewFSEvent(Create, "/test/path", time.Now())
	event2 := NewFSEvent(Modify, "/test/path", time.Now().Add(100*time.Millisecond))
	event3 := NewFSEvent(Remove, "/test/path", time.Now().Add(200*time.Millisecond))

	broker.events <- event1
	broker.events <- event2
	broker.events <- event3

	// Receive deduplicated events from emitted channel
	select {
	case result := <-broker.Next():
		if result.Type != Create {
			t.Errorf("Expected Create event, got %v", result.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Timed out waiting for event deduplication")
	}
}

// TestIsSystemFile tests the isSystemFile function for the current platform.
func TestIsSystemFile(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		// Common test cases for all platforms
		{"Regular file", "/path/to/regular.txt", false},
		{"Regular directory", "/path/to/regular", false},
	}

	// Platform-specific test cases
	switch runtime.GOOS {
	case "windows":
		windowsTests := []struct {
			name     string
			path     string
			expected bool
		}{
			{"Desktop.ini", "C:\\Users\\test\\Desktop.ini", true},
			{"Thumbs.db", "C:\\Users\\test\\Thumbs.db", true},
			{"Recycle Bin", "C:\\$Recycle.bin", true},
			{"System Volume Information", "D:\\System Volume Information", true},
		}
		for _, tt := range windowsTests {
			tests = append(tests, struct {
				name     string
				path     string
				expected bool
			}{tt.name, tt.path, tt.expected})
		}
	case "darwin":
		macTests := []struct {
			name     string
			path     string
			expected bool
		}{
			{".DS_Store", "/Users/test/.DS_Store", true},
			{".Trashes", "/Users/test/.Trashes", true},
			{"._ResourceFork", "/Users/test/._Document", true},
			{".AppleDouble", "/Users/test/.AppleDouble", true},
		}
		for _, tt := range macTests {
			tests = append(tests, struct {
				name     string
				path     string
				expected bool
			}{tt.name, tt.path, tt.expected})
		}
	case "linux":
		linuxTests := []struct {
			name     string
			path     string
			expected bool
		}{
			{".bash_history", "/home/user/.bash_history", true},
			{".config", "/home/user/.config", true},
			{".goutputstream", "/tmp/.goutputstream-XYZ", true},
			{".trash", "/home/user/.trash-1000", true},
		}
		for _, tt := range linuxTests {
			tests = append(tests, struct {
				name     string
				path     string
				expected bool
			}{tt.name, tt.path, tt.expected})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSystemFile(tt.path)
			if result != tt.expected {
				t.Errorf("isSystemFile(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

// TestIsHiddenFile tests the isHiddenFile function for the current platform.
func TestIsHiddenFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create a hidden file (platform-specific)
	var hiddenFilePath string
	var regularFilePath string

	switch runtime.GOOS {
	case "windows":
		// On Windows, we can't easily create a hidden file in tests
		// So we'll just test the function with known paths
		hiddenFilePath = filepath.Join(tempDir, "hidden.txt")
		regularFilePath = filepath.Join(tempDir, "regular.txt")

		// Create the files
		if err := os.WriteFile(hiddenFilePath, []byte("hidden"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		if err := os.WriteFile(regularFilePath, []byte("regular"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Note: On Windows, we can't set the hidden attribute in a cross-platform way
		// So we'll just check that the function doesn't error
		_, err := isHiddenFile(hiddenFilePath)
		if err != nil {
			t.Errorf("isHiddenFile(%q) returned error: %v", hiddenFilePath, err)
		}

		_, err = isHiddenFile(regularFilePath)
		if err != nil {
			t.Errorf("isHiddenFile(%q) returned error: %v", regularFilePath, err)
		}
	default:
		// On Unix-like systems, hidden files start with a dot
		hiddenFilePath = filepath.Join(tempDir, ".hidden")
		regularFilePath = filepath.Join(tempDir, "regular")

		// Create the files
		if err := os.WriteFile(hiddenFilePath, []byte("hidden"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		if err := os.WriteFile(regularFilePath, []byte("regular"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Test hidden file
		hidden, err := isHiddenFile(hiddenFilePath)
		if err != nil {
			t.Errorf("isHiddenFile(%q) returned error: %v", hiddenFilePath, err)
		}
		if !hidden {
			t.Errorf("isHiddenFile(%q) = %v, want true", hiddenFilePath, hidden)
		}

		// Test regular file
		hidden, err = isHiddenFile(regularFilePath)
		if err != nil {
			t.Errorf("isHiddenFile(%q) returned error: %v", regularFilePath, err)
		}
		if hidden {
			t.Errorf("isHiddenFile(%q) = %v, want false", regularFilePath, hidden)
		}
	}
}

// TestFSConfig tests the FSConfig structure and DefaultFSConfig function
func TestFSConfig(t *testing.T) {
	config := DefaultFSConfig()
	if config.Timeout != 300*time.Millisecond {
		t.Errorf("Expected timeout 300ms, got %v", config.Timeout)
	}
	if !config.IgnoreSysFiles {
		t.Error("Expected IgnoreSysFiles to be true")
	}
	if !config.IgnoreHiddenFiles {
		t.Error("Expected IgnoreHiddenFiles to be true")
	}
	if !config.DarwinChmodAsModify {
		t.Error("Expected DarwinChmodAsModify to be true")
	}
	if config.EmitChmod {
		t.Error("Expected EmitChmod to be false")
	}
}

// TestStartStop tests the Start and Stop methods
func TestStartStop(t *testing.T) {
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Start the broker
	broker.Start()

	// Create a temporary directory and add a watch
	tempDir := t.TempDir()
	if err := broker.AddWatch(tempDir); err != nil {
		t.Fatalf("Failed to add watch: %v", err)
	}

	// Stop the broker
	broker.Stop()

	// Verify that the quit channel is closed
	select {
	case <-broker.quit:
		// Success - channel is closed
	default:
		t.Error("Expected quit channel to be closed")
	}
}

// TestErrorHandling tests error handling scenarios
func TestErrorHandling(t *testing.T) {
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()

	// Start the broker
	broker.Start()

	// Create a non-existent directory to watch
	nonExistentDir := filepath.Join(t.TempDir(), "nonexistent")
	err = broker.AddWatch(nonExistentDir)
	if err == nil {
		t.Error("Expected error when watching non-existent directory")
	}

	// Test error channel
	go func() {
		broker.errors <- fmt.Errorf("test error")
	}()

	select {
	case err := <-broker.Error():
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out waiting for error")
	}
}

// TestEventHandlingWithConfig tests event handling with different configurations
func TestEventHandlingWithConfig(t *testing.T) {
	tests := []struct {
		name       string
		config     *FSConfig
		eventType  EventType
		filename   string // Use filename instead of full path
		shouldEmit bool
	}{
		{
			name: "Ignore system files",
			config: &FSConfig{
				Timeout:             100 * time.Millisecond,
				IgnoreSysFiles:      true,
				IgnoreHiddenFiles:   false,
				DarwinChmodAsModify: true,
				EmitChmod:           false,
			},
			eventType:  Create,
			filename:   ".DS_Store", // Use a known system file
			shouldEmit: false,
		},
		{
			name: "Ignore hidden files",
			config: &FSConfig{
				Timeout:             100 * time.Millisecond,
				IgnoreSysFiles:      false,
				IgnoreHiddenFiles:   true,
				DarwinChmodAsModify: true,
				EmitChmod:           false,
			},
			eventType:  Create,
			filename:   ".hidden", // Use a hidden file
			shouldEmit: false,
		},
		{
			name: "Emit chmod events",
			config: &FSConfig{
				Timeout:             100 * time.Millisecond,
				IgnoreSysFiles:      false,
				IgnoreHiddenFiles:   false,
				DarwinChmodAsModify: false, // Set to false to avoid interference
				EmitChmod:           true,
			},
			eventType:  Chmod,
			filename:   "testfile",
			shouldEmit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker, err := NewFSBroker(tt.config)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer broker.Stop()

			broker.Start()

			// Create a temporary file for testing
			tempDir := t.TempDir()
			testFile := filepath.Join(tempDir, tt.filename)
			if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Add watch on the temp directory
			if err := broker.AddWatch(tempDir); err != nil {
				t.Fatalf("Failed to add watch: %v", err)
			}

			// Send event through addEvent instead of directly to events channel
			var op fsnotify.Op
			switch tt.eventType {
			case Create:
				op = fsnotify.Create
			case Modify:
				op = fsnotify.Write
			case Remove:
				op = fsnotify.Remove
			case Rename:
				op = fsnotify.Rename
			case Chmod:
				op = fsnotify.Chmod
			}

			broker.addEvent(op, testFile)

			// Wait for a full ticker cycle plus a small buffer
			time.Sleep(tt.config.Timeout + 50*time.Millisecond)

			// Check if event is emitted
			select {
			case <-broker.Next():
				if !tt.shouldEmit {
					t.Error("Expected event to be filtered, but it was emitted")
				}
			case <-time.After(50 * time.Millisecond):
				if tt.shouldEmit {
					t.Error("Expected event to be emitted, but it was filtered")
				}
			}
		})
	}
}

// TestPlatformSpecificBehavior tests platform-specific behavior
func TestPlatformSpecificBehavior(t *testing.T) {
	config := DefaultFSConfig()
	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer broker.Stop()

	broker.Start()

	// Test platform-specific system file detection
	systemFile := ""
	switch runtime.GOOS {
	case "windows":
		systemFile = "C:\\Windows\\System32\\desktop.ini"
	case "darwin":
		systemFile = "/Users/test/.DS_Store"
	case "linux":
		systemFile = "/home/user/.bash_history"
	}

	event := NewFSEvent(Create, systemFile, time.Now())
	broker.events <- event

	select {
	case <-broker.Next():
		t.Error("Expected system file to be filtered")
	case <-time.After(100 * time.Millisecond):
		// Success - system file was filtered
	}
}

func TestMapOpToEventType(t *testing.T) {
	tests := []struct {
		name     string
		op       fsnotify.Op
		expected EventType
	}{
		{"Create", fsnotify.Create, Create},
		{"Write", fsnotify.Write, Modify},
		{"Rename", fsnotify.Rename, Rename},
		{"Remove", fsnotify.Remove, Remove},
		{"Chmod", fsnotify.Chmod, Chmod},
		{"Combined Write and Chmod", fsnotify.Write | fsnotify.Chmod, Modify}, // Testing combined operations
		// Create has precedence over other operations
		{"Create with Write", fsnotify.Create | fsnotify.Write, Create},
		{"Create with Chmod", fsnotify.Create | fsnotify.Chmod, Create},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapOpToEventType(tt.op)
			if result != tt.expected {
				t.Errorf("mapOpToEventType(%v) = %v, want %v", tt.op, result, tt.expected)
			}
		})
	}
}

func TestResolveAndHandle(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*FSBroker, *EventQueue)
		validate func(*testing.T, *FSBroker)
	}{
		{
			name: "Multiple modify events for same file",
			setup: func(b *FSBroker, eq *EventQueue) {
				now := time.Now()
				eq.Push(NewFSEvent(Modify, "/test/file", now))
				eq.Push(NewFSEvent(Modify, "/test/file", now.Add(time.Second)))
				eq.Push(NewFSEvent(Modify, "/test/file", now.Add(2*time.Second)))
			},
			validate: func(t *testing.T, b *FSBroker) {
				eventCount := 0
				timeout := time.After(500 * time.Millisecond)

				// We expect only one event due to deduplication
				for {
					select {
					case event := <-b.emitch:
						eventCount++
						if event.Type != Modify {
							t.Errorf("Expected Modify event, got %v", event.Type)
						}
						if eventCount > 1 {
							t.Errorf("Expected only one event due to deduplication, got %d", eventCount)
						}
					case <-timeout:
						if eventCount == 0 {
							t.Error("Timeout waiting for event")
						}
						return
					}
				}
			},
		},
		{
			name: "Create followed by modify",
			setup: func(b *FSBroker, eq *EventQueue) {
				now := time.Now()
				eq.Push(NewFSEvent(Create, "/test/file", now))
				eq.Push(NewFSEvent(Modify, "/test/file", now.Add(time.Second)))
			},
			validate: func(t *testing.T, b *FSBroker) {
				eventCount := 0
				timeout := time.After(500 * time.Millisecond)

				// We expect only the Create event
				for {
					select {
					case event := <-b.emitch:
						eventCount++
						if event.Type != Create {
							t.Errorf("Expected Create event, got %v", event.Type)
						}
						if eventCount > 1 {
							t.Errorf("Expected only Create event, got additional events")
						}
					case <-timeout:
						if eventCount == 0 {
							t.Error("Timeout waiting for event")
						}
						return
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultFSConfig()
			broker := &FSBroker{
				events:  make(chan *FSEvent, 100),
				emitch:  make(chan *FSEvent, 100),
				errors:  make(chan error, 10),
				quit:    make(chan struct{}),
				config:  config,
				watched: make(map[string]bool),
			}

			eq := NewEventQueue()
			tt.setup(broker, eq)

			tickerLock := &sync.Mutex{}
			broker.resolveAndHandle(eq, tickerLock)
			tt.validate(t, broker)
		})
	}
}
