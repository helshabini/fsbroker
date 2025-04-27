package fsbroker_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/helshabini/fsbroker" // Adjust import path if necessary
)

const (
	// Generous timeout to wait for events, allowing for FS latency and broker processing.
	// The broker's internal timeout is 300ms by default.
	eventTimeout = 2 * time.Second
)

// setupTestEnv creates a temporary directory, initializes and starts FSBroker watching it.
// Returns the broker, the config used, the watch directory path, and a cleanup function.
func setupTestEnv(t *testing.T) (*fsbroker.FSBroker, *fsbroker.FSConfig, string, func()) {
	t.Helper()

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "fsbroker_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create the 'watch' subdirectory within the tempDir
	watchDir := filepath.Join(tempDir, "watch")
	if err := os.Mkdir(watchDir, 0755); err != nil {
		os.RemoveAll(tempDir) // Attempt cleanup on error
		t.Fatalf("Failed to create watch dir: %v", err)
	}

	config := fsbroker.DefaultFSConfig()
	// Reduce timeout for faster tests, but keep it > fsnotify latency
	config.Timeout = 400 * time.Millisecond

	broker, err := fsbroker.NewFSBroker(config)
	if err != nil {
		os.RemoveAll(tempDir) // Attempt cleanup on error
		t.Fatalf("Failed to create FSBroker: %v", err)
	}

	if err := broker.AddRecursiveWatch(watchDir); err != nil {
		broker.Stop()
		os.RemoveAll(tempDir) // Attempt cleanup on error
		t.Fatalf("Failed to add recursive watch: %v", err)
	}

	broker.Start()

	cleanup := func() {
		broker.Stop()
		// Give fsnotify a moment to release watches before removing dir
		time.Sleep(100 * time.Millisecond)
		err := os.RemoveAll(tempDir)
		if err != nil {
			// Log error but don't fail test during cleanup
			log.Printf("Warning: Failed to remove temp dir %s: %v", tempDir, err)
		}
	}

	// Drain potential initial events if any (shouldn't be)
	drainEvents(broker, 100*time.Millisecond)

	return broker, config, watchDir, cleanup
}

// expectEvent waits for a specific event type within a timeout.
func expectEvent(t *testing.T, broker *fsbroker.FSBroker, expectedType fsbroker.EventType, expectedPath string) *fsbroker.FSEvent {
	t.Helper()
	select {
	case event := <-broker.Next():
		if event.Type != expectedType {
			t.Fatalf("Expected event type %v, but got %v for path %s", expectedType, event.Type, event.Path)
		}
		// Normalize paths for comparison
		relEventPath, err := filepath.Rel(filepath.Dir(expectedPath), event.Path)
		if err != nil {
			t.Logf("Warning: Could not make event path relative: %v", err)
			relEventPath = event.Path // Use absolute if relative fails
		}
		relExpectedPath, err := filepath.Rel(filepath.Dir(expectedPath), expectedPath)
		if err != nil {
			t.Logf("Warning: Could not make expected path relative: %v", err)
			relExpectedPath = expectedPath // Use absolute if relative fails
		}

		if relEventPath != relExpectedPath {
			t.Fatalf("Expected event path %q, but got %q (Type: %v)", expectedPath, event.Path, event.Type)
		}
		t.Logf("Received expected event: Type=%v, Path=%s, Props=%v", event.Type, event.Path, event.Properties)
		return event
	case err := <-broker.Error():
		t.Fatalf("Received unexpected error while waiting for %v on %s: %v", expectedType, expectedPath, err)
	case <-time.After(eventTimeout):
		t.Fatalf("Timeout waiting for event type %v on path %s", expectedType, expectedPath)
	}
	return nil // Should not be reached
}

// expectNoEvent waits for a duration and fails if any event OR error is received.
func expectNoEvent(t *testing.T, broker *fsbroker.FSBroker, duration time.Duration) {
	t.Helper()
	select {
	case event := <-broker.Next():
		t.Fatalf("Received unexpected event: Type=%v, Path=%s", event.Type, event.Path)
	case err := <-broker.Error():
		t.Fatalf("Received unexpected error: %v", err)
	case <-time.After(duration):
		// Success - no event or error received
		t.Logf("Correctly received no event or error within %v", duration)
	}
}

// drainEvents consumes any events for a short duration.
func drainEvents(broker *fsbroker.FSBroker, duration time.Duration) {
	drainTimer := time.NewTimer(duration)
	defer drainTimer.Stop()
	for {
		select {
		case <-broker.Next():
			// consume
		case <-broker.Error():
			// consume
		case <-drainTimer.C:
			return
		}
	}
}

// TestFSBrokerIntegration runs integration tests covering file system operations.
func TestFSBrokerIntegration(t *testing.T) {

	// --- File Tests ---

	t.Run("CreateEmptyFile", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "empty_file.txt")
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Close() // Close immediately

		expectEvent(t, broker, fsbroker.Create, filePath)
	})

	t.Run("CreateNonEmptyFile", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "non_empty_file.txt")
		err := os.WriteFile(filePath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		expectEvent(t, broker, fsbroker.Create, filePath)
		// Important: Depending on timing and OS, a Write might follow closely.
		// fsbroker *should* ideally coalesce this into the Create.
		// We add a small delay and check no Write event arrives immediately after.
		expectNoEvent(t, broker, config.Timeout/2)
	})

	t.Run("ModifyFile", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "modify_file.txt")
		err := os.WriteFile(filePath, []byte("initial"), 0644)
		if err != nil {
			t.Fatalf("Failed to write initial file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, filePath) // Consume create event

		// Append to the file
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("Failed to open file for append: %v", err)
		}
		_, err = f.WriteString("\nappended")
		if err != nil {
			f.Close()
			t.Fatalf("Failed to append to file: %v", err)
		}
		f.Close()

		expectEvent(t, broker, fsbroker.Write, filePath)
	})

	t.Run("ClearFile", func(t *testing.T) {
		// This relies on DarwinChmodAsModify=true in config for macOS
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "clear_file.txt")
		err := os.WriteFile(filePath, []byte("not empty"), 0644)
		if err != nil {
			t.Fatalf("Failed to write initial file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, filePath) // Consume create event

		err = os.Truncate(filePath, 0)
		if err != nil {
			t.Fatalf("Failed to truncate file: %v", err)
		}

		expectEvent(t, broker, fsbroker.Write, filePath)
	})

	t.Run("RenameFileInplace", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		oldPath := filepath.Join(watchDir, "old_name.txt")
		newPath := filepath.Join(watchDir, "new_name.txt")

		err := os.WriteFile(oldPath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, oldPath) // Consume create event

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to rename file: %v", err)
		}

		event := expectEvent(t, broker, fsbroker.Rename, newPath)
		if oldPathProp, ok := event.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename event missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, event.Properties["OldPath"])
		}
	})

	t.Run("MoveFileWatchedToWatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		subDir := filepath.Join(watchDir, "subdir")
		err := os.Mkdir(subDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}
		// Explicitly consume the Create event for the subdirectory
		expectEvent(t, broker, fsbroker.Create, subDir)
		// Drain any other potential related events just in case
		drainEvents(broker, config.Timeout/2)

		oldPath := filepath.Join(watchDir, "move_me.txt")
		newPath := filepath.Join(subDir, "move_me.txt") // Same name, different dir

		err = os.WriteFile(oldPath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, oldPath) // Consume create event

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to move file: %v", err)
		}

		event := expectEvent(t, broker, fsbroker.Rename, newPath)
		if oldPathProp, ok := event.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename event missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, event.Properties["OldPath"])
		}
	})

	t.Run("MoveFileUnwatchedToWatched", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		externalDir := filepath.Dir(watchDir) // Parent temp dir
		externalPath := filepath.Join(externalDir, "external.txt")
		finalPath := filepath.Join(watchDir, "external.txt")

		err := os.WriteFile(externalPath, []byte("external content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create external file: %v", err)
		}
		// No event expected yet as it's outside watchDir

		err = os.Rename(externalPath, finalPath)
		if err != nil {
			t.Fatalf("Failed to move file into watched dir: %v", err)
		}

		expectEvent(t, broker, fsbroker.Create, finalPath)
	})

	t.Run("MoveFileWatchedToUnwatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		internalPath := filepath.Join(watchDir, "internal.txt")
		externalDir := filepath.Dir(watchDir) // Parent temp dir
		externalPath := filepath.Join(externalDir, "moved_out.txt")

		err := os.WriteFile(internalPath, []byte("internal content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create internal file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, internalPath) // Consume create
		// Drain potential Write event associated with create before moving
		drainEvents(broker, config.Timeout/2)

		err = os.Rename(internalPath, externalPath)
		if err != nil {
			t.Fatalf("Failed to move file out of watched dir: %v", err)
		}

		expectEvent(t, broker, fsbroker.Remove, internalPath)
	})

	t.Run("HardDeleteFile", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "delete_me.txt")
		err := os.WriteFile(filePath, []byte("to be deleted"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, filePath) // Consume create

		err = os.Remove(filePath)
		if err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}

		expectEvent(t, broker, fsbroker.Remove, filePath)
	})

	// --- Directory Tests ---

	t.Run("CreateDirectory", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		dirPath := filepath.Join(watchDir, "new_dir")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		expectEvent(t, broker, fsbroker.Create, dirPath)
	})

	t.Run("RenameDirectoryInplace", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		oldPath := filepath.Join(watchDir, "old_dir")
		newPath := filepath.Join(watchDir, "new_dir")

		err := os.Mkdir(oldPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, oldPath) // Consume create

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to rename directory: %v", err)
		}

		event := expectEvent(t, broker, fsbroker.Rename, newPath)
		if oldPathProp, ok := event.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename event missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, event.Properties["OldPath"])
		}
	})

	t.Run("MoveDirectoryWatchedToWatched", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		parentDir := filepath.Join(watchDir, "parent_dir")
		err := os.Mkdir(parentDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create parent dir: %v", err)
		}
		// Consume parent create
		expectEvent(t, broker, fsbroker.Create, parentDir)

		oldPath := filepath.Join(watchDir, "move_this_dir")
		newPath := filepath.Join(parentDir, "move_this_dir") // Moved inside parent

		err = os.Mkdir(oldPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create dir to move: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, oldPath) // Consume create event

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to move directory: %v", err)
		}

		event := expectEvent(t, broker, fsbroker.Rename, newPath)
		if oldPathProp, ok := event.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename event missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, event.Properties["OldPath"])
		}
	})

	t.Run("MoveDirectoryUnwatchedToWatched", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		externalDir := filepath.Join(filepath.Dir(watchDir), "external_dir")
		finalPath := filepath.Join(watchDir, "external_dir")

		err := os.Mkdir(externalDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create external dir: %v", err)
		}

		err = os.Rename(externalDir, finalPath)
		if err != nil {
			t.Fatalf("Failed to move directory into watched dir: %v", err)
		}

		expectEvent(t, broker, fsbroker.Create, finalPath)
	})

	t.Run("MoveDirectoryWatchedToUnwatched", func(t *testing.T) {
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		internalPath := filepath.Join(watchDir, "internal_dir")
		externalPath := filepath.Join(filepath.Dir(watchDir), "moved_out_dir")

		err := os.Mkdir(internalPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create internal dir: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, internalPath) // Consume create

		err = os.Rename(internalPath, externalPath)
		if err != nil {
			t.Fatalf("Failed to move directory out of watched dir: %v", err)
		}

		expectEvent(t, broker, fsbroker.Remove, internalPath)
	})

	t.Run("HardDeleteDirectory", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		dirPath := filepath.Join(watchDir, "delete_this_dir")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		// Create a file inside to test recursive delete event (though fsbroker might only report the top dir remove)
		filePath := filepath.Join(dirPath, "inner_file.txt")
		err = os.WriteFile(filePath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create inner file: %v", err)
		}

		expectEvent(t, broker, fsbroker.Create, dirPath) // Consume dir create
		// We might get a create for the inner file too depending on timing, drain it.
		drainEvents(broker, config.Timeout/2)

		err = os.RemoveAll(dirPath)
		if err != nil {
			t.Fatalf("Failed to remove directory: %v", err)
		}

		// Wait for the first Remove event related to the deletion
		t.Logf("Waiting for first Remove event after RemoveAll(%s)", dirPath)
		var firstRemoveEvent *fsbroker.FSEvent
		select {
		case event := <-broker.Next():
			if event.Type != fsbroker.Remove {
				t.Fatalf("Expected first event to be Remove, got %v for path %s", event.Type, event.Path)
			}
			firstRemoveEvent = event
			t.Logf("Received first remove event: Path=%s", event.Path)
		case err := <-broker.Error():
			t.Fatalf("Received unexpected error while waiting for Remove: %v", err)
		case <-time.After(eventTimeout):
			t.Fatalf("Timeout waiting for Remove event after deleting directory %s", dirPath)
		}

		// Check if the path matches the dir OR the inner file path
		relEventPath, _ := filepath.Rel(watchDir, firstRemoveEvent.Path)
		relDirPath, _ := filepath.Rel(watchDir, dirPath)
		relFilePath, _ := filepath.Rel(watchDir, filePath)

		if relEventPath != relDirPath && relEventPath != relFilePath {
			t.Errorf("Expected Remove event path to be relative '%s' or '%s', but got '%s' (Absolute: %s)", relDirPath, relFilePath, relEventPath, firstRemoveEvent.Path)
		} else {
			t.Logf("Received Remove event path '%s' matches expected paths ('%s' or '%s')", relEventPath, relDirPath, relFilePath)
		}

		// Since the broker doesn't currently consolidate these, drain for a cycle
		// to consume the potential second Remove event.
		t.Logf("Draining potential subsequent Remove event...")
		// Cast duration for multiplication
		drainEvents(broker, time.Duration(float64(config.Timeout)*1.5))

		// Check no *other* unexpected events follow.
		t.Logf("Checking for further unexpected events...")
		expectNoEvent(t, broker, config.Timeout/2) // Use a shorter timeout here
	})

	// --- Other Tests ---

	t.Run("IgnoreHiddenFile", func(t *testing.T) {
		// Assumes DefaultFSConfig has IgnoreHiddenFiles = true
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		// Test Strategy: Create file outside watch dir, make it hidden (Windows), move it in.
		externalDir := filepath.Dir(watchDir)
		externalPath := filepath.Join(externalDir, ".hidden_external")
		finalPath := filepath.Join(watchDir, ".hidden_external") // Final path inside watch dir

		// 1. Create the file externally
		err := os.WriteFile(externalPath, []byte("hidden content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create external hidden file: %v", err)
		}
		// Cleanup external file if test fails before move
		defer os.Remove(externalPath)

		// 2. On Windows, explicitly set the hidden attribute on the external file
		if runtime.GOOS == "windows" {
			// Call the OS-specific function (defined in fsbroker_test_windows.go)
			fsbroker.SetHiddenAttribute(t, externalPath)
		}

		// 3. Move the file into the watched directory
		err = os.Rename(externalPath, finalPath)
		if err != nil {
			t.Fatalf("Failed to move hidden file into watched dir: %v", err)
		}

		// 4. Expect *no* event because the file entering the watch dir is hidden
		expectNoEvent(t, broker, time.Duration(float64(config.Timeout)*1.5)) // Wait a bit longer than one cycle
	})

	t.Run("IgnoreSystemFile", func(t *testing.T) {
		// Assumes DefaultFSConfig has IgnoreSysFiles = true
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		// Determine OS-specific system file name
		var sysFileName string
		switch runtime.GOOS {
		case "windows":
			sysFileName = "desktop.ini"
		case "darwin":
			sysFileName = ".DS_Store"
		case "linux":
			sysFileName = ".bash_history" // Example for Linux
		default:
			sysFileName = ".sysfile_test" // Fallback for other OSes
			t.Logf("Warning: Unknown OS %s for IgnoreSystemFile test, using generic .sysfile_test", runtime.GOOS)
		}
		sysFilePath := filepath.Join(watchDir, sysFileName)

		err := os.WriteFile(sysFilePath, []byte("system stuff"), 0644)
		if err != nil {
			t.Fatalf("Failed to create system file: %v", err)
		}

		// Expect *no* event because the file should be ignored
		expectNoEvent(t, broker, time.Duration(float64(config.Timeout)*1.5))
	})

	t.Run("WriteDeduplication", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout to ensure all rapid writes fall within one tick
		config.Timeout = 3 * time.Second
		defer cleanup()

		filePath := filepath.Join(watchDir, "dedup_write.txt")

		// Initial create
		err := os.WriteFile(filePath, []byte("first"), 0644)
		if err != nil {
			t.Fatalf("Failed initial write: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, filePath) // Consume create

		// Perform rapid writes
		t.Logf("Performing rapid writes...")
		numWrites := 5
		for i := 0; i < numWrites; i++ {
			f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("Failed to open for append %d: %v", i, err)
			}
			_, err = f.WriteString(fmt.Sprintf("\nwrite %d", i+1))
			f.Close() // Close immediately after write
			if err != nil {
				t.Fatalf("Failed append %d: %v", i, err)
			}
			time.Sleep(50 * time.Millisecond) // Delay << config.Timeout
		}
		t.Logf("Finished rapid writes.")

		// Expect only ONE consolidated Write event
		expectEvent(t, broker, fsbroker.Write, filePath)

		// Ensure no other events follow immediately
		expectNoEvent(t, broker, config.Timeout/2)
	})

	t.Run("DeleteDeduplication", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout to ensure rapid actions fall within one tick
		config.Timeout = 3 * time.Second
		defer cleanup()

		filePath := filepath.Join(watchDir, "dedup_delete.txt")

		// Initial create
		err := os.WriteFile(filePath, []byte("to be deleted"), 0644)
		if err != nil {
			t.Fatalf("Failed initial write: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, filePath) // Consume create

		// Perform rapid actions ending in delete (all within one tick)
		t.Logf("Performing rapid write then delete...")
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("Failed open for append: %v", err)
		}
		_, err = f.WriteString("\nadding more before delete")
		f.Close()
		if err != nil {
			t.Fatalf("Failed append: %v", err)
		}
		// No sleep here - we want write and remove close together

		err = os.Remove(filePath)
		if err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
		t.Logf("Finished rapid write then delete.")

		// Expect only ONE Remove event, the write should be ignored
		expectEvent(t, broker, fsbroker.Remove, filePath)

		// Ensure no other events follow immediately (like the ignored write)
		expectNoEvent(t, broker, config.Timeout/2)
	})

	t.Run("MixedRapidActions", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout significantly for this specific test to ensure all actions
		// likely fall within a single processing tick.
		config.Timeout = 3 * time.Second
		defer cleanup()

		fileA := filepath.Join(watchDir, "file_a.txt")
		fileB := filepath.Join(watchDir, "file_b.txt")
		fileC := filepath.Join(watchDir, "file_c.txt")
		fileA1 := filepath.Join(watchDir, "file_a_renamed.txt")

		// --- Perform a burst of actions ---
		t.Logf("Performing mixed rapid actions...")
		// 1. Create A, B, C
		mustWrite(t, fileA, "content A")
		mustWrite(t, fileB, "content B")
		mustWrite(t, fileC, "content C")
		// 2. Rename A -> A1
		mustRename(t, fileA, fileA1)
		// 3. Write to B (now A1 exists, B exists, C exists)
		mustAppend(t, fileB, "\nmore B")
		// 4. Remove C (now A1 exists, B exists)
		mustRemove(t, fileC)
		t.Logf("Finished mixed rapid actions.")

		// --- Collect all events that arrive within a reasonable timeframe ---
		t.Logf("Collecting events...")
		receivedEvents := make(map[string]*fsbroker.FSEvent)
		stopTimer := time.NewTimer(eventTimeout) // Use the general event timeout
	collectLoop:
		for {
			select {
			case event := <-broker.Next():
				t.Logf("Collected Event: Type=%v, Path=%s, OldPath=%v", event.Type, event.Path, event.Properties["OldPath"])
				// Use signature based on final path for uniqueness in this test
				eventSig := fmt.Sprintf("%d-%s", event.Type, event.Path)
				receivedEvents[eventSig] = event
			case err := <-broker.Error():
				t.Errorf("Received unexpected error during collection: %v", err)
			case <-stopTimer.C:
				break collectLoop
			}
		}
		t.Logf("Finished collecting events. Got %d events.", len(receivedEvents))

		// --- Assert expected final events ---
		expectedSignatures := map[string]struct{}{ // Use a set for easy checking
			fmt.Sprintf("%d-%s", fsbroker.Rename, fileA1): {}, // Rename A -> A1
			fmt.Sprintf("%d-%s", fsbroker.Write, fileB):   {}, // Write B
			fmt.Sprintf("%d-%s", fsbroker.Remove, fileC):  {}, // Remove C
		}

		if len(receivedEvents) != len(expectedSignatures) {
			t.Errorf("Expected %d final events, but got %d", len(expectedSignatures), len(receivedEvents))
		}

		for sig, event := range receivedEvents {
			if _, ok := expectedSignatures[sig]; !ok {
				t.Errorf("Received unexpected final event: Type=%v, Path=%s", event.Type, event.Path)
			}
			// Specific checks for properties
			if event.Type == fsbroker.Rename && event.Path == fileA1 {
				if oldPath, _ := event.Properties["OldPath"].(string); oldPath != fileA {
					t.Errorf("Rename event for %s has incorrect OldPath: got %q, want %q", fileA1, oldPath, fileA)
				}
			}
			// Could add checks for other properties if needed
		}

		for sig := range expectedSignatures {
			if _, ok := receivedEvents[sig]; !ok {
				t.Errorf("Missing expected final event with signature: %s", sig)
			}
		}

	})
}

// --- Helper functions for MixedRapidActions ---

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("mustWrite failed for %s: %v", path, err)
	}
	time.Sleep(10 * time.Millisecond) // Tiny sleep between actions
}

func mustAppend(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("mustAppend failed to open %s: %v", path, err)
	}
	_, err = f.WriteString(content)
	f.Close()
	if err != nil {
		t.Fatalf("mustAppend failed to write %s: %v", path, err)
	}
	time.Sleep(10 * time.Millisecond) // Tiny sleep between actions
}

func mustRename(t *testing.T, oldPath, newPath string) {
	t.Helper()
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("mustRename failed for %s -> %s: %v", oldPath, newPath, err)
	}
	time.Sleep(10 * time.Millisecond) // Tiny sleep between actions
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatalf("mustRemove failed for %s: %v", path, err)
	}
	time.Sleep(10 * time.Millisecond) // Tiny sleep between actions
}
