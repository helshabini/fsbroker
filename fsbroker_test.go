package fsbroker_test

import (
	"log"
	"os"
	"path/filepath"
	"strings"
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
	// Ensure macOS clear file test works if applicable
	config.DarwinChmodAsModify = true

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
			t.Fatalf("Expected event path %s, but got %s (Type: %v)", expectedPath, event.Path, event.Type)
		}
		t.Logf("Received expected event: Type=%v, Path=%s, Props=%v", event.Type, event.Path, event.Properties)
		return event
	case err := <-broker.Error():
		t.Fatalf("Received unexpected error: %v", err)
	case <-time.After(eventTimeout):
		t.Fatalf("Timeout waiting for event type %v on path %s", expectedType, expectedPath)
	}
	return nil // Should not be reached
}

// expectNoEvent waits for a duration and fails if any event is received.
func expectNoEvent(t *testing.T, broker *fsbroker.FSBroker, duration time.Duration) {
	t.Helper()
	select {
	case event := <-broker.Next():
		t.Fatalf("Received unexpected event: Type=%v, Path=%s", event.Type, event.Path)
	case err := <-broker.Error():
		t.Fatalf("Received unexpected error: %v", err)
	case <-time.After(duration):
		// Success - no event received
		t.Logf("Correctly received no event within %v", duration)
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
		broker, _, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		internalPath := filepath.Join(watchDir, "internal.txt")
		externalDir := filepath.Dir(watchDir) // Parent temp dir
		externalPath := filepath.Join(externalDir, "moved_out.txt")

		err := os.WriteFile(internalPath, []byte("internal content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create internal file: %v", err)
		}
		expectEvent(t, broker, fsbroker.Create, internalPath) // Consume create

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

		// Need to know how isHiddenFile determines hidden status.
		// Typically starts with '.' on Unix or has Hidden attribute on Windows.
		// Let's test Unix style for broad compatibility.
		filePath := filepath.Join(watchDir, ".hidden_file")
		err := os.WriteFile(filePath, []byte("hidden"), 0644)
		if err != nil {
			// On Windows, creating dotfiles might require specific APIs or fail.
			// If creation fails, we can't test the ignore. Skip instead?
			// For now, assume it works or the test handles the error.
			if strings.Contains(err.Error(), "Access is denied") || strings.Contains(err.Error(), "invalid argument") {
				t.Skipf("Skipping hidden file test: OS may not support creating dotfiles easily (%v)", err)
			}
			t.Fatalf("Failed to create hidden file: %v", err)
		}

		// Expect *no* event because it should be ignored
		expectNoEvent(t, broker, time.Duration(float64(config.Timeout)*1.5)) // Wait a bit longer than one cycle
	})

	// Add more tests for IgnoreSysFiles, EmitChmod=true/false, Filter, etc.
	// Test Write Deduplication explicitly with rapid writes

}
