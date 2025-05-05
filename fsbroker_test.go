package fsbroker_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/helshabini/fsbroker" // Adjust import path if necessary
)

const (
	// Generous timeout to wait for action., allowing for FS latency and broker processing.
	// The broker's internal timeout is 300ms by default.
	// Default timeout for tests NOT overriding config.Timeout. Should be > DefaultFSConfig().Timeout + buffer.
	defaultTestTimeout = 2 * time.Second
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
	config.Timeout = 1 * time.Second

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

	// Drain potential initial action. if any (shouldn't be)
	drainEvents(broker, 100*time.Millisecond)

	return broker, config, watchDir, cleanup
}

// expectAction waits for a specific action.type within a timeout.
func expectAction(t *testing.T, broker *fsbroker.FSBroker, expectedType fsbroker.OpType, expectedPath string, testTimeout time.Duration) *fsbroker.FSAction {
	t.Helper()
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()

	for {
		select {
		case action := <-broker.Next():
			if action.Type == fsbroker.NoOp {
				if action.Subject == nil {
					t.Logf("Received and ignoring NoOp action while waiting for %v", expectedType)
				} else {
					t.Logf("Received and ignoring NoOp action for path %s while waiting for %v", action.Subject.Path, expectedType)
				}
				// Reset the timer or continue waiting? Let's continue waiting for now, the original timer still runs.
				continue // Continue loop to wait for the next event
			}

			if action.Type != expectedType {
				t.Fatalf("Expected action type %v, but got %v for path %s", expectedType, action.Type, action.Subject.Path)
			}
			// Normalize paths for comparison
			relEventPath, err := filepath.Rel(filepath.Dir(expectedPath), action.Subject.Path)
			if err != nil {
				t.Logf("Warning: Could not make action path relative: %v", err)
				relEventPath = action.Subject.Path // Use absolute if relative fails
			}
			relExpectedPath, err := filepath.Rel(filepath.Dir(expectedPath), expectedPath)
			if err != nil {
				t.Logf("Warning: Could not make expected path relative: %v", err)
				relExpectedPath = expectedPath // Use absolute if relative fails
			}

			if relEventPath != relExpectedPath {
				t.Fatalf("Expected action path %q, but got %q (Type: %v)", expectedPath, action.Subject.Path, action.Type)
			}
			t.Logf("Received expected action: Type=%v, Path=%s, Props=%v", action.Type, action.Subject.Path, action.Properties)
			return action // Expected action received

		case err := <-broker.Error():
			t.Fatalf("Received unexpected error while waiting for %v on %s: %v", expectedType, expectedPath, err)

		case <-timer.C:
			t.Fatalf("Timeout waiting for action type %v on path %s (timeout: %v)", expectedType, expectedPath, testTimeout)
		}
	}
	// Note: The loop should only exit via return or t.Fatalf, so this is unreachable.
}

// expectNoEvent waits for a duration and fails if any non-NoOp action OR error is received.
func expectNoEvent(t *testing.T, broker *fsbroker.FSBroker, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case action := <-broker.Next():
			if action.Type != fsbroker.NoOp {
				t.Fatalf("Received unexpected non-NoOp action. Type=%v, Path=%s", action.Type, action.Subject.Path)
			}
			// If it IS a NoOp, log it and continue waiting
			if action.Subject != nil {
				t.Logf("Received and ignored NoOp action during expectNoEvent check for path %s", action.Subject.Path)
			} else {
				t.Logf("Received and ignored NoOp action during expectNoEvent check (subject is nil)")
			}
			// Do not reset timer, continue waiting for the original duration.

		case err := <-broker.Error():
			t.Fatalf("Received unexpected error during expectNoEvent check: %v", err)

		case <-timer.C:
			// Success - no non-NoOp action or error received within duration
			t.Logf("Correctly received no non-NoOp action or error within %v", duration)
			return // Exit function successfully
		}
	}
}

// drainEvents consumes any action. for a short duration.
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

// verifyWatchmapState compares the broker's internal map with the actual filesystem state.
func verifyWatchmapState(t *testing.T, broker *fsbroker.FSBroker, config *fsbroker.FSConfig, watchDir string) {
	t.Helper()

	// Small delay to ensure the broker has processed all queued events
	time.Sleep(100 * time.Millisecond)

	// 1. Collect all expected paths from the filesystem
	expectedPaths := make(map[string]struct{})
	err := filepath.WalkDir(watchDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err // Propagate errors from WalkDir
		}

		// Skip the root watchDir itself
		if path == watchDir {
			return nil
		}

		// Check if the file should be ignored based on config
		baseName := filepath.Base(path)
		if config.IgnoreHiddenFiles {
			hidden, _ := fsbroker.TestIsHiddenFile(path)
			if hidden {
				t.Logf("[VerifyMap] Ignoring hidden file on disk: %s", path)
				return nil // Skip hidden file
			}
		}
		if config.IgnoreSysFiles && fsbroker.TestIsSystemFile(baseName) {
			t.Logf("[VerifyMap] Ignoring system file on disk: %s", path)
			return nil // Skip system file
		}

		// Normalize path for comparison
		absPath, err := filepath.Abs(path)
		if err != nil {
			t.Logf("[VerifyMap] Warning: Failed to get absolute path for %s: %v", path, err)
			absPath = path // Fallback to original path
		}

		// Add to expected paths
		expectedPaths[absPath] = struct{}{}

		return nil
	})

	if err != nil {
		t.Fatalf("[VerifyMap] Error walking directory: %v", err)
	}

	// 2. Check map against expected paths (and vice versa)
	foundPaths := make(map[string]struct{}) // Track which expected paths we've found
	extraInMap := []string{}                // Files in map but not expected (based on disk)

	// Get absolute path of the watch directory for comparison
	absWatchDir, err := filepath.Abs(watchDir)
	if err != nil {
		t.Logf("[VerifyMap] Warning: Failed to get absolute path for watchDir %s: %v", watchDir, err)
		absWatchDir = watchDir // Fallback to the original path
	}

	// Use TestIteratePaths to inspect the watchmap directly
	broker.TestIteratePaths(func(key string, info *fsbroker.FSInfo) {
		absPath, err := filepath.Abs(info.Path)
		if err != nil {
			t.Logf("[VerifyMap] Warning: Failed to get absolute path for map entry %s: %v", info.Path, err)
			absPath = info.Path // Fallback to original path
		}

		// Skip the root watch directory - it's always expected to be in the watchmap
		if absPath == absWatchDir {
			return
		}

		// Try to match both absolute and relative paths in case the broker stores them differently
		exists := false
		relPath, err := filepath.Rel(watchDir, info.Path)
		if err == nil {
			_, exists = expectedPaths[absPath]

			// If not found by absolute path, try to find by normalized relative path
			if !exists {
				if !strings.HasPrefix(relPath, "..") {
					expectedRelPath := filepath.Join(watchDir, relPath)
					expectedAbs, err := filepath.Abs(expectedRelPath)
					if err == nil {
						_, exists = expectedPaths[expectedAbs]
					}
				}
			}
		}

		if !exists {
			extraInMap = append(extraInMap, absPath)
		} else {
			// Mark this expected path as found in the map
			foundPaths[absPath] = struct{}{}
		}
	})

	// 3. Find paths missing from the map
	missingFromMap := []string{} // Files on disk but missing from watchmap
	for expectedPath := range expectedPaths {
		if _, found := foundPaths[expectedPath]; !found {
			missingFromMap = append(missingFromMap, expectedPath)
		}
	}

	// 4. Report any discrepancies
	if len(missingFromMap) > 0 {
		t.Errorf("[VerifyMap] Files/Dirs exist on disk but are MISSING from watchmap:\n  - %s", strings.Join(missingFromMap, "\n  - "))
	}

	if len(extraInMap) > 0 {
		// Create a more detailed error message showing the problematic paths
		var details []string
		for _, path := range extraInMap {
			fileInfo, err := os.Stat(path)
			if os.IsNotExist(err) {
				details = append(details, fmt.Sprintf("%s (does not exist on disk)", path))
			} else if err != nil {
				details = append(details, fmt.Sprintf("%s (error checking: %v)", path, err))
			} else {
				isDir := "file"
				if fileInfo.IsDir() {
					isDir = "directory"
				}
				details = append(details, fmt.Sprintf("%s (%s, exists but should be ignored?)", path, isDir))
			}
		}
		t.Errorf("[VerifyMap] Files/Dirs are in watchmap but SHOULD NOT BE (don't exist on disk or should be ignored):\n  - %s", strings.Join(details, "\n  - "))
	}

	if len(missingFromMap) == 0 && len(extraInMap) == 0 {
		t.Logf("[VerifyMap] ✓ Map state matches expected filesystem state (%d entries).", len(expectedPaths))
	}
}

// TestFSBrokerIntegration runs integration tests covering file system operations.
func TestFSBrokerIntegration(t *testing.T) {

	// --- File Tests ---

	t.Run("CreateEmptyFile", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "empty_file.txt")
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Close() // Close immediately

		// Use default timeout since config.Timeout wasn't changed
		expectAction(t, broker, fsbroker.Create, filePath, defaultTestTimeout)

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("CreateNonEmptyFile", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "non_empty_file.txt")
		err := os.WriteFile(filePath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Use default timeout
		expectAction(t, broker, fsbroker.Create, filePath, defaultTestTimeout)
		// Important: Depending on timing and OS, a Write might follow closely.
		// fsbroker *should* ideally coalesce this into the Create.
		// We add a small delay and check no Write action.arrives immediately after.
		expectNoEvent(t, broker, config.Timeout/2)

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("ModifyFile", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "modify_file.txt")
		err := os.WriteFile(filePath, []byte("initial"), 0644)
		if err != nil {
			t.Fatalf("Failed to write initial file: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, filePath, defaultTestTimeout) // Consume create event

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

		expectAction(t, broker, fsbroker.Write, filePath, defaultTestTimeout)

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("ClearFile", func(t *testing.T) {
		// This relies on DarwinChmodAsModify=true in config for macOS
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "clear_file.txt")
		err := os.WriteFile(filePath, []byte("not empty"), 0644)
		if err != nil {
			t.Fatalf("Failed to write initial file: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, filePath, defaultTestTimeout) // Consume create event

		err = os.Truncate(filePath, 0)
		if err != nil {
			t.Fatalf("Failed to truncate file: %v", err)
		}

		expectAction(t, broker, fsbroker.Write, filePath, defaultTestTimeout)

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("RenameFileInplace", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		oldPath := filepath.Join(watchDir, "old_name.txt")
		newPath := filepath.Join(watchDir, "new_name.txt")

		err := os.WriteFile(oldPath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, oldPath, defaultTestTimeout) // Consume create event

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to rename file: %v", err)
		}

		action := expectAction(t, broker, fsbroker.Rename, newPath, defaultTestTimeout)
		if oldPathProp, ok := action.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename action.missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, action.Properties["OldPath"])
		}

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MoveFileWatchedToWatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		subDir := filepath.Join(watchDir, "subdir")
		err := os.Mkdir(subDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create subdir: %v", err)
		}
		// Explicitly consume the Create action.for the subdirectory
		expectAction(t, broker, fsbroker.Create, subDir, defaultTestTimeout)
		// Drain any other potential related action. just in case
		drainEvents(broker, config.Timeout/2)

		oldPath := filepath.Join(watchDir, "move_me.txt")
		newPath := filepath.Join(subDir, "move_me.txt") // Same name, different dir

		err = os.WriteFile(oldPath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, oldPath, defaultTestTimeout) // Consume create event
		// Drain any other potential related action. just in case
		drainEvents(broker, config.Timeout/2)

		time.Sleep(50 * time.Millisecond) // Delay << config.Timeout

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to move file: %v", err)
		}

		action := expectAction(t, broker, fsbroker.Rename, newPath, defaultTestTimeout)
		if oldPathProp, ok := action.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename action.missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, action.Properties["OldPath"])
		}

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MoveFileUnwatchedToWatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		externalDir := filepath.Dir(watchDir) // Parent temp dir
		externalPath := filepath.Join(externalDir, "external.txt")
		finalPath := filepath.Join(watchDir, "external.txt")

		err := os.WriteFile(externalPath, []byte("external content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create external file: %v", err)
		}
		// No action.expected yet as it's outside watchDir

		time.Sleep(50 * time.Millisecond) // Delay << config.Timeout

		err = os.Rename(externalPath, finalPath)
		if err != nil {
			t.Fatalf("Failed to move file into watched dir: %v", err)
		}

		expectAction(t, broker, fsbroker.Create, finalPath, defaultTestTimeout)

		// Verify map state at the end
		verifyWatchmapState(t, broker, config, watchDir)
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
		expectAction(t, broker, fsbroker.Create, internalPath, defaultTestTimeout) // Consume create
		// Drain potential Write action.associated with create before moving
		drainEvents(broker, config.Timeout/2)

		err = os.Rename(internalPath, externalPath)
		if err != nil {
			t.Fatalf("Failed to move file out of watched dir: %v", err)
		}

		expectAction(t, broker, fsbroker.Remove, internalPath, defaultTestTimeout)

		// Verify map state (should be empty)
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("HardDeleteFile", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		filePath := filepath.Join(watchDir, "delete_me.txt")
		err := os.WriteFile(filePath, []byte("to be deleted"), 0644)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, filePath, defaultTestTimeout) // Consume create

		err = os.Remove(filePath)
		if err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}

		expectAction(t, broker, fsbroker.Remove, filePath, defaultTestTimeout)

		// Verify map state (should be empty)
		verifyWatchmapState(t, broker, config, watchDir)
	})

	// --- Directory Tests ---

	t.Run("CreateDirectory", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		dirPath := filepath.Join(watchDir, "new_dir")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		expectAction(t, broker, fsbroker.Create, dirPath, defaultTestTimeout)

		// Verify map state
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("RenameDirectoryInplace", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		oldPath := filepath.Join(watchDir, "old_dir")
		newPath := filepath.Join(watchDir, "new_dir")

		err := os.Mkdir(oldPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, oldPath, defaultTestTimeout) // Consume create

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to rename directory: %v", err)
		}

		action := expectAction(t, broker, fsbroker.Rename, newPath, defaultTestTimeout)
		if oldPathProp, ok := action.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename action.missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, action.Properties["OldPath"])
		}

		// Verify map state
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MoveDirectoryWatchedToWatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		parentDir := filepath.Join(watchDir, "parent_dir")
		err := os.Mkdir(parentDir, 0755)
		if err != nil {
			t.Fatalf("Failed to create parent dir: %v", err)
		}
		// Consume parent create
		expectAction(t, broker, fsbroker.Create, parentDir, defaultTestTimeout)
		drainEvents(broker, config.Timeout/2)

		oldPath := filepath.Join(watchDir, "move_this_dir")
		newPath := filepath.Join(parentDir, "move_this_dir") // Moved inside parent

		err = os.Mkdir(oldPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create dir to move: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, oldPath, defaultTestTimeout) // Consume create event
		drainEvents(broker, config.Timeout/2)

		time.Sleep(50 * time.Millisecond) // Delay << config.Timeout

		err = os.Rename(oldPath, newPath)
		if err != nil {
			t.Fatalf("Failed to move directory: %v", err)
		}

		action := expectAction(t, broker, fsbroker.Rename, newPath, defaultTestTimeout)
		if oldPathProp, ok := action.Properties["OldPath"].(string); !ok || oldPathProp != oldPath {
			t.Errorf("Rename action.missing or incorrect 'OldPath' property. Expected %s, Got %v", oldPath, action.Properties["OldPath"])
		}

		// Verify map state
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MoveDirectoryUnwatchedToWatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
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

		expectAction(t, broker, fsbroker.Create, finalPath, defaultTestTimeout)

		// Verify map state
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MoveDirectoryWatchedToUnwatched", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		internalPath := filepath.Join(watchDir, "internal_dir")
		externalPath := filepath.Join(filepath.Dir(watchDir), "moved_out_dir")

		err := os.Mkdir(internalPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create internal dir: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, internalPath, defaultTestTimeout) // Consume create
		drainEvents(broker, config.Timeout/2)

		time.Sleep(50 * time.Millisecond) // Delay << config.Timeout

		err = os.Rename(internalPath, externalPath)
		if err != nil {
			t.Fatalf("Failed to move directory out of watched dir: %v", err)
		}

		expectAction(t, broker, fsbroker.Remove, internalPath, defaultTestTimeout)

		// Verify map state (should be empty)
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("HardDeleteDirectory", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		defer cleanup()

		dirPath := filepath.Join(watchDir, "delete_this_dir")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}
		// Create a file inside to test recursive delete action.(though fsbroker might only report the top dir remove)
		filePath := filepath.Join(dirPath, "inner_file.txt")
		err = os.WriteFile(filePath, []byte("content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create inner file: %v", err)
		}

		expectAction(t, broker, fsbroker.Create, dirPath, defaultTestTimeout) // Consume dir create
		// We might get a create for the inner file too depending on timing, drain it.
		drainEvents(broker, config.Timeout/2)

		err = os.RemoveAll(dirPath)
		if err != nil {
			t.Fatalf("Failed to remove directory: %v", err)
		}

		// Wait for the first Remove action.related to the deletion
		t.Logf("Waiting for first Remove action.after RemoveAll(%s)", dirPath)
		var firstRemoveEvent *fsbroker.FSAction
		// Use a timeout relative to the default broker config + buffer
		removeWaitTimeout := config.Timeout + 500*time.Millisecond
		timer := time.NewTimer(removeWaitTimeout)
		defer timer.Stop()

	loop:
		for {
			select {
			case action := <-broker.Next():
				if action.Type == fsbroker.NoOp {
					if action.Subject != nil {
						t.Logf("HardDeleteDirectory: Ignoring NoOp action for path %s while waiting for Remove", action.Subject.Path)
					} else {
						t.Logf("HardDeleteDirectory: Ignoring NoOp action (subject is nil) while waiting for Remove")
					}
					continue // Keep waiting
				}
				if action.Type != fsbroker.Remove {
					// Fail if we receive a non-NoOp, non-Remove action first
					path := "<nil subject>"
					if action.Subject != nil {
						path = action.Subject.Path
					}
					t.Fatalf("Expected first non-NoOp action to be Remove, got %v for path %s", action.Type, path)
				}
				firstRemoveEvent = action
				t.Logf("Received first remove action. Path=%s", action.Subject.Path) // Safe now, type is Remove
				break loop                                                           // Exit the waiting loop
			case err := <-broker.Error():
				t.Fatalf("Received unexpected error while waiting for Remove: %v", err)
			case <-timer.C:
				t.Fatalf("Timeout waiting for Remove action.after deleting directory %s (timeout: %v)", dirPath, removeWaitTimeout)
			}
		}

		// Check if the path matches the dir OR the inner file path
		relEventPath, _ := filepath.Rel(watchDir, firstRemoveEvent.Subject.Path)
		relDirPath, _ := filepath.Rel(watchDir, dirPath)
		relFilePath, _ := filepath.Rel(watchDir, filePath)

		if relEventPath != relDirPath && relEventPath != relFilePath {
			t.Errorf("Expected Remove action.path to be relative '%s' or '%s', but got '%s' (Absolute: %s)", relDirPath, relFilePath, relEventPath, firstRemoveEvent.Subject.Path)
		} else {
			t.Logf("Received Remove action.path '%s' matches expected paths ('%s' or '%s')", relEventPath, relDirPath, relFilePath)
		}

		// Since the broker doesn't currently consolidate these, drain for a cycle
		// to consume the potential second Remove action.
		t.Logf("Draining potential subsequent Remove action...")
		// Cast duration for multiplication
		drainEvents(broker, time.Duration(float64(config.Timeout)*1.5))

		// Check no *other* unexpected action. follow.
		t.Logf("Checking for further unexpected action....")
		expectNoEvent(t, broker, config.Timeout/2) // Use a shorter timeout here

		// Verify map state (should be empty)
		verifyWatchmapState(t, broker, config, watchDir)
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

		// Call the OS-specific function (defined in fsbroker_test_windows.go)
		fsbroker.SetHiddenAttribute(t, externalPath)

		// 3. Move the file into the watched directory
		err = os.Rename(externalPath, finalPath)
		if err != nil {
			t.Fatalf("Failed to move hidden file into watched dir: %v", err)
		}

		// 4. Expect *no* action.because the file entering the watch dir is hidden
		expectNoEvent(t, broker, time.Duration(float64(config.Timeout)*1.5)) // Wait a bit longer than one cycle

		// Verify map state (should be empty as hidden file was ignored)
		verifyWatchmapState(t, broker, config, watchDir)
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

		// Expect *no* action.because the file should be ignored
		expectNoEvent(t, broker, time.Duration(float64(config.Timeout)*1.5))

		// Verify map state (should be empty as system file was ignored)
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("WriteDeduplication", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout to ensure all rapid writes fall within one tick
		// Make sure the test timeout is longer than the broker timeout
		config.Timeout = 2 * time.Second
		testWaitTimeout := config.Timeout + 500*time.Millisecond
		defer cleanup()

		filePath := filepath.Join(watchDir, "dedup_write.txt")

		// Initial create
		err := os.WriteFile(filePath, []byte("first"), 0644)
		if err != nil {
			t.Fatalf("Failed initial write: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, filePath, testWaitTimeout) // Consume create

		// Perform rapid writes
		t.Logf("Performing rapid writes...")
		numWrites := 5
		for i := range numWrites {
			f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("Failed to open for append %d: %v", i, err)
			}
			_, err = f.WriteString(fmt.Sprintf("\nwrite %d", i+1))
			f.Close() // Close immediately after write
			if err != nil {
				t.Fatalf("Failed append %d: %v", i, err)
			}
			time.Sleep(5 * time.Millisecond) // Delay << config.Timeout
		}
		t.Logf("Finished rapid writes.")

		// Expect only ONE consolidated Write event
		expectAction(t, broker, fsbroker.Write, filePath, testWaitTimeout)

		// Ensure no other action. follow immediately
		expectNoEvent(t, broker, config.Timeout/3)

		// Verify map state
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("DeleteDeduplication", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout to ensure rapid actions fall within one tick
		// Make sure the test timeout is longer than the broker timeout
		config.Timeout = 2 * time.Second
		testWaitTimeout := config.Timeout + 500*time.Millisecond
		defer cleanup()

		filePath := filepath.Join(watchDir, "dedup_delete.txt")

		// Initial create
		err := os.WriteFile(filePath, []byte("to be deleted"), 0644)
		if err != nil {
			t.Fatalf("Failed initial write: %v", err)
		}
		expectAction(t, broker, fsbroker.Create, filePath, testWaitTimeout) // Consume create

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

		// Expect only ONE Remove action. the write should be ignored
		expectAction(t, broker, fsbroker.Remove, filePath, testWaitTimeout)

		// Ensure no other action. follow immediately (like the ignored write)
		expectNoEvent(t, broker, config.Timeout/3)

		// Verify map state (should be empty)
		verifyWatchmapState(t, broker, config, watchDir)
	})

	t.Run("MixedRapidActions", func(t *testing.T) {
		broker, config, watchDir, cleanup := setupTestEnv(t)
		// Extend timeout significantly for this specific test to ensure all actions
		// likely fall within a single processing tick.
		config.Timeout = 2 * time.Second
		// Set the collection timeout based on the configured broker timeout + buffer
		collectionTimeout := config.Timeout + 500*time.Millisecond
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

		// --- Collect all action. that arrive within a reasonable timeframe ---
		t.Logf("Collecting action....")
		receivedEvents := make(map[string]*fsbroker.FSAction)
		stopTimer := time.NewTimer(collectionTimeout)
	collectLoop:
		for {
			select {
			case action := <-broker.Next():
				if action.Type == fsbroker.NoOp {
					// Log NoOp, handling potential nil subject
					if action.Subject != nil {
						t.Logf("Ignoring NoOp action during collection: Path=%s", action.Subject.Path)
					} else {
						t.Logf("Ignoring NoOp action during collection (subject is nil)")
					}
					continue // Skip NoOp actions
				}
				// Non-NoOp actions MUST have a subject. Panic if not.
				t.Logf("Collected Event: Type=%v, Path=%s, OldPath=%v", action.Type, action.Subject.Path, action.Properties["OldPath"])
				// Use signature based on final path for uniqueness in this test
				actionsig := fmt.Sprintf("%s-%s", action.Type, action.Subject.Path)
				receivedEvents[actionsig] = action
			case err := <-broker.Error():
				t.Errorf("Received unexpected error during collection: %v", err)
			case <-stopTimer.C:
				break collectLoop
			}
		}
		t.Logf("Finished collecting action after %v. Got %d action(s).", collectionTimeout, len(receivedEvents))

		// --- Assert expected final action. ---
		expectedSignatures := map[string]struct{}{ // Use a set for easy checking
			fmt.Sprintf("%s-%s", fsbroker.Create, fileA1): {}, // Rename A -> A1
			fmt.Sprintf("%s-%s", fsbroker.Create, fileB):  {}, // Write B -> should be create because it's the first time we see it after rename
		}

		if len(receivedEvents) != len(expectedSignatures) {
			t.Errorf("Expected %d final action., but got %d", len(expectedSignatures), len(receivedEvents))
		}

		for sig, action := range receivedEvents {
			if _, ok := expectedSignatures[sig]; !ok {
				t.Errorf("Received unexpected final action. Type=%v, Path=%s", action.Type, action.Subject.Path)
			}
			// Specific checks for properties
			if action.Type == fsbroker.Rename && action.Subject.Path == fileA1 {
				if oldPath, _ := action.Properties["OldPath"].(string); oldPath != fileA {
					t.Errorf("Rename action.for %s has incorrect OldPath: got %q, want %q", fileA1, oldPath, fileA)
				}
			}
			// Could add checks for other properties if needed
		}

		for sig := range expectedSignatures {
			if _, ok := receivedEvents[sig]; !ok {
				t.Errorf("Missing expected final action.with signature: %s", sig)
			}
		}

		// Verify final map state matches the expected disk state
		verifyWatchmapState(t, broker, config, watchDir)

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
