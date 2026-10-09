package fsbroker

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

// TestIsHiddenFileDeletePending checks that a directory that has been deleted
// while a handle is still open on it is not reported as an error. Such a
// directory lingers in a delete pending state, where GetFileAttributes fails
// with ERROR_ACCESS_DENIED instead of ERROR_FILE_NOT_FOUND. A watched
// directory is in that state while fsnotify still holds its watch handle.
func TestIsHiddenFileDeletePending(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsbroker_test_*")
	if err != nil {
		t.Fatalf("Windows: Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dirPath := filepath.Join(tempDir, "delete_pending")
	if err := os.Mkdir(dirPath, 0755); err != nil {
		t.Fatalf("Windows: Failed to create %s: %v", dirPath, err)
	}

	pointer, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		t.Fatalf("Windows: Failed to get UTF16 pointer for %s: %v", dirPath, err)
	}

	// Mark the directory for deletion through an open handle. Unlike a plain
	// os.Remove, which newer Windows versions may complete immediately with
	// POSIX semantics, this always leaves it delete pending until the handle
	// is closed.
	handle, err := windows.CreateFile(pointer, windows.DELETE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatalf("Windows: Failed to open %s: %v", dirPath, err)
	}
	defer windows.CloseHandle(handle)

	deleteFile := byte(1) // FILE_DISPOSITION_INFO.DeleteFile
	if err := windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &deleteFile, 1); err != nil {
		t.Fatalf("Windows: Failed to mark %s for deletion: %v", dirPath, err)
	}

	if _, err := windows.GetFileAttributes(pointer); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) {
		t.Fatalf("Windows: Expected %s to be delete pending (ERROR_ACCESS_DENIED), got: %v", dirPath, err)
	}

	hidden, err := isHiddenFile(dirPath)
	if err != nil {
		t.Errorf("Windows: Expected no error for a delete pending directory, got: %v", err)
	}
	if hidden {
		t.Errorf("Windows: Expected a delete pending directory not to be reported as hidden")
	}
}
