//go:build windows
// +build windows

package fsbroker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// SetHiddenAttribute sets the hidden attribute on a file in Windows.
// This is called by the IgnoreHiddenFile test.
func SetHiddenAttribute(t *testing.T, filePath string) {
	t.Helper()
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		t.Fatalf("Windows: Failed to get absolute path for hidden file: %v", err)
	}
	pointer, err := windows.UTF16PtrFromString(absPath)
	if err != nil {
		t.Fatalf("Windows: Failed to get UTF16 pointer for hidden file path: %v", err)
	}
	attributes, err := windows.GetFileAttributes(pointer)
	if err != nil {
		t.Fatalf("Windows: Failed to get file attributes for hidden file: %v", err)
	}
	newAttributes := attributes | windows.FILE_ATTRIBUTE_HIDDEN
	if err = windows.SetFileAttributes(pointer, newAttributes); err != nil {
		t.Fatalf("Windows: Failed to set hidden attribute on file: %v", err)
	}
	t.Logf("Windows: Set hidden attribute on %s", filePath)
	// Short delay to allow attribute change to potentially register fully?
	time.Sleep(50 * time.Millisecond)
}

// testIsHiddenFile checks if a file is hidden on Windows.
func TestIsHiddenFile(path string) (bool, error) {
	return isHiddenFile(path)
}

// testIsSystemFile checks for common and Windows specific system file names.
func TestIsSystemFile(name string) bool {
	return isSystemFile(name)
}

func (b *FSBroker) TestIteratePaths(callback func(key string, value *FSInfo)) {
	b.watchmap.IteratePaths(callback)
}

// TestGetFileIDShareWriteOnly checks that a file id can be read from a file
// that another handle holds open for writing without sharing reads. Opening it
// for reading fails with a sharing violation, which used to make FromOSInfo
// return nil while a watch was being registered.
func TestGetFileIDShareWriteOnly(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsbroker_test_*")
	if err != nil {
		t.Fatalf("Windows: Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "write_locked.txt")
	pointer, err := windows.UTF16PtrFromString(filePath)
	if err != nil {
		t.Fatalf("Windows: Failed to get UTF16 pointer for file path: %v", err)
	}

	handle, err := windows.CreateFile(pointer, windows.GENERIC_WRITE, windows.FILE_SHARE_WRITE, nil, windows.CREATE_ALWAYS, 0, 0)
	if err != nil {
		t.Fatalf("Windows: Failed to open %s for writing: %v", filePath, err)
	}
	defer windows.CloseHandle(handle)

	if _, err := os.Open(filePath); err == nil {
		t.Logf("Windows: os.Open unexpectedly succeeded, the sharing violation this test covers did not occur: %v", filePath)
	}

	stat, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Windows: Failed to stat %s: %v", filePath, err)
	}

	id, err := getFileID(filePath)
	if err != nil {
		t.Fatalf("Windows: Failed to get the file id of %s: %v", filePath, err)
	}
	if id == 0 {
		t.Errorf("Windows: Expected a non-zero file id for %s, got 0", filePath)
	}

	if info := FromOSInfo(filePath, stat); info == nil {
		t.Errorf("Windows: Expected file info for %s, got nil", filePath)
	}
}
