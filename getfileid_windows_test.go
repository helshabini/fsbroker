package fsbroker

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

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
