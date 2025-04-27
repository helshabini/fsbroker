//go:build windows
// +build windows

package fsbroker

import (
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
