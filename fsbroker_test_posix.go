//go:build !windows
// +build !windows

package fsbroker

import (
	"testing"
)

// SetHiddenAttribute is a no-op on non-Windows platforms.
// The actual implementation is in fsbroker_test_windows.go
func SetHiddenAttribute(t *testing.T, filePath string) {
	// No action needed on Unix-like systems where dotfiles are conventionally hidden.
	t.Logf("Skipping setHiddenAttribute on non-Windows platform for %s", filePath)
}

// testIsHiddenFile checks if a file is hidden on Unix-like systems (starts with a dot).
// Renamed to avoid potential conflicts if helper becomes exported later.
func TestIsHiddenFile(path string) (bool, error) {
	return isHiddenFile(path)
}

// testIsSystemFile checks for common and POSIX/macOS specific system file names.
// Renamed to avoid potential conflicts if helper becomes exported later.
func TestIsSystemFile(name string) bool {
	return isSystemFile(name)
}

func (b *FSBroker) TestIteratePaths(callback func(key string, value *FSInfo)) {
	b.watchmap.IteratePaths(callback)
}
