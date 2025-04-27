//go:build !windows
// +build !windows

package fsbroker

import "testing"

// SetHiddenAttribute is a no-op on non-Windows platforms.
// The actual implementation is in fsbroker_test_windows.go
func SetHiddenAttribute(t *testing.T, filePath string) {
	// No action needed on Unix-like systems where dotfiles are conventionally hidden.
	t.Logf("Skipping setHiddenAttribute on non-Windows platform for %s", filePath)
}
