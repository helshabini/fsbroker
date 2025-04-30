package fsbroker

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestFSInfo_IsDir(t *testing.T) {
	tests := []struct {
		name     string
		mode     os.FileMode
		expected bool
	}{
		{
			name:     "Directory Mode",
			mode:     os.ModeDir | 0755,
			expected: true,
		},
		{
			name:     "Regular File Mode",
			mode:     0644,
			expected: false,
		},
		{
			name:     "Symlink Mode (not dir)", // Symlinks themselves aren't dirs via Mode().IsDir()
			mode:     os.ModeSymlink | 0777,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &FSInfo{
				Mode: uint32(tt.mode),
			}
			if got := info.IsDir(); got != tt.expected {
				t.Errorf("IsDir() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFSInfo_String(t *testing.T) {
	now := time.Now()
	info := &FSInfo{
		Id:      12345,
		Path:    "/some/test/path.txt",
		Size:    1024,
		ModTime: now,
		Mode:    0644, // Octal 644
	}

	expected := fmt.Sprintf("Id: 12345, Path: /some/test/path.txt, Mode: %o", uint32(0644))
	if got := info.String(); got != expected {
		t.Errorf("String() = %q, want %q", got, expected)
	}

	// Test with a directory mode
	infoDir := &FSInfo{
		Id:      54321,
		Path:    "/another/dir",
		Size:    4096,
		ModTime: now,
		Mode:    uint32(os.ModeDir | 0755), // Octal 755 + Dir bit
	}
	expectedDir := fmt.Sprintf("Id: 54321, Path: /another/dir, Mode: %o", uint32(os.ModeDir|0755))
	if gotDir := infoDir.String(); gotDir != expectedDir {
		t.Errorf("String() for dir = %q, want %q", gotDir, expectedDir)
	}
}
