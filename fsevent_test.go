package fsbroker

import (
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestNewFSEvent(t *testing.T) {
	now := time.Now()
	// Allow a small buffer for timestamp comparison
	buffer := 50 * time.Millisecond

	tests := []struct {
		name         string
		fsnotifyOp   fsnotify.Op
		expectedType OpType
		path         string
	}{
		{
			name:         "Create Event",
			fsnotifyOp:   fsnotify.Create,
			expectedType: Create,
			path:         "/tmp/create.txt",
		},
		{
			name:         "Write Event",
			fsnotifyOp:   fsnotify.Write,
			expectedType: Write,
			path:         "/var/log/app.log",
		},
		{
			name:         "Remove Event",
			fsnotifyOp:   fsnotify.Remove,
			expectedType: Remove,
			path:         "/home/user/file_to_remove",
		},
		{
			name:         "Rename Event",
			fsnotifyOp:   fsnotify.Rename,
			expectedType: Rename,
			path:         "/docs/old_name.doc",
		},
		{
			name:         "Chmod Event",
			fsnotifyOp:   fsnotify.Chmod,
			expectedType: Chmod,
			path:         "/config/settings.conf",
		},
		{
			name:         "Combined Write+Chmod", // Should map to Write
			fsnotifyOp:   fsnotify.Write | fsnotify.Chmod,
			expectedType: Write, // mapOpToOpType prioritizes Write
			path:         "/data/combined.dat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsEvent := &fsnotify.Event{
				Name: tt.path,
				Op:   tt.fsnotifyOp,
			}

			got := NewFSEvent(fsEvent)

			if got == nil {
				t.Fatal("NewFSEvent returned nil")
			}
			if got.Event != fsEvent {
				t.Errorf("FSEvent.Event = %p, want %p", got.Event, fsEvent)
			}
			if got.Type != tt.expectedType {
				t.Errorf("FSEvent.Type = %v, want %v", got.Type, tt.expectedType)
			}
			if got.Path != tt.path {
				t.Errorf("FSEvent.Path = %q, want %q", got.Path, tt.path)
			}
			if got.Timestamp.IsZero() {
				t.Error("FSEvent.Timestamp was not set (is zero)")
			}
			// Check if timestamp is reasonably close to now
			if got.Timestamp.Before(now.Add(-buffer)) || got.Timestamp.After(now.Add(buffer)) {
				t.Errorf("FSEvent.Timestamp = %v, expected to be close to %v (within %v)", got.Timestamp, now, buffer)
			}
		})
	}
}
