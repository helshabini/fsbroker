package fsbroker

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestSplitOps(t *testing.T) {
	tests := []struct {
		op   fsnotify.Op
		want []fsnotify.Op
	}{
		{fsnotify.Write, []fsnotify.Op{fsnotify.Write}},
		{fsnotify.Write | fsnotify.Chmod, []fsnotify.Op{fsnotify.Write, fsnotify.Chmod}},
		{fsnotify.Chmod | fsnotify.Rename, []fsnotify.Op{fsnotify.Chmod, fsnotify.Rename}},
		{fsnotify.Remove | fsnotify.Create, []fsnotify.Op{fsnotify.Create, fsnotify.Remove}},
		{0, nil},
	}
	for _, tt := range tests {
		if got := splitOps(tt.op); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitOps(%v) = %v, want %v", tt.op, got, tt.want)
		}
	}
}

// TestCombinedOpEvent checks that an fsnotify event carrying several
// operations is not dropped. kqueue reports a truncate followed by a write
// as a single WRITE|CHMOD event when both happen before it is read, which
// used to match none of the handled operations and was silently discarded.
func TestCombinedOpEvent(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsbroker_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "file.txt")
	if err := os.WriteFile(filePath, []byte("data"), 0644); err != nil {
		t.Fatalf("Failed to write %s: %v", filePath, err)
	}

	config := DefaultFSConfig()
	config.Timeout = 100 * time.Millisecond

	broker, err := NewFSBroker(config)
	if err != nil {
		t.Fatalf("Failed to create FSBroker: %v", err)
	}
	if err := broker.AddWatch(tempDir); err != nil {
		broker.Stop()
		t.Fatalf("Failed to add watch on %s: %v", tempDir, err)
	}
	// Feed the broker from a channel owned by the test, as the combined event
	// cannot be produced reliably from the file system.
	events := make(chan fsnotify.Event, 1)
	broker.watcher.Events = events
	broker.Start()
	defer broker.Stop()

	events <- fsnotify.Event{Name: filePath, Op: fsnotify.Write | fsnotify.Chmod}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case action := <-broker.Next():
			if action.Type == Write && action.Subject.Path == filePath {
				return
			}
			t.Logf("Ignoring action: Type=%v, Path=%s", action.Type, action.Subject.Path)
		case err := <-broker.Error():
			t.Fatalf("Unexpected error: %v", err)
		case <-deadline:
			t.Fatalf("Timeout waiting for a Write action on %s from a WRITE|CHMOD event", filePath)
		}
	}
}
