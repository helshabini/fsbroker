package fsbroker

import (
	"reflect"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestNewFSAction(t *testing.T) {
	now := time.Now()
	testPath := "/test/path/new"
	testOp := Create

	action := NewFSAction(testOp, testPath, now)

	if action == nil {
		t.Fatal("NewFSAction returned nil")
	}
	if action.Type != testOp {
		t.Errorf("FSAction.Type = %v, want %v", action.Type, testOp)
	}
	if !action.Timestamp.Equal(now) {
		t.Errorf("FSAction.Timestamp = %v, want %v", action.Timestamp, now)
	}
	// NewFSAction doesn't set Subject directly
	if action.Subject != nil {
		t.Errorf("FSAction.Subject expected nil, got %v", action.Subject)
	}
	// NewFSAction doesn't use the path argument directly in the struct
	// We can't directly test the path argument was stored unless it was used in Subject
	if action.Events == nil {
		t.Error("FSAction.Events is nil, want initialized slice")
	}
	if len(action.Events) != 0 {
		t.Errorf("FSAction.Events length = %d, want 0", len(action.Events))
	}
	if action.Properties == nil {
		t.Error("FSAction.Properties is nil, want initialized map")
	}
	if len(action.Properties) != 0 {
		t.Errorf("FSAction.Properties length = %d, want 0", len(action.Properties))
	}
}

func TestFromFSEvent(t *testing.T) {
	now := time.Now()
	testPath := "/test/path/from_event"
	fsEvent := &fsnotify.Event{
		Name: testPath,
		Op:   fsnotify.Write,
	}
	fse := &FSEvent{
		Event:     fsEvent,
		Type:      Write,
		Path:      testPath,
		Timestamp: now,
	}

	action := FromFSEvent(fse)

	if action == nil {
		t.Fatal("FromFSEvent returned nil")
	}
	if action.Type != Write {
		t.Errorf("FSAction.Type = %v, want %v", action.Type, Write)
	}
	if !action.Timestamp.Equal(now) {
		t.Errorf("FSAction.Timestamp = %v, want %v", action.Timestamp, now)
	}
	if action.Subject != nil {
		t.Errorf("FSAction.Subject expected nil, got %v", action.Subject)
	}
	if action.Events == nil {
		t.Error("FSAction.Events is nil, want initialized slice")
	}
	if len(action.Events) != 1 {
		t.Errorf("FSAction.Events length = %d, want 1", len(action.Events))
		return // Avoid panic on next line
	}
	if action.Events[0] != fsEvent {
		t.Errorf("FSAction.Events[0] = %p, want %p", action.Events[0], fsEvent)
	}
	if action.Properties == nil {
		t.Error("FSAction.Properties is nil, want initialized map")
	}
	if len(action.Properties) != 0 {
		t.Errorf("FSAction.Properties length = %d, want 0", len(action.Properties))
	}
}

func TestMapOpToOpType(t *testing.T) {
	tests := []struct {
		name     string
		inputOp  fsnotify.Op
		expected OpType
	}{
		{"Create", fsnotify.Create, Create},
		{"Write", fsnotify.Write, Write},
		{"Remove", fsnotify.Remove, Remove},
		{"Rename", fsnotify.Rename, Rename},
		{"Chmod", fsnotify.Chmod, Chmod},
		{"Write+Chmod", fsnotify.Write | fsnotify.Chmod, Write},      // Write has priority
		{"Create+Write", fsnotify.Create | fsnotify.Write, Create},   // Create has priority
		{"Remove+Rename", fsnotify.Remove | fsnotify.Rename, Rename}, // Rename has priority over Remove
		{"Unknown (0)", 0, -1},
		{"Unknown high bit", 1 << 30, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapOpToOpType(tt.inputOp); !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("mapOpToOpType(%v) = %v, want %v", tt.inputOp, got, tt.expected)
			}
		})
	}
}
