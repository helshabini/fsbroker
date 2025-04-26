package fsbroker

import (
	"testing"
	"time"
)

func TestNewFSEvent(t *testing.T) {
	t.Run("Basic event creation", func(t *testing.T) {
		path := "/test/path"
		now := time.Now()
		event := NewFSEvent(Create, path, now)

		if event.Type != Create {
			t.Errorf("Expected event type Create, got %v", event.Type)
		}
		if event.Path != path {
			t.Errorf("Expected path %s, got %s", path, event.Path)
		}
		if event.Timestamp != now {
			t.Errorf("Expected timestamp %v, got %v", now, event.Timestamp)
		}
		if event.Properties == nil {
			t.Errorf("Expected Properties map to be initialized, got nil")
		}
	})

	t.Run("Event with properties", func(t *testing.T) {
		path := "/test/path"
		now := time.Now()
		event := NewFSEvent(Write, path, now)

		// Add some properties
		event.Properties["size"] = "1024"
		event.Properties["modified"] = "true"

		if len(event.Properties) != 2 {
			t.Errorf("Expected 2 properties, got %d", len(event.Properties))
		}
		if event.Properties["size"] != "1024" {
			t.Errorf("Expected size property to be 1024, got %s", event.Properties["size"])
		}
		if event.Properties["modified"] != "true" {
			t.Errorf("Expected modified property to be true, got %s", event.Properties["modified"])
		}
	})

	t.Run("Event with empty path", func(t *testing.T) {
		event := NewFSEvent(Remove, "", time.Now())
		if event.Path != "" {
			t.Errorf("Expected empty path, got %s", event.Path)
		}
	})

	t.Run("Event with nil timestamp", func(t *testing.T) {
		event := NewFSEvent(Chmod, "/test/path", time.Time{})
		if !event.Timestamp.IsZero() {
			t.Errorf("Expected zero timestamp, got %v", event.Timestamp)
		}
	})
}

func TestEventTypeString(t *testing.T) {
	tests := []struct {
		eventType EventType
		expected  string
	}{
		{Create, "Create"},
		{Write, "Write"},
		{Rename, "Rename"},
		{Remove, "Remove"},
		{Chmod, "Chmod"},
		{EventType(999), "Unknown"}, // Test unknown event type
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.eventType.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, tt.eventType.String())
			}
		})
	}
}

func TestFSEventSignature(t *testing.T) {
	t.Run("Basic signature", func(t *testing.T) {
		event := NewFSEvent(Create, "/test/path", time.Now())
		expectedSignature := "0-/test/path"
		if event.Signature() != expectedSignature {
			t.Errorf("Expected signature %s, got %s", expectedSignature, event.Signature())
		}
	})

	t.Run("Signature with different event types", func(t *testing.T) {
		tests := []struct {
			eventType EventType
			path      string
			expected  string
		}{
			{Create, "/test/path", "0-/test/path"},
			{Write, "/test/path", "1-/test/path"},
			{Rename, "/test/path", "2-/test/path"},
			{Remove, "/test/path", "3-/test/path"},
			{Chmod, "/test/path", "4-/test/path"},
		}

		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				event := NewFSEvent(tt.eventType, tt.path, time.Now())
				if event.Signature() != tt.expected {
					t.Errorf("Expected signature %s, got %s", tt.expected, event.Signature())
				}
			})
		}
	})

	t.Run("Signature with empty path", func(t *testing.T) {
		event := NewFSEvent(Create, "", time.Now())
		expectedSignature := "0-"
		if event.Signature() != expectedSignature {
			t.Errorf("Expected signature %s, got %s", expectedSignature, event.Signature())
		}
	})

	t.Run("Signature with special characters in path", func(t *testing.T) {
		path := "/test/path with spaces/and/special@characters"
		event := NewFSEvent(Create, path, time.Now())
		expectedSignature := "0-" + path
		if event.Signature() != expectedSignature {
			t.Errorf("Expected signature %s, got %s", expectedSignature, event.Signature())
		}
	})
}
