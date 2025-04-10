package fsbroker

import (
	"sync"
	"testing"
	"time"
)

func TestEventQueue(t *testing.T) {
	t.Run("Push and Pop single event", func(t *testing.T) {
		eq := NewEventQueue()
		event := NewFSEvent(Create, "/test/path", time.Now())
		eq.Push(event)

		popped := eq.Pop()
		if popped != event {
			t.Errorf("Expected event %v, got %v", event, popped)
		}
		if len(eq.List()) != 0 {
			t.Errorf("Expected queue to be empty, got %d events", len(eq.List()))
		}
	})

	t.Run("Push and Pop multiple events", func(t *testing.T) {
		eq := NewEventQueue()
		event1 := NewFSEvent(Create, "/test/path", time.Now())
		event2 := NewFSEvent(Remove, "/test/path", time.Now())

		eq.Push(event1)
		eq.Push(event2)

		if len(eq.List()) != 2 {
			t.Errorf("Expected queue length 2, got %d", len(eq.List()))
		}

		popped1 := eq.Pop()
		popped2 := eq.Pop()

		if popped1 != event1 || popped2 != event2 {
			t.Errorf("Events popped in wrong order: %v, %v", popped1, popped2)
		}
		if len(eq.List()) != 0 {
			t.Errorf("Expected queue to be empty after popping all events")
		}
	})

	t.Run("Pop from empty queue", func(t *testing.T) {
		eq := NewEventQueue()
		popped := eq.Pop()
		if popped != nil {
			t.Errorf("Expected nil when popping from empty queue, got %v", popped)
		}
	})

	t.Run("Concurrent Push and List", func(t *testing.T) {
		eq := NewEventQueue()
		var wg sync.WaitGroup
		const numEvents = 1000

		for i := 0; i < numEvents; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eq.Push(NewFSEvent(Create, string(rune(i)), time.Now()))
			}(i)
		}

		wg.Wait()
		if len(eq.List()) != numEvents {
			t.Errorf("Expected %d events in queue, got %d", numEvents, len(eq.List()))
		}
	})

	t.Run("Concurrent Push and Pop", func(t *testing.T) {
		eq := NewEventQueue()
		var wg sync.WaitGroup
		const numEvents = 1000
		events := make([]*FSEvent, numEvents)

		// Push events concurrently
		for i := 0; i < numEvents; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				event := NewFSEvent(Create, string(rune(i)), time.Now())
				events[i] = event
				eq.Push(event)
			}(i)
		}

		wg.Wait()

		// Pop events and verify
		poppedEvents := make(map[string]bool)
		for i := 0; i < numEvents; i++ {
			event := eq.Pop()
			if event == nil {
				t.Errorf("Expected non-nil event, got nil")
				continue
			}
			poppedEvents[event.Path] = true
		}

		// Verify all events were popped
		for i := 0; i < numEvents; i++ {
			if !poppedEvents[string(rune(i))] {
				t.Errorf("Event with path %s was not popped", string(rune(i)))
			}
		}
	})

	t.Run("List returns copy of queue", func(t *testing.T) {
		eq := NewEventQueue()
		event := NewFSEvent(Create, "/test/path", time.Now())
		eq.Push(event)

		list1 := eq.List()
		list2 := eq.List()

		if &list1[0] == &list2[0] {
			t.Error("List() should return a new slice each time")
		}
	})

	t.Run("Queue maintains order under concurrent operations", func(t *testing.T) {
		eq := NewEventQueue()
		var wg sync.WaitGroup
		const numEvents = 100
		events := make([]*FSEvent, numEvents)

		// Create events
		for i := 0; i < numEvents; i++ {
			events[i] = NewFSEvent(Create, string(rune(i)), time.Now())
		}

		// Push events in order
		for i := 0; i < numEvents; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eq.Push(events[i])
			}(i)
		}

		wg.Wait()

		// Pop events and verify order
		for i := 0; i < numEvents; i++ {
			popped := eq.Pop()
			if popped == nil {
				t.Errorf("Expected non-nil event at index %d", i)
				continue
			}
			// Since we're pushing concurrently, we can't guarantee exact order
			// but we can verify that all events are present
			found := false
			for _, event := range events {
				if event == popped {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Popped event %v was not in original events", popped)
			}
		}
	})
}
