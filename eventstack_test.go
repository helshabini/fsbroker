package fsbroker

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Helper to create a dummy FSEvent
func newTestEvent(path string) *FSEvent {
	// Note: Creating a realistic fsnotify.Event isn't strictly necessary
	// for testing the stack's logic, but we need *some* event.
	fsEvent := &fsnotify.Event{Name: path, Op: fsnotify.Write}
	return &FSEvent{Path: path, Type: Write, Timestamp: time.Now(), Event: fsEvent}
}

func TestNewEventStack(t *testing.T) {
	es := NewEventStack()
	if es == nil {
		t.Fatal("NewEventStack returned nil")
	}
	if len(es.stack) != 0 {
		t.Errorf("Expected new stack to be empty, got length %d", len(es.stack))
	}
}

func TestEventStack_PushAndLen(t *testing.T) {
	es := NewEventStack()
	event1 := newTestEvent("file1.txt")
	event2 := newTestEvent("file2.txt")

	if es.Len() != 0 {
		t.Errorf("Expected initial length 0, got %d", es.Len())
	}

	es.Push(event1)
	if es.Len() != 1 {
		t.Errorf("Expected length 1 after first push, got %d", es.Len())
	}
	if es.stack[0] != event1 {
		t.Error("First pushed event not found at the start of the stack")
	}

	es.Push(event2)
	if es.Len() != 2 {
		t.Errorf("Expected length 2 after second push, got %d", es.Len())
	}
	if es.stack[1] != event2 {
		t.Error("Second pushed event not found at the end of the stack")
	}
}

func TestEventStack_Pop(t *testing.T) {
	es := NewEventStack()
	event1 := newTestEvent("file1.txt")
	event2 := newTestEvent("file2.txt")

	// Test pop from empty stack
	if popped := es.Pop(); popped != nil {
		t.Errorf("Expected nil when popping from empty stack, got %v", popped)
	}

	es.Push(event1)
	es.Push(event2)

	// Test pop order (LIFO)
	popped2 := es.Pop()
	if popped2 != event2 {
		t.Errorf("Expected to pop event2, got %v", popped2)
	}
	if es.Len() != 1 {
		t.Errorf("Expected length 1 after first pop, got %d", es.Len())
	}

	popped1 := es.Pop()
	if popped1 != event1 {
		t.Errorf("Expected to pop event1, got %v", popped1)
	}
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after second pop, got %d", es.Len())
	}

	// Test pop from empty stack again
	if popped := es.Pop(); popped != nil {
		t.Errorf("Expected nil when popping from empty stack after pops, got %v", popped)
	}
}

func TestEventStack_List(t *testing.T) {
	es := NewEventStack()
	event1 := newTestEvent("file1.txt")
	event2 := newTestEvent("file2.txt")

	// Test list on empty stack
	if list := es.List(); len(list) != 0 {
		t.Errorf("Expected empty list, got %v", list)
	}

	es.Push(event1)
	es.Push(event2)

	list := es.List()
	expectedList := []*FSEvent{event1, event2}
	if !reflect.DeepEqual(list, expectedList) {
		t.Errorf("Expected list %v, got %v", expectedList, list)
	}

	// Ensure List returns a copy
	list[0] = nil // Modify the returned list
	if es.stack[0] == nil {
		t.Error("Modification of list returned by List affected the internal stack")
	}
	if es.Len() != 2 {
		t.Errorf("Internal stack length changed unexpectedly, expected 2, got %d", es.Len())
	}
}

func TestEventStack_Delete(t *testing.T) {
	es := NewEventStack()
	event1 := newTestEvent("file1.txt")
	event2 := newTestEvent("file2.txt")
	event3 := newTestEvent("file3.txt")
	nonExistentEvent := newTestEvent("other.txt")

	es.Push(event1)
	es.Push(event2)
	es.Push(event3) // Stack: [event1, event2, event3]

	// Test deleting a non-existent event
	es.Delete(nonExistentEvent)
	if es.Len() != 3 {
		t.Errorf("Expected length 3 after deleting non-existent event, got %d", es.Len())
	}

	// Test deleting middle element
	es.Delete(event2) // Stack: [event1, event3]
	if es.Len() != 2 {
		t.Errorf("Expected length 2 after deleting event2, got %d", es.Len())
	}
	expectedStackAfterDelete2 := []*FSEvent{event1, event3}
	if !reflect.DeepEqual(es.stack, expectedStackAfterDelete2) {
		t.Errorf("Expected stack %v after deleting event2, got %v", expectedStackAfterDelete2, es.stack)
	}

	// Test deleting first element
	es.Delete(event1) // Stack: [event3]
	if es.Len() != 1 {
		t.Errorf("Expected length 1 after deleting event1, got %d", es.Len())
	}
	expectedStackAfterDelete1 := []*FSEvent{event3}
	if !reflect.DeepEqual(es.stack, expectedStackAfterDelete1) {
		t.Errorf("Expected stack %v after deleting event1, got %v", expectedStackAfterDelete1, es.stack)
	}

	// Test deleting last element
	es.Delete(event3) // Stack: []
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after deleting event3, got %d", es.Len())
	}
	if len(es.stack) != 0 {
		t.Errorf("Expected empty stack after deleting event3, got %v", es.stack)
	}

	// Test deleting from empty stack
	es.Delete(event1)
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after deleting from empty stack, got %d", es.Len())
	}
}

func TestEventStack_Clear(t *testing.T) {
	es := NewEventStack()
	event1 := newTestEvent("file1.txt")
	event2 := newTestEvent("file2.txt")

	// Test clear on empty stack
	es.Clear()
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after clearing empty stack, got %d", es.Len())
	}

	es.Push(event1)
	es.Push(event2)

	if es.Len() != 2 {
		t.Errorf("Expected length 2 before clear, got %d", es.Len())
	}

	es.Clear()
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", es.Len())
	}
	if len(es.stack) != 0 {
		t.Errorf("Expected internal stack to be empty after clear, got %v", es.stack)
	}
}

func TestEventStack_Concurrency(t *testing.T) {
	es := NewEventStack()
	numGoroutines := 100
	numOpsPerGoroutine := 50
	var wg sync.WaitGroup

	// Concurrent Pushes
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(gIndex int) {
			defer wg.Done()
			for j := range numOpsPerGoroutine {
				event := newTestEvent("file_" + string(rune(gIndex)) + "_" + string(rune(j)))
				es.Push(event)
			}
		}(i)
	}
	wg.Wait()

	expectedLenAfterPushes := numGoroutines * numOpsPerGoroutine
	if es.Len() != expectedLenAfterPushes {
		t.Errorf("Expected length %d after concurrent pushes, got %d", expectedLenAfterPushes, es.Len())
	}

	// Concurrent Pops and Deletes
	poppedCount := 0
	deletedCount := 0
	var popLock sync.Mutex // To safely count pops

	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for j := range numOpsPerGoroutine { // Mix of Pops and Deletes
				if j%2 == 0 {
					// Pop
					if popped := es.Pop(); popped != nil {
						popLock.Lock()
						poppedCount++
						popLock.Unlock()
					}
				} else {
					// Delete (attempt to delete something, might be already popped)
					// For simplicity, just try deleting the first element if it exists.
					// A more robust test might try deleting specific known elements.
					list := es.List() // Get a snapshot
					if len(list) > 0 {
						if es.Delete(list[0]) { // Check if delete was successful (Delete should probably return bool)
							popLock.Lock()
							deletedCount++ // Not perfectly accurate due to races, but gives an idea
							popLock.Unlock()
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	// We can't know the exact final length due to the mix of pops and deletes,
	// but it should be less than the initial count after pushes.
	if es.Len() >= expectedLenAfterPushes {
		t.Errorf("Expected length to decrease after concurrent pops/deletes, but it was %d (started at %d)", es.Len(), expectedLenAfterPushes)
	}
	t.Logf("Stack length after concurrent pops/deletes: %d (popped: %d, potentially deleted: %d)", es.Len(), poppedCount, deletedCount)

	// Test Clear under concurrency (less critical as individual ops are locked)
	es.Clear()
	if es.Len() != 0 {
		t.Errorf("Expected length 0 after final Clear, got %d", es.Len())
	}
}
