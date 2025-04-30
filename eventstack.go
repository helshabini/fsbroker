package fsbroker

import (
	"slices"
	"sync"
)

type EventStack struct {
	stack     []*FSEvent
	stackLock sync.Mutex
}

func NewEventStack() *EventStack {
	return &EventStack{
		stack:     make([]*FSEvent, 0),
		stackLock: sync.Mutex{},
	}
}

func (eq *EventStack) Push(action *FSEvent) {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	eq.stack = append(eq.stack, action)
}

func (eq *EventStack) Pop() *FSEvent {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	if len(eq.stack) == 0 {
		return nil
	}
	action := eq.stack[len(eq.stack)-1]
	eq.stack = eq.stack[0 : len(eq.stack)-1]
	return action
}

func (eq *EventStack) List() []*FSEvent {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	// Create a new slice with the same length and capacity
	stackCopy := make([]*FSEvent, len(eq.stack))
	// Copy the elements
	copy(stackCopy, eq.stack)
	return stackCopy
}

func (eq *EventStack) Delete(action *FSEvent) bool {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	for i, e := range eq.stack {
		if e == action {
			eq.stack = slices.Delete(eq.stack, i, i+1)
			return true // Indicate deletion occurred
		}
	}
	return false // Indicate element was not found/deleted
}

func (eq *EventStack) Clear() {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	eq.stack = make([]*FSEvent, 0)
}

func (eq *EventStack) Len() int {
	eq.stackLock.Lock()
	defer eq.stackLock.Unlock()
	return len(eq.stack)
}
