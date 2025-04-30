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

func (es *EventStack) Push(action *FSEvent) {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	es.stack = append(es.stack, action)
}

func (es *EventStack) Pop() *FSEvent {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	if len(es.stack) == 0 {
		return nil
	}
	action := es.stack[len(es.stack)-1]
	es.stack = es.stack[0 : len(es.stack)-1]
	return action
}

func (es *EventStack) List() []*FSEvent {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	// Create a new slice with the same length and capacity
	stackCopy := make([]*FSEvent, len(es.stack))
	// Copy the elements
	copy(stackCopy, es.stack)
	return stackCopy
}

func (es *EventStack) Map() map[string]*FSEvent {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	// Create a new map with same length and capacity
	stackMap := make(map[string]*FSEvent, len(es.stack))
	// Construct the elements
	for _, e := range es.stack {
		stackMap[e.Signature()] = e
	}
	return stackMap
}

func (es *EventStack) Delete(action *FSEvent) bool {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	for i, e := range es.stack {
		if e == action {
			es.stack = slices.Delete(es.stack, i, i+1)
			return true // Indicate deletion occurred
		}
	}
	return false // Indicate element was not found/deleted
}

func (es *EventStack) Clear() {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	es.stack = make([]*FSEvent, 0)
}

func (es *EventStack) Len() int {
	es.stackLock.Lock()
	defer es.stackLock.Unlock()
	return len(es.stack)
}
