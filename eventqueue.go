package fsbroker

import (
	"slices"
	"sync"
)

type EventQueue struct {
	queue     []*FSEvent
	queueLock sync.Mutex
}

func NewEventQueue() *EventQueue {
	return &EventQueue{
		queue:     make([]*FSEvent, 0),
		queueLock: sync.Mutex{},
	}
}

func (eq *EventQueue) Push(event *FSEvent) {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	eq.queue = append(eq.queue, event)
}

func (eq *EventQueue) Pop() *FSEvent {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	if len(eq.queue) == 0 {
		return nil
	}
	event := eq.queue[0]
	eq.queue = eq.queue[1:]
	return event
}

func (eq *EventQueue) List() []*FSEvent {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	// Create a new slice with the same length and capacity
	queueCopy := make([]*FSEvent, len(eq.queue))
	// Copy the elements
	copy(queueCopy, eq.queue)
	return queueCopy
}

func (eq *EventQueue) Delete(event *FSEvent) {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	for i, e := range eq.queue {
		if e == event {
			eq.queue = slices.Delete(eq.queue, i, i+1)
			return
		}
	}
}

func (eq *EventQueue) Clear() {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	eq.queue = make([]*FSEvent, 0)
}

func (eq *EventQueue) Len() int {
	eq.queueLock.Lock()
	defer eq.queueLock.Unlock()
	return len(eq.queue)
}
