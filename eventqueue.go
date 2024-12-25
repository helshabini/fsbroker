package fsbroker

import (
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
	return eq.queue
}
