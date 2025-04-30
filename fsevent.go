package fsbroker

import (
	"time"

	"github.com/fsnotify/fsnotify"
)


type FSEvent struct {
	Event *fsnotify.Event
	Type  OpType
	Path string
	Timestamp time.Time
}

func NewFSEvent(event *fsnotify.Event) *FSEvent {
	return &FSEvent{
		Event:     event,
		Type:      mapOpToOpType(event.Op),
		Path: 		event.Name,
		Timestamp: time.Now(),
	}
}
