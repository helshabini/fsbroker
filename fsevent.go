package fsbroker

import (
	"fmt"
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

func (e *FSEvent) Signature() string {
	return fmt.Sprintf("%s-%s", e.Type, e.Path)
}
