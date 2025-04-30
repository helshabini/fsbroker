package fsbroker

import (
	"fmt"
	"time"

	"github.com/fsnotify/fsnotify"
)

//go:generate stringer -type OpType

type OpType int

const (
	Create OpType = iota
	Write
	Rename
	Remove
	Chmod
	NoOp
)

type FSAction struct {
	Type      OpType
	Timestamp time.Time
	Subject   *FSInfo
	Events    []*FSEvent
	Properties map[string]any
}

func NewFSAction(op OpType, path string, timestamp time.Time) *FSAction {
	return &FSAction{
		Type:      op,
		Timestamp: timestamp,
		Subject:   nil,
		Events:    make([]*FSEvent, 0),
		Properties: make(map[string]any, 0),
	}
}

func FromFSEvent(event *FSEvent) *FSAction {
	action := &FSAction{
		Type:      event.Type,
		Timestamp: event.Timestamp,
		Subject:   nil,
		Events:    make([]*FSEvent, 0),
		Properties: make(map[string]any, 0),
	}

	action.Events = append(action.Events, event)

	return action
}

func (a *FSAction) Signature() string {
	return fmt.Sprintf("%s-%s", a.Type, a.Subject.Path)
}

func AppendEvent(actions map[uint64]*FSAction, event *FSEvent, id uint64) *FSAction {
	action, found := actions[id]
	if found {
		action.Events = append(action.Events, event)
		return action
	}
	
	action = FromFSEvent(event)
	actions[id] = action
	return action
}

// mapOpToActionType maps fsnotify.Op to ActionType.
func mapOpToOpType(op fsnotify.Op) OpType {
	switch {
	case op&fsnotify.Create == fsnotify.Create:
		return Create
	case op&fsnotify.Write == fsnotify.Write:
		return Write
	case op&fsnotify.Rename == fsnotify.Rename:
		return Rename
	case op&fsnotify.Remove == fsnotify.Remove:
		return Remove
	case op&fsnotify.Chmod == fsnotify.Chmod:
		return Chmod
	default:
		return -1 // Unknown event
	}
}
