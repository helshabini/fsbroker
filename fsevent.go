package fsbroker

import (
	"fmt"
	"os"
	"time"
)

type EventType int

const (
	Create EventType = iota
	Write
	Rename
	Remove
	Chmod
)

func (t *EventType) String() string {
	switch *t {
	case Create:
		return "Create"
	case Write:
		return "Write"
	case Rename:
		return "Rename"
	case Remove:
		return "Remove"
	case Chmod:
		return "Chmod"
	default:
		return "Unknown"
	}
}

type FSEvent struct {
	Type       EventType
	Path       string
	Timestamp  time.Time
	Properties map[string]any
}

func NewFSEvent(op EventType, path string, timestamp time.Time) *FSEvent {
	return &FSEvent{
		Type:       op,
		Path:       path,
		Timestamp:  timestamp,
		Properties: make(map[string]any),
	}
}

func (action *FSEvent) Signature() string {
	return fmt.Sprintf("%d-%s", action.Type, action.Path)
}

func (action *FSEvent) EnrichFromInfo(info *Info) {
	action.Properties["Id"] = info.Id
	action.Properties["Size"] = info.Size
	action.Properties["ModTime"] = info.ModTime
	action.Properties["Mode"] = info.Mode
}

func (action *FSEvent) EnrichFromStat(stat os.FileInfo) {
	action.Properties["Size"] = stat.Size()
	action.Properties["ModTime"] = stat.ModTime()
	action.Properties["Mode"] = uint32(stat.Mode())
}
