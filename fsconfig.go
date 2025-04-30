package fsbroker

import (
	"time"
)

type FSConfig struct {
	Timeout           time.Duration // duration to wait for events to be grouped and processed
	IgnoreSysFiles    bool          // ignore common system files and directories
	IgnoreHiddenFiles bool          // ignore hidden files
	EmitChmod         bool          // emit chmod events
}

func DefaultFSConfig() *FSConfig {
	return &FSConfig{
		Timeout:           300 * time.Millisecond,
		IgnoreSysFiles:    true,
		IgnoreHiddenFiles: true,
		EmitChmod:         false,
	}
}
