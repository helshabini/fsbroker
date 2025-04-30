package fsbroker

import (
	"testing"
	"time"
)

func TestDefaultFSConfig(t *testing.T) {
	expectedTimeout := 300 * time.Millisecond
	expectedIgnoreSysFiles := true
	expectedIgnoreHiddenFiles := true
	expectedEmitChmod := false

	config := DefaultFSConfig()

	if config == nil {
		t.Fatal("DefaultFSConfig returned nil")
	}

	if config.Timeout != expectedTimeout {
		t.Errorf("DefaultFSConfig.Timeout = %v, want %v", config.Timeout, expectedTimeout)
	}
	if config.IgnoreSysFiles != expectedIgnoreSysFiles {
		t.Errorf("DefaultFSConfig.IgnoreSysFiles = %v, want %v", config.IgnoreSysFiles, expectedIgnoreSysFiles)
	}
	if config.IgnoreHiddenFiles != expectedIgnoreHiddenFiles {
		t.Errorf("DefaultFSConfig.IgnoreHiddenFiles = %v, want %v", config.IgnoreHiddenFiles, expectedIgnoreHiddenFiles)
	}
	if config.EmitChmod != expectedEmitChmod {
		t.Errorf("DefaultFSConfig.EmitChmod = %v, want %v", config.EmitChmod, expectedEmitChmod)
	}
}
