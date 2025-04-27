//go:build !fsbroker_debug
// +build !fsbroker_debug

package fsbroker

import (
	"log/slog"
	"os"
)

func init() {
	// Default level (Info) when debug tag is not present
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	h := slog.NewTextHandler(os.Stdout, opts)
	slog.SetDefault(slog.New(h))
}

// logDebug is a no-op when the fsbroker_debug tag is *not* enabled.
func logDebug(_ string, _ ...any) {
	// Compiled out in non-debug builds
}
