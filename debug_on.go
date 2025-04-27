//go:build fsbroker_debug
// +build fsbroker_debug

package fsbroker

import (
	"fmt"
	"log/slog"
	"os"
)

func init() {
	fmt.Println("[FSBroker] Debug logging ENABLED (compiled with 'fsbroker_debug' tag)")
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	h := slog.NewTextHandler(os.Stdout, opts)
	slog.SetDefault(slog.New(h))
}

// logDebug is the active logging function when the fsbroker_debug tag is enabled.
func logDebug(msg string, args ...any) {
	slog.Debug(msg, args...)
}
