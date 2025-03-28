//go:build linux
// +build linux

package fsbroker

import (
	"path/filepath"
	"strings"
)

// isSystemFile checks if the file is a common Linux system or temporary file.
func isSystemFile(name string) bool {
	base := strings.ToLower(filepath.Base(name))
	switch base {
	case ".bash_history", ".bash_logout", ".bash_profile", ".bashrc", ".profile",
		".login", ".sudo_as_admin_successful", ".xauthority", ".xsession-errors",
		".viminfo", ".cache", ".config", ".local", ".dbus", ".gvfs",
		".recently-used", ".fontconfig", ".iceauthority":
		return true
	}

	// Patterns for temporary GNOME/GTK files and trash directories
	return strings.HasPrefix(base, ".goutputstream-") ||
		strings.HasPrefix(base, ".trash-") ||
		base == "snap" || base == ".flatpak"
}

// isHiddenFile checks if a file is hidden on Unix-like systems.
func isHiddenFile(path string) (bool, error) {
	// On Unix-like platforms (Linux, macOS), hidden files start with a dot
	return strings.HasPrefix(filepath.Base(path), "."), nil
}
