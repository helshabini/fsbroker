package fsbroker

import (
	"fmt"
	"os"
	"time"
)

type FSInfo struct {
	Id      uint64
	Path    string
	Size    uint64
	ModTime time.Time
	Mode    uint32
}

func (info *FSInfo) IsDir() bool {
	return os.FileMode(info.Mode).IsDir()
}

func (info *FSInfo) String() string {
	return fmt.Sprintf("Id: %d, Path: %s, Mode: %o",
		info.Id, info.Path, info.Mode)
}
