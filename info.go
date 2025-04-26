package fsbroker

import (
	"fmt"
	"os"
	"time"
)

type Info struct {
	Id      uint64
	Path    string
	Size    uint64
	ModTime time.Time
	Mode    uint32
}

func (info *Info) IsDir() bool {
	return os.FileMode(info.Mode).IsDir()
}

func (info *Info) String() string {
	return fmt.Sprintf("Id: %d, Path: %s, Mode: %o",
		info.Id, info.Path, info.Mode)
}
