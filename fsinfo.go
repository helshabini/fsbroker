package fsbroker

import (
	"fmt"
	"os"
	"strings"
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

func (info *FSInfo) Clone() *FSInfo {
	return &FSInfo{
		Id: info.Id,
		Path: strings.Clone(info.Path),
		Size: info.Size,
		ModTime: info.ModTime,
		Mode: info.Mode,
	}
}
