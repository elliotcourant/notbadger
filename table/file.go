package table

import (
	"path"
)

const (
	TableFileExtension = ".sst"
)

func ParseFileId(name string) (partitionId uint32, fileId uint64, ok bool) {
	name = path.Base(name)

}
