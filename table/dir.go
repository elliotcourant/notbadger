package table

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/elliotcourant/timber"
	"path"
	"path/filepath"
	"strings"
)

const (
	TableFileExtension  = ".sst"
	TableFileNameLength = 24
)

// ParseFileId reads the file name into a partitionId and fileId, if the file name could not be parsed then this method
// will return false.
func ParseFileId(name string) (partitionId uint32, fileId uint64, ok bool) {
	name = path.Base(name)

	// Make sure the provided file has the correct file extension.
	if !strings.HasSuffix(name, TableFileExtension) {
		// The file does not have the write file extension so it's not a valid table file. We should simply return now.
		return
	}

	name = strings.TrimSuffix(name, TableFileExtension)

	// If the file name is not long enough or is too long then the file is not valid and we should return.
	if len(name) != TableFileNameLength {
		return
	}

	// Table file names are hexadecimal, and consist of a 4 byte uint32 and an 8 byte uint64. This means that we need to
	// grab the first 8 characters for the partition and the next 16 characters for the file ID to get the hexadecimal
	// representation.
	var partitionIdSegment, fileIdSegment []byte
	var err error

	// Grab the partitionIdSegment from the first 8 characters.
	if partitionIdSegment, err = hex.DecodeString(name[0:8]); err != nil {
		// If there was something wrong decode the hexadecimal string then we need to return false.
		timber.Warningf("could not decode partitionId for table file %s: %v", name, err)
		return
	}

	if fileIdSegment, err = hex.DecodeString(name[8:24]); err != nil {
		// If there was something wrong decode the hexadecimal string then we need to return false.
		timber.Warningf("could not decode fileId for table file %s: %v", name, err)
		return
	}
	return binary.BigEndian.Uint32(partitionIdSegment), binary.BigEndian.Uint64(fileIdSegment), true
}

func IdToFileName(partitionId uint32, fileId uint64) string {
	return fmt.Sprintf("%08X%016X%s", partitionId, fileId, TableFileExtension)
}

// NewFilename should be named TableFilepath -- it combines the dir with the ID to make a table filepath.
func NewFilename(partitionId uint32, fileId uint64, directory string) string {
	return filepath.Join(directory, IdToFileName(partitionId, fileId))
}