package z

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"sync"
)

const (
	// This is O_DSYNC (datasync) on platforms that support it -- see file_unix.go
	dataSyncFileFlag = 0x0
)

const (
	// Sync indicates that O_DSYNC should be set on the underlying file,
	// ensuring that data writes do not return until the data is flushed
	// to disk.
	Sync = 1 << iota
	// ReadOnly opens the underlying file on a read-only basis.
	ReadOnly
)

var (
	// CastagnoliCrcTable is a CRC32 polynomial table. This is used for creating checksums for files.
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

type (
	// Closer holds the two things we need to close a goroutine and wait for it to finish: a chan to tell the goroutine
	// to shut down, and a WaitGroup with which to wait for it to finish shutting down.
	Closer struct {
		closed  chan struct{}
		waiting sync.WaitGroup
	}
)

// OpenExistingFile opens an existing file, errors if it doesn't exist.
func OpenExistingFile(fileName string, flags uint32) (*os.File, error) {
	openFlags := os.O_RDWR
	if flags&ReadOnly != 0 {
		openFlags = os.O_RDONLY
	}

	if flags&Sync != 0 {
		openFlags |= dataSyncFileFlag
	}
	return os.OpenFile(fileName, openFlags, 0)
}

// OpenTruncFile opens the file with O_RDWR | O_CREATE | O_TRUNC
func OpenTruncFile(fileName string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if sync {
		flags |= dataSyncFileFlag
	}
	return os.OpenFile(fileName, flags, 0600)
}

// CompareKeys checks the key without timestamp and checks the timestamp if keyNoTs
// is same.
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp.
func CompareKeys(key1, key2 []byte) int {
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// KeyWithTs generates a new key by appending ts to key.
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// ParseKey parses the actual key from the key bytes.
func ParseKey(key []byte) []byte {
	if key == nil {
		return nil
	}

	return key[:len(key)-8]
}

// ParseTs parses the timestamp from the key bytes.
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// SameKey checks for key equality ignoring the version timestamp suffix.
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}

	return bytes.Equal(ParseKey(src), ParseKey(dst))
}