package table

import (
	b "github.com/dgraph-io/ristretto/z"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"os"
	"sync"
)

type (
	Table struct {
		sync.Mutex

		file       *os.File
		tableSize  int
		blockIndex []pb.BlockOffset
		ref        int32 // Reference counting?
		memoryMap  []byte

		// The following are initialized once and are constant.
		smallest, largest []byte // Smallest and largest keys (with timestamps). TODO Head, tail?
		partitionId       uint32
		id                uint64
		bloomFilter       *b.Bloom
		Checksum          []byte // TODO Maybe xxhash this?

		// Stores the total size of key-values stored in this table (including the size on vlog).
		estimatedSize uint64
		IsInMemory    bool
		options       *Options
	}
)

func OpenTable(file *os.File, options Options) (*Table, error) {
	_, err := file.Stat()
	if err != nil {
		// It's OK to ignore fd.Close() errs in this function because we have only read
		// from the file.
		_ = file.Close()
		return nil, z.Wrap(err)
	}

	return nil, nil
}
