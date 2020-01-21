package table

import (
	"fmt"
	b "github.com/dgraph-io/ristretto/z"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	intSize = int(unsafe.Sizeof(int(0)))
)

type (
	// TableInterface is apparently useful for testing.
	// TODO (elliotcourant) Add documentation on what this is used for.
	TableInterface interface {
		Smallest() []byte // Head
		Biggest() []byte  // Tail
		DoesNotHave(hash uint64) bool
	}

	Table struct {
		sync.Mutex

		file       *os.File
		tableSize  int
		blockIndex []pb.BlockOffset
		references int32 // Reference counting?
		memoryMap  []byte

		// The following are initialized once and are constant.
		smallest, largest []byte // Smallest and largest keys (with timestamps). TODO Head, tail?
		partitionId       uint32
		fileId            uint64
		bloomFilter       *b.Bloom
		Checksum          []byte // TODO Maybe xxhash this?

		// Stores the total size of key-values stored in this table (including the size on vlog).
		estimatedSize uint64
		IsInMemory    bool
		options       *Options
	}

	block struct {
		offset            int
		data              []byte
		checksum          []byte // TODO (elliotcourant) This might be able to be a static size?
		entriesIndexStart int
		entryOffsets      []uint32
		checksumLength    int // TODO (elliotcourant) Is this really necessary?
	}
)

func OpenTable(file *os.File, opts Options) (*Table, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		// It's OK to ignore fd.Close() errs in this function because we have only read
		// from the file.
		_ = file.Close()
		return nil, z.Wrap(err)
	}

	fileName := fileInfo.Name()
	partitionId, fileId, ok := ParseFileId(fileName)
	if !ok {
		_ = file.Close()
		return nil, errors.Errorf("invalid filename: %s", fileName)
	}

	table := &Table{
		file:        file,
		references:  1, // Caller is given one reference.
		partitionId: partitionId,
		fileId:      fileId,
		IsInMemory:  false,
		options:     &opts,
		tableSize:   int(fileInfo.Size()),
	}

	switch opts.LoadingMode {
	case options.LoadToRAM:
		// Move the cursor to the beginning of the file.
		if _, err := table.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		// Setup the memory map so that we can fit the entire file in memory.
		table.memoryMap = make([]byte, table.tableSize)

		// Read the contents of the file into memory.
		if n, err := table.file.Read(table.memoryMap); err != nil {
			// It's okay to ignore the error here because we have only read from the file.
			_ = table.file.Close()
			return nil, z.Wrapf(err, "failed to load table file into memory")
		} else if n != table.tableSize {
			return nil, errors.Errorf(
				"failed to read all bytes from the file. bytes in file/read: %d/%d",
				table.tableSize,
				n,
			)
		}
	case options.MemoryMap:
		// Use the memoryMap byte array to map the file.
		if table.memoryMap, err = z.Mmap(file, false, int64(table.tableSize)); err != nil {
			_ = table.file.Close()
			return nil, z.Wrapf(err, "unable to map file: %q", fileInfo.Name())
		}
	case options.FileIO:
		// If we are not loading the table into memory in any form then make sure the memory map table gets set to nil
		// so that we don't use it.
		table.memoryMap = nil
	default:
		panic(fmt.Sprintf("invalid loading mode: %v", opts.LoadingMode))
	}

	// TODO (elliotcourant) build init head and tail.

	return nil, nil
}

// CompressionType returns the compression algorithm used for block compression.
func (t *Table) CompressionType() options.CompressionType {
	return t.options.Compression
}

// IncrementReference bumps the reference count (having to do with whether the file should be deleted or not).
func (t *Table) IncrementReference() {
	atomic.AddInt32(&t.references, 1)
}

// DecrementReference subtracts from the reference count, and if the reference count results in 0 then that means there
// is not a single reference left in the database for this table. The file will be deleted.
func (t *Table) DecrementReference() error {
	newReference := atomic.AddInt32(&t.references, -1)
	if newReference == 0 {
		// We can safely delete this file, because for all the current file we always have at least one reference
		// pointing to them.

		// It's necessary to delete Windows files.
		if t.options.LoadingMode == options.MemoryMap {
			if err := z.Munmap(t.memoryMap); err != nil {
				return err
			}
			t.memoryMap = nil
		}

		// file can be nil if the table belongs to L0 and it is opened in memory. See OpenTableInMemory method.
		if t.file == nil {
			return nil
		}

		// Truncate the file.
		if err := t.file.Truncate(0); err != nil {
			return err
		}

		fileName := t.file.Name()

		// Close the file so that we can delete it.
		if err := t.file.Close(); err != nil {
			return err
		}

		if err := os.Remove(fileName); err != nil {
			return err
		}
	}

	return nil
}

// size returns the total size in bytes of the block.
func (b *block) size() int64 {
	return int64(3*intSize /* Size of the offset, entriesIndexStart and checksumLength */ +
		cap(b.data) + cap(b.checksum) + cap(b.entryOffsets)*4)
}
