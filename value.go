package notbadger

import (
	"fmt"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"golang.org/x/net/trace"
	"os"
	"sync"
)

type (
	request struct {
		// Input values from the change set.
		Entries []*Entry

		Pointers []valuePointer
	}

	logFile struct {
		path string

		// This is a lock on the log file. It guards the fd’s value, the file’s existence and the file’s memory map.
		//
		// Use shared ownership when reading/writing the file or memory map, use exclusive ownership to open/close the
		// descriptor, unmap or remove the file.
		lock        sync.RWMutex
		file        *os.File
		fileId      uint32
		fileMap     []byte
		size        uint32
		loadingMode options.FileLoadingMode
		dataKey     *pb.DataKey
		baseIV      []byte
		registry    *KeyRegistry
	}

	// logFileDiscardStats keeps track of the amount of data that could be discarded for a given logfile.
	logFileDiscardStats struct {
		sync.RWMutex

		// TODO (elliotcourant) Name this variable better.
		m                 map[uint32]int64
		flushChannel      chan map[uint32]int64
		closer            *z.Closer
		updatesSinceFlush int
	}

	valueLog struct {
		directoryPath string
		elog          trace.EventLog

		// filesLock guards our view of which files exist, which to be deleted and how many active iterators.
		filesLock        sync.RWMutex
		filesMap         map[uint32]*logFile
		filesToBeDeleted []uint32

		// TODO (elliotcourant) I feel like this could be converted to a wait group.
		// A refcount of iterators -- when this hits zero, we can delete the filesToBeDeleted.
		numActiveIterators int32

		db                *DB
		maxFileId         uint32 // accessed via atomics.
		writableLogOffset uint32 // read by read, written by write. Must access via atomics.
		numEntriesWritten uint32
		options           Options

		garbageChannel      chan struct{}
		logFileDiscardStats *logFileDiscardStats
	}
)

func valueLogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.vlog", dirPath, string(os.PathSeparator), fid)
}
