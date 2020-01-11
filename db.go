package notbadger

import (
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/skiplist"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"os"
	"sync"
)

type (
	DB struct {
		// TODO (elliotcourant) add meaningful comment.
		directoryLockGuard *directoryLockGuard

		// valueDirectoryLockGuard will be nil if the primary directory and the value directory are the same.
		valueDirectoryLockGuard *directoryLockGuard

		// partitions represents the groups of in memory tables that will be used for each partition.
		partitions          map[PartitionId]*partitionMemoryTables
		partitionsReadLock  sync.RWMutex
		partitionsWriteLock sync.Mutex

		valueLog valueLog

		// less than or equal to a pointer to the last valueLog value put into any of the partitions active table.
		valueHead valuePointer

		writeChannel chan *request

		// closeOnce is used to make sure that the database can only be closed once.
		closeOnce sync.Once
	}

	partitionMemoryTables struct {
		// Guards against changes to this partition's in memory tables. Not individual reads and writes.
		sync.RWMutex

		// active is equivalent to badger's DB.mt. Represents the latest (actively written) in-memory table for each
		// partition.
		active *skiplist.SkipList

		// flushed is equivalent to badger's DB.imm. Add here only AFTER pushing to the flush channel.
		flushed []*skiplist.SkipList
	}
)

func Open(opts Options) (*DB, error) {
	if opts.InMemory && (opts.Dir != "" || opts.ValueDir != "") {
		return nil, errors.New("Cannot use badger in Disk-less mode with Dir or ValueDir set")
	}

	opts.maxBatchSize = (15 * opts.MaxTableSize) / 100
	opts.maxBatchCount = opts.maxBatchSize / int64(skiplist.MaxNodeSize)

	// We are limiting opt.ValueThreshold to maxValueThreshold for now.
	if opts.ValueThreshold > maxValueThreshold {
		return nil, errors.Errorf(
			"Invalid ValueThreshold, must be less or equal to %d",
			maxValueThreshold,
		)
	}

	if !(opts.ValueLogFileSize <= 2<<30 && opts.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}

	if !(opts.ValueLogLoadingMode == options.FileIO || opts.ValueLogLoadingMode == options.MemoryMap) {
		return nil, ErrInvalidLoadingMode
	}

	// Compact L0 on close if either it is set or if KeepL0InMemory is set. When
	// keepL0InMemory is set we need to compact L0 on close otherwise we might lose data.
	opts.CompactL0OnClose = opts.CompactL0OnClose || opts.KeepL0InMemory

	if opts.ReadOnly {
		// Can't truncate if the DB is read only.
		opts.Truncate = false
		// Do not perform compaction in read only mode.
		opts.CompactL0OnClose = false
	}

	var dirLockGuard, valueDirLockGuard *directoryLockGuard

	// Create directories and acquire lock on it only if badger is not running in InMemory mode.
	// We don't have any directories/files in InMemory mode so we don't need to acquire
	// any locks on them.
	if !opts.InMemory {
		if err := createDirs(opts); err != nil {
			return nil, err
		}

	}
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func createDirs(opt Options) error {
	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return z.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return errors.Errorf("Cannot find directory %q for read-only open", path)
			}
			// Try to create the directory
			err = os.Mkdir(path, 0700)
			if err != nil {
				return z.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}

	return nil
}
