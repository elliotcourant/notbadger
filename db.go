package notbadger

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/skiplist"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
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

		manifest   *manifestFile
		blockCache *ristretto.Cache

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

func Open(opts Options) (db *DB, err error) {
	if opts.InMemory && (opts.Directory != "" || opts.ValueDirectory != "") {
		return nil, errors.New("Cannot use badger in Disk-less mode with Directory or ValueDirectory set")
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

	// Compact L0 on close if either it is set or if KeepL0InMemory is set. When keepL0InMemory is set we need to
	// compact L0 on close otherwise we might lose data.
	opts.CompactL0OnClose = opts.CompactL0OnClose || opts.KeepL0InMemory

	if opts.ReadOnly {
		// Can't truncate if the DB is read only.
		opts.Truncate = false
		// Do not perform compaction in read only mode.
		opts.CompactL0OnClose = false
	}

	var directoryLockGuard, valueDirectoryLockGuard *directoryLockGuard

	// Create directories and acquire lock on it only if badger is not running in InMemory mode. We don't have any
	// directories/files in InMemory mode so we don't need to acquire any locks on them.
	if !opts.InMemory {
		if err := createDirs(opts); err != nil {
			return nil, err
		}
		directoryLockGuard, err = acquireDirectoryLock(opts.Directory, lockFileName, opts.ReadOnly)
		if err != nil {
			return nil, err
		}

		// Make sure to cleanup at the end if there is a problem.
		defer func() {
			// At the end of the open function we throw out the local variables if there is a problem. This is done by
			// checking to see if a variable is nil at the end. If it is then we need to dispose of it gracefully.
			if directoryLockGuard != nil {
				_ = directoryLockGuard.release()
			}
		}()

		absoluteDirectoryPath, err := filepath.Abs(opts.Directory)
		if err != nil {
			return nil, err
		}

		absoluteValueDirectoryPath, err := filepath.Abs(opts.ValueDirectory)
		if err != nil {
			return nil, err
		}

		// If the value directory path is not the same as the normal directory path then we need to acquire a directory
		// lock on the value directory as well. We want to do this comparison with the absolute paths to make sure that
		// the paths are actually the same. It's possible to provide a path to the same directory as different strings
		// but by resolving the absolute directory we know the actual path and can compare them.
		if absoluteValueDirectoryPath != absoluteDirectoryPath {
			valueDirectoryLockGuard, err = acquireDirectoryLock(opts.ValueDirectory, lockFileName, opts.ReadOnly)
			if err != nil {
				return nil, err
			}

			// Make sure that if something fails later on we still clean up this directory lock.
			defer func() {
				if valueDirectoryLockGuard != nil {
					_ = valueDirectoryLockGuard.release()
				}
			}()
		}
	}

	// Open/create the manifest file. This will give us the initial state of our entire database.
	manifestFile, _, err := openOrCreateManifestFile(opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	config := ristretto.Config{
		// Use 5% of cache memory for storing counters.
		NumCounters: int64(float64(opts.MaxCacheSize) * 0.05 * 2),
		MaxCost:     int64(float64(opts.MaxCacheSize) * 0.95),
		BufferItems: 64,
		Metrics:     true,
	}
	cache, err := ristretto.NewCache(&config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cache")
	}

	db = &DB{
		directoryLockGuard:      directoryLockGuard,
		valueDirectoryLockGuard: valueDirectoryLockGuard,
		partitions:              nil,
		partitionsReadLock:      sync.RWMutex{},
		partitionsWriteLock:     sync.Mutex{},
		valueLog:                valueLog{},
		valueHead:               valuePointer{},
		writeChannel:            nil,
		manifest:                manifestFile,
		closeOnce:               sync.Once{},
		blockCache:              cache,
	}

	valueDirectoryLockGuard = nil
	directoryLockGuard = nil
	manifestFile = nil

	return db, nil
}

func exists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return true, err
	}
}

func createDirs(opt Options) error {
	for _, path := range []string{opt.Directory, opt.ValueDirectory} {
		dirExists, err := exists(path)
		if err != nil {
			return z.Wrapf(err, "invalid dir: %q", path)
		}

		if !dirExists {
			if opt.ReadOnly {
				return errors.Errorf("cannot find directory %q for read-only open", path)
			}
			// Try to create the directory
			if err = os.Mkdir(path, 0700); err != nil {
				return z.Wrapf(err, "error creating dir: %q", path)
			}
		}
	}

	return nil
}
