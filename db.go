package notbadger

import (
	"github.com/elliotcourant/timber"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/skiplist"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

var (
	notBadgerPrefix   = []byte("!notbgr!")        // Prefix for internal keys used by badger.
	head              = []byte("!notbgr!head")    // For storing value offset for replay.
	transactionKey    = []byte("!notbgr!txn")     // For indicating end of entries in txn.
	notBadgerMove     = []byte("!notbgr!move")    // For key-value pairs which got moved during GC.
	lfDiscardStatsKey = []byte("!notbgr!discard") // For storing lfDiscardStats
)

type (
	DB struct {
		// eventLog is for debugging and doing traces within NotBadger.
		eventLog trace.EventLog

		// TODO (elliotcourant) add meaningful comment.
		directoryLockGuard *directoryLockGuard

		// valueDirectoryLockGuard will be nil if the primary directory and the value directory are the same.
		valueDirectoryLockGuard *directoryLockGuard

		// partitions represents the groups of in memory tables that will be used for each partition.
		partitions          map[PartitionId]*partitionMemoryTables
		partitionsReadLock  sync.RWMutex
		partitionsWriteLock sync.Mutex

		// levelsController manages the individual tables for each partition.
		levelsController *levelsController

		valueLog valueLog

		// less than or equal to a pointer to the last valueLog value put into any of the partitions active table.
		valueHead valuePointer

		writeChannel chan *request

		manifest   *manifestFile
		blockCache *ristretto.Cache

		// options represents the initial configuration that the database was opened with. This is
		// referenced throughout the lifetime of the database.
		options Options

		oracle   *oracle
		registry *KeyRegistry
		size     *databaseSize
		closers  closers

		// closeOnce is used to make sure that the database can only be closed once.
		closeOnce sync.Once
	}

	// TODO (elliotcourant) Add meaningful comment.
	partitionMemoryTables struct {
		// Guards against changes to this partition's in memory tables. Not individual reads and writes.
		sync.RWMutex

		// active is equivalent to badger's DB.mt. Represents the latest (actively written) in-memory table for each
		// partition.
		active *skiplist.SkipList

		// flushed is equivalent to badger's DB.imm. Add here only AFTER pushing to the flush channel.
		flushed []*skiplist.SkipList
	}

	// TODO (elliotcourant) Add meaningful comment.
	flushTask struct {
		memoryTable  *skiplist.SkipList
		valuePointer valuePointer
		dropPrefix   []byte
	}

	closers struct {
		updateSize            *z.Closer
		compactors            *z.Closer
		memoryTable           *z.Closer // TODO this might need to be split for partitions
		writes                *z.Closer
		valueGarbageCollector *z.Closer
		publish               *z.Closer
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
	manifestFile, manifest, err := openOrCreateManifestFile(opts)
	if err != nil {
		return nil, err
	}

	// If we have a problem in the Open method then we will need to cleanup the manifestFile at
	// the end.
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	eventLog := z.NoEventLog
	if opts.EventLogging {
		eventLog = trace.NewEventLog("NotBadger", "DB")
	}

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
		blockCache:              cache,
		closeOnce:               sync.Once{},
		directoryLockGuard:      directoryLockGuard,
		eventLog:                eventLog,
		manifest:                manifestFile,
		partitions:              make(map[PartitionId]*partitionMemoryTables),
		partitionsReadLock:      sync.RWMutex{},
		partitionsWriteLock:     sync.Mutex{},
		options:                 opts,
		oracle:                  newOracle(opts),
		size:                    &databaseSize{},
		valueDirectoryLockGuard: valueDirectoryLockGuard,
		valueHead:               valuePointer{},
		valueLog:                valueLog{},
		writeChannel:            nil,
	}

	if db.options.InMemory {
		db.options.SyncWrites = false
		db.options.ValueThreshold = maxValueThreshold
	}

	keyRegistryOptions := KeyRegistryOptions{
		Directory:                     opts.Directory,
		ReadOnly:                      opts.ReadOnly,
		EncryptionKey:                 opts.EncryptionKey,
		EncryptionKeyRotationDuration: opts.EncryptionKeyRotationDuration,
		InMemory:                      opts.InMemory,
	}

	if db.registry, err = OpenKeyRegistry(keyRegistryOptions); err != nil {
		return nil, err
	}

	// Calculate the size of the database on the disk.
	db.calculateSize()
	db.closers.updateSize = z.NewCloser(1)
	// updateSize will update the database size variables once every minute
	go db.updateSize(db.closers.updateSize)

	// 0 is the default partition.
	db.partitions[0] = &partitionMemoryTables{
		active:  skiplist.NewSkiplist(arenaSize(db.options)),
		flushed: make([]*skiplist.SkipList, db.options.NumMemoryTables),
	}

	// newLevelsController potentially loads files in the directory.
	if db.levelsController, err = newLevelsController(db, &manifest); err != nil {
		return nil, err
	}

	if !opts.ReadOnly {
		db.closers.compactors = z.NewCloser(1)
		// TODO left off here.
	}

	valueDirectoryLockGuard = nil
	directoryLockGuard = nil
	manifestFile = nil

	return db, nil
}

// handleFlushTask must be run serially.
func (db *DB) handleFlushTask(task flushTask) error {
	// There can be a scenario, when an empty memory table is flushed. For example, when the memory
	// table is empty and after writing the request to the value log, the rotation count exceeds
	// db.LogRotatesToFlush.
	if task.memoryTable.Empty() {
		return nil
	}

	// TODO (elliotcourant) Add Option logging.
	db.eventLog.Printf("storing offset: %+v\n", task.valuePointer)
	value := task.valuePointer.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the commits
	headTimestamp := z.KeyWithTs(head, db.oracle.nextTimestamp())

	task.memoryTable.Put(headTimestamp, z.ValueStruct{
		Value: value,
	})

	// dataKey, err := db.

	return nil
}

func (db *DB) updateSize(lc *z.Closer) {
	defer lc.Done()
	if db.options.InMemory {
		return
	}

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// calculateSize does a file walk, calculates the size of the value log and stores it in the
// z.LSMSize and z.ValueLogSize
func (db *DB) calculateSize() {
	if db.options.InMemory {
		return
	}

	totalSize := func(dir string) (lsmSize, valueLogSize int64) {
		if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			fileExtension := filepath.Ext(path)

			switch fileExtension {
			case tableFileExtension:
				lsmSize += info.Size()
			case valueLogFileExtension:
				valueLogSize += info.Size()
			default:
				timber.Warningf(
					"unknown file extension '%s' for file %s/%s",
					fileExtension,
					dir,
					info.Name(),
				)
			}

			return nil
		}); err != nil {
			db.eventLog.Printf("error while calculating total size of directory: %s", dir)
		}

		return
	}

	lsmSize, valueLogSize := totalSize(db.options.Directory)

	// If valueDir is different from dir, we'd have to do another walk.
	if db.options.ValueDirectory != db.options.Directory {
		_, valueLogSize = totalSize(db.options.ValueDirectory)
	}

	atomic.StoreInt64(&db.size.LSMSize, lsmSize)
	atomic.StoreInt64(&db.size.ValueLogSize, valueLogSize)
}

func arenaSize(options Options) int64 {
	return options.MaxTableSize + options.maxBatchSize + options.maxBatchCount*
		int64(skiplist.MaxNodeSize)
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
