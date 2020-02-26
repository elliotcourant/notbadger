package notbadger

import (
	"fmt"
	"github.com/elliotcourant/notbadger/table"
	"github.com/elliotcourant/notbadger/z"
	"github.com/elliotcourant/timber"
	"golang.org/x/net/trace"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// compactionPriority represents a unit of work that needs to be performed by the compactor.
	compactionPriority struct {
		partitionId PartitionId
		level       uint8
		score       float64
		dropPrefix  []byte
	}

	levelsController struct {
		eventLog   trace.EventLog
		partitions map[PartitionId]*partitionLevels
		db         *DB
	}

	partitionLevels struct {
		nextFileId       uint64
		levels           []*levelHandler
		compactionStatus compactionStatus
	}

	levelHandler struct {
		sync.RWMutex

		// For level >= 1, tables are sorted by key ranges, which do not overlap.
		// For level 0, tables are sorted by time.
		// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
		tables    []*table.Table
		totalSize int64

		// The following are initialized once and are constant.
		level        uint8
		strLevel     string
		maxTotalSize int64
		db           *DB
	}
)

func newLevelsController(db *DB, manifest *Manifest) (*levelsController, error) {
	z.AssertTrue(db.options.NumLevelZeroTablesStall > db.options.NumLevelZeroTables)
	s := &levelsController{
		db:         db,
		eventLog:   db.eventLog,
		partitions: map[PartitionId]*partitionLevels{},
	}

	// Setup the initial partition.
	s.setupPartition(0)

	// If the database is in memory this is all we need to do.
	if db.options.InMemory {
		return s, nil
	}

	// Compare the manifest to the directory. If there are partition missing we need to throw an error and if there are
	// extra file that should not exist (that are table partition) they will be removed.
	if err := revertToManifest(db, manifest, getFileIdMap(db.options.Directory)); err != nil {
		return nil, err
	}

	// Some partition may have been deleted, Reload the things.
	var flags uint32 = z.Sync
	if db.options.ReadOnly {
		flags |= z.ReadOnly
	}

	var mutex sync.Mutex
	maxFileIds := map[PartitionId]uint64{}
	tables := map[PartitionId][][]*table.Table{}
	// tables := make([][]*table.Table, db.options.MaxLevels)

	// We found that using 3 goroutines allows disk throughput to be utilized to its max. Disk utilization is the main
	// thing we should focus on, while trying to read the data. That's the one factor that remains constant between HDD
	// and SSD.
	throttle := z.NewThrottle(3)

	start := time.Now()
	var numberOpened int32
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for partitionId, partition := range manifest.Partitions {
		// If this is the first time we have seen a partition then setup the tables and maxFileIds map.
		if _, ok := tables[partitionId]; !ok {
			maxFileIds[partitionId] = 0
			tables[partitionId] = make([][]*table.Table, db.options.MaxLevels)
		}

		for fileId, tableManifest := range partition.Tables {
			fileName := table.NewFilename(uint32(partitionId), fileId, db.options.Directory)

			select {
			case <-tick.C:
				timber.Infof("%d tables out of %d for partition %d opened in %s",
					atomic.LoadInt32(&numberOpened),
					len(partition.Tables),
					partitionId,
					time.Since(start),
				)
			default:
			}

			// If we fail to throttle then we need to close all of our tables that we've opened and exit.
			if err := throttle.Do(); err != nil {
				closeAllTables(tables)
				return nil, err
			}

			// If we find a file
			if fileId > maxFileIds[partitionId] {
				maxFileIds[partitionId] = fileId
			}

			go func(partitionId PartitionId, fileName string, tableManifest TableManifest) {
				var err error
				defer func() {
					throttle.Done(err)
					atomic.AddInt32(&numberOpened, 1)
				}()

				file, e := z.OpenExistingFile(fileName, flags)
				if e != nil {
					err = z.Wrapf(e, "opening file: %q", fileName)
					return
				}

				dataKey, e := db.registry.dataKey(partitionId, tableManifest.KeyID)
				if e != nil {
					err = z.Wrapf(e, "failed to read data key")
					return
				}

				tableOptions := buildTableOptions(db.options)

				// Set compression from the table manifest.
				tableOptions.Compression = tableManifest.Compression
				tableOptions.DataKey = dataKey
				tableOptions.Cache = db.blockCache
				t, e := table.OpenTable(file, tableOptions)
				if e != nil {
					if strings.HasPrefix(e.Error(), "CHECKSUM_MISMATCH:") {
						timber.Errorf(e.Error())
						timber.Errorf("ignoring table %s", file.Name())
						// We don't want to set the error here, we will just skip this table.
					} else {
						err = z.Wrapf(err, "opening table: %q", fileName)
					}
					return
				}

				mutex.Lock()
				tables[partitionId][tableManifest.Level] = append(tables[partitionId][tableManifest.Level], t)
				mutex.Unlock()
			}(partitionId, fileName, tableManifest)
		}
	}

	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, err
	}

	timber.Infof("all %d tables opened in %s", atomic.LoadInt32(&numberOpened), time.Since(start))

	for partitionId, maxFileId := range maxFileIds {
		s.partitions[partitionId].nextFileId = maxFileId + 1
	}

	for partitionId, partition := range tables {
		for i, partitionTables := range partition {
			s.partitions[partitionId].levels[i].initTables(partitionTables)
		}
	}

	// Make sure none of the key ranges overlap when they are not supposed to.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, z.Wrapf(err, "failed to validate levels")
	}

	if err := syncDir(db.options.Directory); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef() because that would
// delete the underlying files.)  We ignore errors, which is OK because tables are read-only.
func closeAllTables(tables map[PartitionId][][]*table.Table) {
	for _, partition := range tables {
		for _, tableSlice := range partition {
			for _, t := range tableSlice {
				_ = t.Close()
			}
		}
	}
}

// revertToManifest checks that all necessary table files exist and removes all table files not referenced by the
// manifest. idMap is a set of table file id's that were read from the directory listing.
func revertToManifest(db *DB, manifest *Manifest, idMap map[PartitionId]map[uint64]struct{}) error {
	// 1. Make sure all of the files in the manifest exist.
	for partitionId, partition := range manifest.Partitions {
		for id := range partition.Tables {
			if _, ok := idMap[partitionId][id]; !ok {
				return fmt.Errorf("file does not exist for table %d", id)
			}
		}
	}

	// 2. Delete any files that shouldn't exist.
	for partitionId, files := range idMap {
		for fileId := range files {
			if _, ok := manifest.Partitions[partitionId]; !ok {
				db.eventLog.Printf("table file %d/%d not referenced in manifest\n", partitionId, fileId)
				fileName := table.NewFilename(uint32(partitionId), fileId, db.options.Directory)
				if err := os.Remove(fileName); err != nil {
					return z.Wrapf(
						err,
						"failed to remove excess table file %d/%d - %s",
						partitionId,
						fileId,
						fileName,
					)
				}
			}
		}
	}

	return nil
}

// close will cleanup all of the levels and partitions within this level controller.
func (l *levelsController) close() error {
	if err := l.cleanupLevels(); err != nil {
		return z.Wrapf(err, "failed to close levels controller")
	}

	return nil
}

func (l *levelsController) setupPartition(partitionId PartitionId) {
	// If the partition is already setup then do nothing.
	if _, ok := l.partitions[partitionId]; ok {
		return
	}

	l.partitions[partitionId] = &partitionLevels{
		levels: make([]*levelHandler, l.db.options.MaxLevels),
		compactionStatus: compactionStatus{
			levels: make([]*levelCompactionStatus, l.db.options.MaxLevels),
		},
	}

	for i := uint8(0); i < l.db.options.MaxLevels; i++ {
		l.partitions[partitionId].levels[i] = newLevelHandler(l.db, i)

		if i == 0 {
			// Do nothing for the first level.
		} else if i == 1 {
			l.partitions[partitionId].levels[i].maxTotalSize = l.db.options.LevelOneSize
		} else {
			l.partitions[partitionId].levels[i].maxTotalSize =
				l.partitions[partitionId].levels[i-1].maxTotalSize * int64(l.db.options.LevelSizeMultiplier)
		}

		l.partitions[partitionId].compactionStatus.levels[i] = new(levelCompactionStatus)
	}
}

func (l *levelsController) validate() error {
	for _, p := range l.partitions {
		if err := p.validate(); err != nil {
			return z.Wrapf(err, "failed to validate partition")
		}
	}

	return nil
}

// cleanupLevels will close all of the partitions and their level handlers within this level controller.
func (l *levelsController) cleanupLevels() error {
	var firstError error
	for _, partition := range l.partitions {
		for _, l := range partition.levels {
			if err := l.close(); err != nil && firstError == nil {
				firstError = err
			}
		}
	}

	return firstError
}

func (l *levelsController) startCompaction(closer *z.Closer) {
	n := l.db.options.NumCompactors
	closer.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		go l.runWorker(closer)
	}
}

func (l *levelsController) runWorker(closer *z.Closer) {
	defer closer.Done()

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	// Wait for a random amount of time, this is to offset the synchronization of the compaction workers.
	case <-randomDelay.C:
	// But if the database gets closed before this finishes then just exit.
	case <-closer.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	ticker := time.NewTimer(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Gather the levels that need compaction.
			priorities := l.pickCompactionLevels()
		}
	}
}

// pickCompactionLevels determines which levels in the database need compaction. This is based on the approach that
// RocksDB takes, and is outlined here: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// This method must use the same exact criteria for guaranteeing compaction's progress that addLevel0Table uses.
func (l *levelsController) pickCompactionLevels() (priorities []compactionPriority) {

}

func (p *partitionLevels) validate() error {
	for _, l := range p.levels {
		if err := l.validate(); err != nil {
			return z.Wrapf(err, "failed to valid level handler")
		}
	}

	return nil
}
