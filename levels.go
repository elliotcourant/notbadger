package notbadger

import (
	"github.com/elliotcourant/notbadger/table"
	"github.com/elliotcourant/notbadger/z"
	"golang.org/x/net/trace"
	"sync"
)

type (
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
}

func revertToManifest(db *DB, manifest *Manifest, idMap map[uint64]struct{}) error {

}

func (l *levelsController) setupPartition(partitionId PartitionId) {
	// If the partition is already setup then do nothing.
	if _, ok := l.partitions[partitionId]; ok {
		return
	}

	l.partitions[partitionId] = &partitionLevels{
		nextFileId: 0,
		levels:     make([]*levelHandler, l.db.options.MaxLevels),
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
