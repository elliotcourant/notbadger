package notbadger

import (
	"github.com/elliotcourant/notbadger/table"
	"golang.org/x/net/trace"
	"sync"
)

type (
	levelsController struct {
		eventLog trace.EventLog
	}

	partitionLevels struct {
		nextFileId uint64
		levels     []*levelHandler
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
	return nil, nil
}
