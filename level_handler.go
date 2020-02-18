package notbadger

import (
	"encoding/hex"
	"fmt"
	"github.com/elliotcourant/notbadger/table"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"sort"
)

func newLevelHandler(db *DB, level uint8) *levelHandler {
	return &levelHandler{
		level:    level,
		strLevel: fmt.Sprintf("L%d", level),
		db:       db,
	}
}

// initTables replaces s.tables with given tables. This is done during loading.
func (l *levelHandler) initTables(tables []*table.Table) {
	l.Lock()
	defer l.Unlock()

	l.tables = tables

	// Now that we have the tables setup,
	l.totalSize = 0
	for _, t := range tables {
		l.totalSize += t.Size()
	}

	if l.level == 0 {
		// Key range will overlap. Just sort by fileID in ascending order because newer tables are at the end of
		// level 0.
		sort.Slice(l.tables, func(i, j int) bool {
			return l.tables[i].FileId() < l.tables[j].FileId()
		})
	} else {
		// Sort tables by keys.
		sort.Slice(l.tables, func(i, j int) bool {
			return z.CompareKeys(l.tables[i].Smallest(), l.tables[j].Smallest()) < 0
		})
	}
}

func (l *levelHandler) close() error {
	l.RLock()
	defer l.RUnlock()

	var err error
	for _, t := range l.tables {
		if closeErr := t.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return z.Wrapf(err, "failed to close level handler")
}

// Check does some sanity check on one level of data or in-memory index.
func (l *levelHandler) validate() error {
	if l.level == 0 {
		return nil
	}

	l.RLock()
	defer l.RUnlock()
	numTables := len(l.tables)
	for j := 1; j < numTables; j++ {
		if j >= len(l.tables) {
			return fmt.Errorf("level %d, j=%d numberTables=%d", l.level, j, numTables)
		}

		if z.CompareKeys(l.tables[j-1].Largest(), l.tables[j].Smallest()) >= 0 {
			// TODO (elliotcourant) Change this to use fmt.Errorf()
			return errors.Errorf(
				"inter: largest(j-1) \n%s\n vs smallest(j): \n%s\n: level=%d j=%d numTables=%d",
				hex.Dump(l.tables[j-1].Largest()), hex.Dump(l.tables[j].Smallest()),
				l.level, j, numTables)
		}

		if z.CompareKeys(l.tables[j].Smallest(), l.tables[j].Largest()) > 0 {
			// TODO (elliotcourant) Change this to use fmt.Errorf()
			return errors.Errorf(
				"intra: %q vs %q: level=%d j=%d numTables=%d",
				l.tables[j].Smallest(), l.tables[j].Largest(), l.level, j, numTables)
		}
	}
	return nil
}
