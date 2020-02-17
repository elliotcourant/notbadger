package notbadger

import (
	"fmt"
)

func newLevelHandler(db *DB, level uint8) *levelHandler {
	return &levelHandler{
		level:    level,
		strLevel: fmt.Sprintf("L%d", level),
		db:       db,
	}
}
