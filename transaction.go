package notbadger

type (
	Transaction struct {
		readTimestamp   uint64
		commitTimestamp uint64

		update bool     // update is used to conditionally keep track of reads.
		reads  []uint64 // contains fingerprints of keys read.
		writes []uint64 // contains fingerprints of keys written.

		pendingWrites map[string]*Entry

		db        *DB
		discarded bool

		size              int64
		count             int64
		numberOfIterators int32
	}
)
