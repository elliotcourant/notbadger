package notbadger

type (
	Transaction struct {
		readTimestamp   uint64
		commitTimestamp uint64

		update bool                     // update is used to conditionally keep track of reads.
		reads  map[PartitionId][]uint64 // contains fingerprints of keys read.
		writes map[PartitionId][]uint64 // contains fingerprints of keys written.

		pendingWrites map[PartitionId]map[string]*Entry

		db        *DB
		discarded bool

		size              int64
		count             int64
		numberOfIterators int32
	}
)
