package notbadger

import "sync"

type (
	oracle struct {
		// referenceCount used to see if there are still references to the oracle that are active.
		// Must be at the top for memory alignment. (Badger issue #311)
		referenceCount int64

		// isManaged is used to keep track of whether or not the transaction timestamps are generated
		// by the database itself, or by the user.
		isManaged bool

		// Used for nextTransactionTimestamp and commits.
		sync.Mutex

		// writeChannelLock is for ensure that transactions go to the write channel in the same order
		// as their commit timestamps.
		writeChannelLock sync.Mutex

		// TODO (elliotcourant) add meaningful comment.
		nextTransactionTimestamp uint64
	}
)
