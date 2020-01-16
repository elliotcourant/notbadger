package notbadger

import (
	"sync"

	"github.com/elliotcourant/notbadger/z"
)

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

		// Used to block NewTransaction, so all previous commits are visible to a
		// new read.
		transactionMark *z.WaterMark

		// Either of these is used to determine which version can be permanently discarded
		// during compaction.
		discardTimestamp uint64       // Used by ManagedDB.
		readMark         *z.WaterMark // Used by DB.

		// commits stores a key fingerprint and latest commit counter for it.
		// refCount is used to clear out the commits map to avoid a memory blowup.
		// TODO (elliotcourant) this might need to be a map[uint64]map[uint64]uint64
		//  to account for partitions, as a single key could exist in multiple partitons.
		//  Another option would be to hash the key with the partitionId as an 8 byte
		//  prefix for the byte array.
		commits map[uint64]uint64

		// closer is used to stop watermarks.
		closer *z.Closer
	}
)

func newOracle(opts Options) *oracle {
	orc := &oracle{
		isManaged: opts.managedTransactions,
		commits:   make(map[uint64]uint64),

		readMark:        &z.WaterMark{Name: "notbadger.PendingReads"},
		transactionMark: &z.WaterMark{Name: "notbadger.TransactionTimestamp"},
		closer:          z.NewCloser(2),
	}

	orc.readMark.Init(orc.closer, opts.EventLogging)
	orc.transactionMark.Init(orc.closer, opts.EventLogging)

	return orc
}

func (o *oracle) nextTimestamp() uint64 {
	o.Lock()
	defer o.Unlock()

	// TODO (elliotcourant) Maybe change this to atomic.LoadUint64() ?
	return o.nextTransactionTimestamp
}
