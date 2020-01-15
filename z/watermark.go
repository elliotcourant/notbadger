package z

import "golang.org/x/net/trace"

type (
	WaterMark struct {
		doneUntil   uint64
		lastIndex   uint64
		Name        string
		markChannel chan mark
		eventLog    trace.EventLog
	}

	// mark contains one of more indices, along with a done boolean to indicate the
	// status of the index: begin or done. It also contains waiters, who could be
	// waiting for the watermark to reach >= a certain index.
	mark struct {
		// Either this is an (index, waiter) pair or (index, done) or (indices, done).
		index    uint64
		waiter   chan struct{}
		indicies []uint64

		// Done will be true once the last index is finished.
		done bool
	}
)

func (w *WaterMark) Init(closer *Closer, eventLogging bool) {
	w.markChannel = make(chan mark, 100)
	if eventLogging {
		w.eventLog = trace.NewEventLog("WaterMark", w.Name)
	} else {
		w.eventLog = NoEventLog
	}
	// TODO (elliotcourant) Need to add watermark process.
	return
}
