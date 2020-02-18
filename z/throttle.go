package z

import (
	"sync"
)

type (
	// Throttle allows a limited number of workers to run at a time. It also provides a mechanism to check for errors
	// encountered by workers and wait for them to finish.
	Throttle struct {
		once         sync.Once
		waitGroup    sync.WaitGroup
		channel      chan struct{}
		errorChannel chan error
		finishError  error
	}
)

// NewThrottle creates a new throttle with a max number of workers.
func NewThrottle(max int) *Throttle {
	return &Throttle{
		channel:      make(chan struct{}, max),
		errorChannel: make(chan error, max),
	}
}

// Do should be called by workers before they start working. It blocks if there are already maximum number of workers
// working. If it detects an error from previously Done workers, it would return it.
func (t *Throttle) Do() error {
	for {
		select {
		case t.channel <- struct{}{}:
			t.waitGroup.Add(1)
			return nil
		case err := <-t.errorChannel:
			if err != nil {
				return err
			}
		}
	}
}

// Finish waits until all workers have finished working. It would return any error passed by Done. If Finish is called
// multiple time, it will wait for workers to finish only once(first time). From next calls, it will return same error
// as found on first call.
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.waitGroup.Wait()
		close(t.channel)
		close(t.errorChannel)
		for err := range t.errorChannel {
			if err != nil {
				t.finishError = err
				return
			}
		}
	})

	return t.finishError
}

// Done should be called by workers when they finish working. They can also pass the error status of work done.
func (t *Throttle) Done(err error) {
	if err != nil {
		t.errorChannel <- err
	}

	select {
	case <-t.channel:
	default:
		panic("Throttle Do Done mismatch")
	}

	t.waitGroup.Done()
}