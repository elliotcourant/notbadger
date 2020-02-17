package notbadger

import (
	"bytes"
	"fmt"
	"github.com/elliotcourant/notbadger/z"
	"sync"
)

var (
	// infiniteRange is a variable representing a key range that is infinite, or includes all keys.
	infiniteRange = keyRange{infinite: true}
)

type (
	// keyRange represents the start and end of groups of keys.
	keyRange struct {
		left, right []byte
		infinite    bool
	}

	compactionStatus struct {
		sync.RWMutex
		levels []*levelCompactionStatus
	}

	levelCompactionStatus struct {
		ranges     []keyRange
		deleteSize int64
	}
)

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, infinite=%v]", r.left, r.right, r.infinite)
}

func (r keyRange) equals(destination keyRange) bool {
	return bytes.Equal(r.left, destination.left) &&
		bytes.Equal(r.right, destination.right) &&
		r.infinite == destination.infinite
}

func (r keyRange) overlapsWith(destination keyRange) bool {
	// If either one of the ranges is infinite then it will overlap.
	// TODO (elliotcourant) This logic was copied from badger, but this seems weird. Double check this.
	if r.infinite || destination.infinite {
		return true
	}

	// If the left is greater than the destinations right, then there is not any overlap.
	if z.CompareKeys(r.left, destination.right) > 0 {
		return false
	}

	// If the right is less than the destination left, then there is not any overlap.
	if z.CompareKeys(r.right, destination.left) < 0 {
		return false
	}

	// Under any other scenarios the key ranges would overlap.
	return true
}
