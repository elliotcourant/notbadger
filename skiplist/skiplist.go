package skiplist

import (
	"math"
	"math/rand"
	"notbadger/z"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3

	// MaxNodeSize is the memory footprint of a node of maximum height.
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type (
	// SkipList maps keys to values (in memory)
	SkipList struct {
		height     int32 // Current height. 1 <= height <= kMaxHeight. CAS.
		head       *node
		references int32
		arena      *Arena
	}

	// Iterator is an iterator over skiplist object. For new objects, you just need to initialize Iterator.skipList.
	Iterator struct {
		skipList *SkipList
		node     *node
	}

	node struct {
		// Multiple parts of the valueAddress are encoded as a single uint64 so that it
		// can be atomically loaded and stored:
		//   valueAddress offset: uint32 (bits 0-31)
		//   valueAddress size  : uint16 (bits 32-63)
		valueAddress uint64

		// A byte slice is 24 bytes. We are trying to save space here.
		keyOffset uint32 // Immutable. No need to lock to access key.
		keySize   uint16 // Immutable. No need to lock to access key.

		// Height of the tower.
		height uint16

		// Most nodes do not need to use the full height of the tower, since the
		// probability of each successive level decreases exponentially. Because
		// these elements are never accessed, they do not need to be allocated.
		// Therefore, when a node is allocated in the arena, its memory footprint
		// is deliberately truncated to not include unneeded tower elements.
		//
		// All accesses to elements should use CAS operations, with no need to lock.
		tower [maxHeight]uint32
	}
)

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, z.ValueStruct{}, maxHeight)
	return &SkipList{
		height:     1,
		head:       head,
		arena:      arena,
		references: 1,
	}
}

// IncrementReferences increases the count for the number references to this SkipList.
func (s *SkipList) IncrementReferences() {
	atomic.AddInt32(&s.references, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *SkipList) DecrementReferences() {
	newRef := atomic.AddInt32(&s.references, -1)
	if newRef > 0 {
		return
	}

	s.arena.reset()

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil

	// Since the head references the arena's buf, as long as the head is kept around
	// GC can't release the buf.
	s.head = nil
}

func (s *SkipList) getNext(node *node, height int) *node {
	return s.arena.getNode(node.getNextOffset(height))
}

func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *SkipList) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.head {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := z.CompareKeys(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.head {
			return nil, false
		}

		return x, false
	}
}

// Empty returns if the Skiplist is empty.
func (s *SkipList) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions  will NEVER return the
// head nodes.
func (s *SkipList) findLast() *node {
	n := s.head
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier version of the same
// key.
func (s *SkipList) Get(key []byte) z.ValueStruct {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return z.ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !z.SameKey(key, nextKey) {
		return z.ValueStruct{}
	}

	valOffset, valSize := n.getValueAddress()
	vs := s.arena.getVal(valOffset, valSize)
	vs.Version = z.ParseTs(nextKey)
	return vs
}

// Put inserts the key-value pair.
func (s *SkipList) Put(key []byte, value z.ValueStruct) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.

	listHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[listHeight] = s.head
	next[listHeight] = nil
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].setValue(s.arena, value)
			return
		}
	}

	// We do need to create a new node.
	height := randomHeight()
	x := newNode(s.arena, key, value, height)

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				z.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				z.AssertTrue(prev[i] != next[i])
			}
			nextOffset := s.arena.getNodeOffset(next[i])
			x.tower[i] = nextOffset
			if prev[i].casNextOffset(i, nextOffset, s.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				z.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				prev[i].setValue(s.arena, value)
				return
			}
		}
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *SkipList) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := s.getNext(before, level)
		if next == nil {
			return before, next
		}
		nextKey := next.key(s.arena)
		cmp := z.CompareKeys(key, nextKey)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *SkipList) NewIterator() *Iterator {
	s.IncrementReferences()
	return &Iterator{
		skipList: s,
	}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal arena.
func (s *SkipList) MemSize() int64 {
	return s.arena.size()
}

// Close frees the resources held by the iterator
func (s *Iterator) Close() error {
	s.skipList.DecrementReferences()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool {
	return s.node != nil
}

// Key returns the key at the current position.
func (s *Iterator) Key() []byte {
	return s.skipList.arena.getKey(s.node.keyOffset, s.node.keySize)
}

// Value returns value.
func (s *Iterator) Value() z.ValueStruct {
	valOffset, valSize := s.node.getValueAddress()
	return s.skipList.arena.getVal(valOffset, valSize)
}

// Next advances to the next position.
func (s *Iterator) Next() {
	z.AssertTrue(s.Valid())
	s.node = s.skipList.getNext(s.node, 0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	z.AssertTrue(s.Valid())
	s.node, _ = s.skipList.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.node, _ = s.skipList.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *Iterator) SeekForPrev(target []byte) {
	s.node, _ = s.skipList.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.node = s.skipList.getNext(s.skipList.head, 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.node = s.skipList.findLast()
}

func newNode(arena *Arena, key []byte, value z.ValueStruct, height int) *node {
	// The base level is already allocated in the node struct.
	offset := arena.putNode(height)
	node := arena.getNode(offset)
	node.keyOffset = arena.putKey(key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.valueAddress = encodeValueAddress(arena.putVal(value), value.EncodedSize())
	return node
}

func encodeValueAddress(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValueAddress(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func (s *node) getValueAddress() (offset uint32, size uint32) {
	valueAddress := atomic.LoadUint64(&s.valueAddress)
	return decodeValueAddress(valueAddress)
}

func (s *node) key(arena *Arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

func (s *node) setValue(arena *Arena, value z.ValueStruct) {
	valueOffset := arena.putVal(value)
	valueAddress := encodeValueAddress(valueOffset, value.EncodedSize())
	atomic.StoreUint64(&s.valueAddress, valueAddress)
}

func (s *node) getNextOffset(height int) uint32 {
	return atomic.LoadUint32(&s.tower[height])
}

func (s *node) casNextOffset(height int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&s.tower[height], old, val)
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Uint32() <= heightIncrease {
		h++
	}
	return h
}
