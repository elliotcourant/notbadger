package table

import (
	"bytes"
	"math"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
)

const (
	headerSize = uint16(unsafe.Sizeof(header{}))
)

type (
	Builder struct {
		// buffer can be tests or hundreds of megabytes for a single file.
		buffer *bytes.Buffer

		baseKey      []byte   // base key for the current block.
		baseOffset   uint32   // Offset for the current block.
		entryOffsets []uint32 // Offsets of entries present in the current block
		tableIndex   pb.TableIndex
		keyHashes    []uint64 // Uses for building the bloom filter.
		options      *Options
	}

	// TODO (elliotcourant) this could probably be represented as a single uint32 that breaks itself into two uint16s.
	header struct {
		overlap uint16 // Overlap with base key.
		diff    uint16 // length of the diff.
	}
)

func NewBuilder(options Options) *Builder {
	return &Builder{
		buffer:     newBuffer(1 << 20),
		tableIndex: pb.TableIndex{},
		keyHashes:  make([]uint64, 0, 1024),
		options:    &options, // TODO (elliotcourant) Un-pointer-ify this if it's not needed
	}
}

// Close closes the table builder. This currently does nothing. Maybe it implements an interface somewhere, the world
// may never know. I'm just porting BadgerDB. TODO (elliotcourant) wtf is this here for?
func (t *Builder) Close() {}

// Empty will return true if nothing has been written to the buffer yet.
func (t *Builder) Empty() bool {
	return t.buffer.Len() == 0
}

// keyDifference returns a suffix of the provided newKey that is different from the table builder's baseKey.
func (t *Builder) keyDifference(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(t.baseKey); i++ {
		if newKey[i] != t.baseKey[i] {
			break
		}
	}

	return newKey[i:]
}

func (t *Builder) addHelper(key []byte, value z.ValueStruct, valuePointerLength uint64) {
	// TODO (elliotcourant) Benchmark farm hash against crc and xxhash.
	t.keyHashes = append(t.keyHashes, farm.Fingerprint64(z.ParseKey(key)))

	var diffKey []byte

	// If there is not a base key then there is nothing to "diff", so we can store the provided key as the base key and
	// set the diffKey to be the provided key as is.
	if len(t.baseKey) == 0 {
		// Make a copy of the key, the build should not keep references. Otherwise the called has to be very careful and
		// will have to make copies of keys every time they add to the builder, which I've been told is even worse.
		t.baseKey = append(t.baseKey[:0], key...)
		diffKey = key
	} else {
		// If there is a base key already then we want to get the difference between that key and this key.
		diffKey = t.keyDifference(key)
	}

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// Store the current entry's offset.
	z.AssertTrue(uint32(t.buffer.Len()) < math.MaxInt32)
	t.entryOffsets = append(t.entryOffsets, uint32(t.buffer.Len())-t.baseOffset)

	// Write the 4 byte (uint16 - uint16) header.
	t.buffer.Write(h.Encode())

	// Followed by the diff key. The length for the diff key is in the last 2 bytes of the header immediately before this
	t.buffer.Write(diffKey)
}

// Encode returns the header in the form of a byte array. A more in depth explanation of this method is that it takes
// the value of the header in memory and through pointer fuckery writes the raw value of the struct in memory to a
// 4 byte array and returns that array. The reason this is done instead of using a binary encoding is that this is
// SIGNIFICANTLY faster.
// See: https://gist.github.com/jarifibrahim/30237927ff3a4b200d4907c97bd93f41
func (h header) Encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// newBuffer is just a simple wrapper function to create a bytes.Buffer of a specific size easily.
func newBuffer(size int) *bytes.Buffer {
	b := new(bytes.Buffer)
	b.Grow(size)
	return b
}
