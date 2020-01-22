package table

import (
	"bytes"
	"github.com/dgryski/go-farm"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"unsafe"
)

const (
	headerSize = uint16(unsafe.Sizeof(header{}))
)

type (
	TableBuilder struct {
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

func NewTableBuilder(options Options) *TableBuilder {
	return &TableBuilder{
		buffer:     newBuffer(1 << 20),
		tableIndex: pb.TableIndex{},
		keyHashes:  make([]uint64, 0, 1024),
		options:    &options, // TODO (elliotcourant) Un-pointer-ify this if it's not needed
	}
}

// Close closes the table builder. This currently does nothing. Maybe it implements an interface somewhere, the world
// may never know. I'm just porting BadgerDB. TODO (elliotcourant) wtf is this here for?
func (t *TableBuilder) Close() {}

// Empty will return true if nothing has been written to the buffer yet.
func (t *TableBuilder) Empty() bool {
	return t.buffer.Len() == 0
}

// keyDifference returns a suffix of the provided newKey that is different from the table builder's baseKey.
func (t *TableBuilder) keyDifference(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(t.baseKey); i++ {
		if newKey[i] != t.baseKey[i] {
			break
		}
	}

	return newKey[i:]
}

func (t *TableBuilder) addHelper(key []byte, value z.ValueStruct, valuePointerLength uint64) {
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

	if len(diffKey) == 0 {

	}
}

func (h header) Encode1() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (h header) Encode2() []byte {
	return []byte{
		uint8(h.overlap & 0xff),
		uint8((h.overlap >> 8) & 0xff),
		uint8(h.diff & 0xff),
		uint8((h.diff >> 8) & 0xff),
	}
}

// newBuffer is just a simple wrapper function to create a bytes.Buffer of a specific size easily.
func newBuffer(size int) *bytes.Buffer {
	b := new(bytes.Buffer)
	b.Grow(size)
	return b
}
