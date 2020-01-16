package notbadger

import "unsafe"

const (
	valuePointerSize = unsafe.Sizeof(valuePointer{})
)

type (
	// Entry provides Key, Value, UserMeta and ExpiresAt. This struct can be used by the user to set data.
	Entry struct {
		Key       []byte
		Value     []byte
		UserMeta  byte
		ExpiresAt uint64 // time.Unix
		meta      byte

		// Fields maintained internally.
		offset       uint32
		skipValueLog bool
		headerLength int // Length of the header.
	}

	valuePointer struct {
		Fid    uint32
		Len    uint32
		Offset uint32
	}
)

func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}

	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
}

// Encode encodes Pointer into byte buffer.
func (v valuePointer) Encode() []byte {
	b := make([]byte, valuePointerSize)

	// Copy over the content from p to b.
	*(*valuePointer)(unsafe.Pointer(&b[0])) = v

	return b
}
