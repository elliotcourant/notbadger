package z

import "encoding/binary"

type (
	// ValueStruct represents the value info that can be associated with a key, but also the internal
	// Meta field.
	ValueStruct struct {
		Meta      uint8
		UserMeta  uint8
		ExpiresAt uint64
		Value     []byte

		Version uint64 // This field is not serialized. Only for internal usage.
	}
)

// EncodedSize is the size (in bytes) of the ValueStruct once it has been marshalled.
func (v *ValueStruct) EncodedSize() uint32 {
	return 1 + 1 + 8 + uint32(len(v.Value))
}

// Marshal encodes the ValueStruct into the destination byte array provided. The destination byte array must be at least
// the encoded size of the ValueStruct.
func (v *ValueStruct) Marshal(dst []byte) {
	dst[0] = v.Meta
	dst[1] = v.UserMeta
	binary.BigEndian.PutUint64(dst[2:2+8], v.ExpiresAt)
	copy(dst[10:], v.Value)
}

// Unmarshal decodes the ValueStruct from the source bytes. The source bytes must be at least 10 bytes to not cause an
// invalid index panic.
func (v *ValueStruct) Unmarshal(src []byte) {
	v.Meta = src[0]
	v.UserMeta = src[1]
	v.ExpiresAt = binary.BigEndian.Uint64(src[2 : 2+8])
	v.Value = src[10:]
}
