package experiments

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"
)

func BenchmarkEncodingSingle(b *testing.B) {
	v := valuePointerTest{
		Fid:    3049,
		Offset: 353928,
		Len:    2839,
	}
	x := make([]byte, 12)
	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			v.EncodeBinary(x)
		}
		// Avoid any compiler optimization
		zz = x
	})
	b.Run("Slice", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			v.EncodeSlice(x)
		}
		// Avoid any compiler optimization
		zz = x
	})
}

var zz []byte

func BenchmarkEncodingu32Slice(b *testing.B) {
	count := 10000
	u32slice := make([]uint32, count)
	for i := 0; i < count; i++ {
		u32slice[i] = rand.Uint32()
	}
	x := make([]byte, 4*count)
	y := x

	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				binary.BigEndian.PutUint32(y[:4], u32slice[j])
				y = y[:4]
			}
		}
	})
	b.Run("Slice", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			x = u32SliceToBytes(u32slice)
		}
		// Avoid any compiler optimization
		zz = x
	})
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

type valuePointerTest struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

const vpsize = 12

// Encode encodes Pointer into byte buffer.
func (p valuePointerTest) EncodeBinary(b []byte) []byte {
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint32(b[8:12], p.Offset)
	return b[:vpsize]
}

// Encode encodes Pointer into byte buffer.
func (p valuePointerTest) EncodeSlice(b []byte) []byte {
	*(*valuePointerTest)(unsafe.Pointer(&b[0])) = p
	return b[:vpsize]
}
