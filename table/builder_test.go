package table

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeader_Encode(t *testing.T) {
	h := header{
		overlap: 4561,
		diff:    11,
	}

	h1, h2 := h.Encode1(), h.Encode2()
	assert.Equal(t, h1, h2)

	a := uint8(h.overlap & 0xff)
	b := uint8((h.overlap >> 8) & 0xff)
	c := uint8(h.diff & 0xff)
	d := uint8((h.diff >> 8) & 0xff)
	e := []byte{a, b, c, d}
	assert.Equal(t, h1, e)

	//x := (b << 8) + (a & 0xff)
	//fmt.Println(x)
	//
	//u := uint32(h.overlap)<<16 | uint32(h.diff)<<8
	//unsignedEncoded := make([]byte, 4)
	//binary.BigEndian.PutUint32(unsignedEncoded, u)
	//
	//assert.Equal(t, headerEncoded, unsignedEncoded)
}

func BenchmarkHeader_Encode1(b *testing.B) {
	h := header{
		overlap: 4561,
		diff:    2314,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for x := 0; x < 10000; x++ {
			_ = h.Encode1()

		}
	}
}

func BenchmarkHeader_Encode2(b *testing.B) {
	h := header{
		overlap: 4561,
		diff:    2314,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for x := 0; x < 10000; x++ {
			_ = h.Encode2()

		}
	}
}
