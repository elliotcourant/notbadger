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

	e := h.Encode()

	assert.Len(t, e, 4)
}

func BenchmarkHeader_Encode1(b *testing.B) {
	h := header{
		overlap: 4561,
		diff:    2314,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Encode()
	}
}
