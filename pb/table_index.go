package pb

type (
	TableIndex struct {
		Offsets       []BlockOffset
		BloomFilter   []byte
		EstimatedSize uint64
	}
)
