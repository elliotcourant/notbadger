package pb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestManifestChange_Marshal_Unmarshal(t *testing.T) {
	change := ManifestChange{
		PartitionId:         12451,
		TableId:             5324,
		Operation:           ManifestChangeCreate,
		Level:               3,
		KeyId:               1858291421,
		EncryptionAlgorithm: EncryptionAlgorithmAES,
		Compression:         0,
	}
	encoded := change.Marshal()

	result := ManifestChange{}
	err := result.Unmarshal(encoded)
	assert.NoError(t, err)
	assert.Equal(t, change, result)
}

func TestManifestChangeSet_Marshal_Unmarshal(t *testing.T) {
	set := ManifestChangeSet{
		Changes: []ManifestChange{
			{
				PartitionId:         12451,
				TableId:             5324,
				Operation:           ManifestChangeCreate,
				Level:               3,
				KeyId:               1858291421,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
			{
				PartitionId:         5325,
				TableId:             4212415,
				Operation:           ManifestChangeDelete,
				Level:               1,
				KeyId:               643264327432,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
		},
	}
	encoded := set.Marshal()

	result := ManifestChangeSet{}
	err := result.Unmarshal(encoded)
	assert.NoError(t, err)
	assert.Equal(t, set, result)
}

// TODO (elliotcourant) Add comparison benchmark for protobuf marshal.
func BenchmarkManifestChange_Marshal(b *testing.B) {
	change := ManifestChange{
		PartitionId:         12451,
		TableId:             5324,
		Operation:           ManifestChangeCreate,
		Level:               3,
		KeyId:               1858291421,
		EncryptionAlgorithm: EncryptionAlgorithmAES,
		Compression:         0,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		change.Marshal()
	}
}

func BenchmarkManifestChange_MarshalEx(b *testing.B) {
	change := ManifestChange{
		PartitionId:         12451,
		TableId:             5324,
		Operation:           ManifestChangeCreate,
		Level:               3,
		KeyId:               1858291421,
		EncryptionAlgorithm: EncryptionAlgorithmAES,
		Compression:         0,
	}

	dst := make([]byte, ManifestChangeSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = change.MarshalEx(dst)
	}
}

// TODO (elliotcourant) Add comparison benchmark for protobuf unmarshal.
func BenchmarkManifestChange_Unmarshal(b *testing.B) {
	change := ManifestChange{
		PartitionId:         12451,
		TableId:             5324,
		Operation:           ManifestChangeCreate,
		Level:               3,
		KeyId:               1858291421,
		EncryptionAlgorithm: EncryptionAlgorithmAES,
		Compression:         0,
	}
	encoded := change.Marshal()

	result := ManifestChange{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = result.Unmarshal(encoded)
	}
}

// TODO (elliotcourant) Add comparison benchmark for protobuf marshal.
func BenchmarkManifestChangeSet_Marshal(b *testing.B) {
	set := ManifestChangeSet{
		Changes: []ManifestChange{
			{
				PartitionId:         12451,
				TableId:             5324,
				Operation:           ManifestChangeCreate,
				Level:               3,
				KeyId:               1858291421,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
			{
				PartitionId:         5325,
				TableId:             4212415,
				Operation:           ManifestChangeDelete,
				Level:               1,
				KeyId:               643264327432,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		set.Marshal()
	}
}

// TODO (elliotcourant) Add comparison benchmark for protobuf unmarshal.
func BenchmarkManifestChangeSet_Unmarshal(b *testing.B) {
	set := ManifestChangeSet{
		Changes: []ManifestChange{
			{
				PartitionId:         12451,
				TableId:             5324,
				Operation:           ManifestChangeCreate,
				Level:               3,
				KeyId:               1858291421,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
			{
				PartitionId:         5325,
				TableId:             4212415,
				Operation:           ManifestChangeDelete,
				Level:               1,
				KeyId:               643264327432,
				EncryptionAlgorithm: EncryptionAlgorithmAES,
				Compression:         0,
			},
		},
	}

	encoded := set.Marshal()

	result := ManifestChangeSet{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = result.Unmarshal(encoded)
	}
}