package table

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIdToFileName(t *testing.T) {
	name := IdToFileName(1234, 7574334)
	assert.NotEmpty(t, name)
	assert.Len(t, name, 24+len(TableFileExtension))
}

func TestParseFileId(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		inPartitionId, inFileId := uint32(1234), uint64(24782134)
		name := IdToFileName(inPartitionId, inFileId)
		assert.NotEmpty(t, name)
		assert.Len(t, name, 24+len(TableFileExtension))

		partitionId, fileId, ok := ParseFileId(name)
		assert.True(t, ok)
		assert.Equal(t, inPartitionId, partitionId)
		assert.Equal(t, inFileId, fileId)
	})

	t.Run("no extension", func(t *testing.T) {
		_, _, ok := ParseFileId("dfhjkasfas")
		assert.False(t, ok)
	})

	t.Run("too short", func(t *testing.T) {
		_, _, ok := ParseFileId("0000.sst")
		assert.False(t, ok)
	})

	t.Run("too long", func(t *testing.T) {
		_, _, ok := ParseFileId("00000000000000000000000000.sst")
		assert.False(t, ok)
	})

	t.Run("bad hexadecimal value", func(t *testing.T) {
		_, _, ok := ParseFileId("000004Z200000000017A2536.sst")
		assert.False(t, ok)
	})
}
