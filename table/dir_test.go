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
