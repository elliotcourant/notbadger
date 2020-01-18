package pb

import (
	"encoding/binary"
	"github.com/elliotcourant/notbadger/z"
)

type (
	DataKey struct {
		PartitionId uint32
		KeyId       uint64
		Data        []byte
		Iv          []byte
		CreatedAt   int64
	}
)

func (d *DataKey) Marshall(encryptionKey []byte) ([]byte, error) {
	var data []byte
	var err error
	if len(encryptionKey) == 0 {
		data = d.Data
	} else {
		data, err = z.XORBlock(d.Data, encryptionKey, d.Iv)
	}

	dataSize, ivSize := uint32(len(data)), uint32(len(d.Iv))
	buf := make([]byte, 4+8+8+4+dataSize+4+ivSize)
	i := uint32(0)

	binary.BigEndian.PutUint32(buf[i:i+4], d.PartitionId)
	i += 4

	binary.BigEndian.PutUint64(buf[i:i+8], d.KeyId)
	i += 8

	binary.BigEndian.PutUint32(buf[i:i+4], dataSize)
	i += 4

	copy(buf[i:i+dataSize], data)
	i += dataSize

	binary.BigEndian.PutUint32(buf[i:i+4], ivSize)
	i += 4

	copy(buf[i:i+ivSize], d.Iv)
	i += ivSize

	binary.BigEndian.PutUint64(buf[i:i+8], uint64(d.CreatedAt))

	return buf, err
}
