package pb

import (
	"encoding/binary"
	"fmt"
)

const (
	// ManifestChangeSize is a static size. This is how many bytes each ManifestChange consumes when written to the disk
	ManifestChangeSize = 0 + // Simply here to align the other items.
		4 + // PartitionId (uint32 - 4 bytes)
		8 + // TableId (uint64 - 8 bytes)
		1 + // Operation (uint8 - 1 byte)
		1 + // Level (uint8 - 1 byte)
		8 + // KeyId (uint64 - 8 bytes)
		1 + // EncryptionAlgorithm (uint8 - 1 byte)
		1 // Compression (uint32 - 4 bytes)
)

type (
	// EncryptionAlgorithm indicates the type of encryption that should be used.
	// TODO (elliotcourant) Provide more insight into how this is used.
	EncryptionAlgorithm uint8

	// ManifestChange_Operation indicates what type of change is being applied to the manifest.
	ManifestChangeOperation uint8

	ManifestChange struct {
		PartitionId uint32

		TableId uint64

		Operation ManifestChangeOperation

		Level uint8

		KeyId uint64

		EncryptionAlgorithm EncryptionAlgorithm

		Compression uint8
	}

	// ManifestChangeSet represents a group of changes that must be applied atomically.
	ManifestChangeSet struct {
		Changes []ManifestChange
	}
)

const (
	// TODO (elliotcourant) Add meaningful comments.
	ManifestChangeCreate ManifestChangeOperation = iota
	ManifestChangeDelete
)

const (
	// TODO (elliotcourant) Add meaningful comments.
	EncryptionAlgorithmAES EncryptionAlgorithm = 0
)

func (mc *ManifestChange) MarshalEx(dst []byte) error {
	// If the provided bytes aren't long enough to decode the manifest change then we can fail early.
	if len(dst) < ManifestChangeSize {
		// TODO (elliotcourant) Add test to cover a bad src.
		return fmt.Errorf(
			"cannot marshal ManifestChange, buffer is too small. Need: %d Got: %d",
			ManifestChangeSize,
			len(dst),
		)
	}

	i := 0

	// First 4 bytes is the PartitionId
	binary.BigEndian.PutUint32(dst[i:i+4], mc.PartitionId)
	i += 4

	binary.BigEndian.PutUint64(dst[i:i+8], mc.TableId)
	i += 8

	dst[i] = uint8(mc.Operation)
	i++

	dst[i] = mc.Level
	i++

	binary.BigEndian.PutUint64(dst[i:i+8], mc.KeyId)
	i += 8

	dst[i] = uint8(mc.EncryptionAlgorithm)
	i++

	dst[i] = mc.Compression

	return nil
}

func (mc *ManifestChange) Marshal() []byte {
	buf := make([]byte, ManifestChangeSize, ManifestChangeSize)
	_ = mc.MarshalEx(buf)
	return buf
}

func (mc *ManifestChange) Unmarshal(src []byte) error {
	// If the provided bytes aren't long enough to decode the manifest change then we can fail early.
	if len(src) < ManifestChangeSize {
		// TODO (elliotcourant) Add test to cover a bad src.
		return fmt.Errorf(
			"cannot unmarshal ManifestChange, buffer is too small. Need: %d Got: %d",
			ManifestChangeSize,
			len(src),
		)
	}
	*mc = ManifestChange{}

	i := 0

	mc.PartitionId = binary.BigEndian.Uint32(src[i : i+4])
	i += 4

	mc.TableId = binary.BigEndian.Uint64(src[i : i+8])
	i += 8

	mc.Operation = ManifestChangeOperation(src[i])
	i++

	mc.Level = src[i]
	i++

	mc.KeyId = binary.BigEndian.Uint64(src[i : i+8])
	i += 8

	mc.EncryptionAlgorithm = EncryptionAlgorithm(src[i])
	i++

	mc.Compression = src[i]
	return nil
}

func (mcs *ManifestChangeSet) Marshal() []byte {
	// A manifest change set requires a 4 byte prefix to indicate the number of changes that are being pushed in this
	// set. This gives us a max of uint32 number of changes per set.
	// TODO (elliotcourant) Find out if this could be reduced to a uint16 or if at all possible a uint8. This would
	//  reduce the size on disk of change sets by a small margin but might pay off in read and write performance.
	buf := make([]byte, 4+(ManifestChangeSize*len(mcs.Changes)))

	// Add the count prefix. Since changes are static in their size we can simply use a single integer to indicate how
	// many records and how to read them.
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(mcs.Changes)))

	for i := 0; i < len(mcs.Changes); i++ {
		// We don't need to worry about an error here. The only error that would be returned from the marshal would be
		// the destination not being large enough. We've already guaranteed that it will be.
		_ = mcs.Changes[i].MarshalEx(buf[4+(i*ManifestChangeSize):])
	}

	return buf
}

func (mcs *ManifestChangeSet) Unmarshal(src []byte) error {
	// We need at least 4 bytes to grab the size of the set. It might be possible for the set to be 0. But we will also
	// validate the size of the src once we know how many items should be present.
	if len(src) < 4 {
		return fmt.Errorf("invalid manifest change set source. must be at least 4 bytes")
	}

	count := binary.BigEndian.Uint32(src[0:4])

	expectedTotalSize := 4 + (ManifestChangeSize * count)

	// Once we know the count we can assert how much space that many changes would actually take up, and thus we can
	// assert whether or not we have enough data in our src to actually read that much.
	if uint32(len(src)) < expectedTotalSize {
		return fmt.Errorf(
			"cannot unmarshal manifest set, source is too short. expected: %d got: %d",
			expectedTotalSize,
			len(src),
		)
	}

	// But if all the sizes meet the minimum then we can parse all of our changes.
	mcs.Changes = make([]ManifestChange, count)

	for i := uint32(0); i < count; i++ {
		// We don't need to handle an error here, the only error that we could receive would be if the src was not large
		// enough. But we've already guaranteed that it will be.
		_ = mcs.Changes[i].Unmarshal(src[4+(i*ManifestChangeSize):])
	}

	return nil
}
