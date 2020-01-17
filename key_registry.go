package notbadger

import (
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type (
	// KeyRegistry used to maintain all the data keys.
	KeyRegistry struct {
		sync.RWMutex
		// Might need to be separated by partition.
		dataKeys    map[PartitionId]map[uint64]*pb.DataKey
		lastCreated int64 // lastCreated is the timestamp(seconds) of the last data key generated.
		nextKeyId   uint64
		file        *os.File
		options     KeyRegistryOptions
	}

	KeyRegistryOptions struct {
		Directory                     string
		ReadOnly                      bool
		EncryptionKey                 []byte
		EncryptionKeyRotationDuration time.Duration
		InMemory                      bool
	}
)

// newKeyRegistry just creates a very basic registry and initializes its variables.
func newKeyRegistry(opts KeyRegistryOptions) *KeyRegistry {
	return &KeyRegistry{
		dataKeys:  map[PartitionId]map[uint64]*pb.DataKey{},
		nextKeyId: 0,
		options:   opts,
	}
}

// OpenKeyRegistry opens key registry if it exists, otherwise it'll create key registry and returns
// key registry.
func OpenKeyRegistry(opts KeyRegistryOptions) (*KeyRegistry, error) {
	// Make sure the encryption key length is actually valid.
	if len(opts.EncryptionKey) > 0 {
		switch len(opts.EncryptionKey) {
		default:
			return nil, z.Wrapf(ErrInvalidEncryptionKey, "during OpenKeyRegistry")
		case 16, 24, 32:
			break
		}
	}

	// If the database is opened in memory only mode then we don't need to write the key registry to
	// the disk.
	if opts.InMemory {
		return newKeyRegistry(opts), nil
	}

	path := filepath.Join(opts.Directory, keyRegistryFileName)
	var flags uint32
	if opts.ReadOnly {
		flags |= z.ReadOnly
	} else {
		flags |= z.Sync
	}

	// Try to open an existing the key registry file.
	_, err := z.OpenExistingFile(path, flags)

	// If the file does not exist then we need to create it.
	if os.IsNotExist(err) {

	}

	return nil, nil
}

// latestDataKey will give you the latest generated dataKey based on the rotation period. If the
// last generated dataKey lifetime exceeds the rotation period. It'll create new dataKey.
func (k *KeyRegistry) latestDataKey() (*pb.DataKey, error) {
	// If there is no encryption key then there is nothing to do here.
	if len(k.options.EncryptionKey) == 0 {
		return nil, nil
	}

	panic("encryption not implemented")

	// TODO (elliotcourant) Implement latestDataKey.

	return nil, nil
}
