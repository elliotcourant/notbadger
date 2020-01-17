package notbadger

import (
	"bytes"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	sanityText = []byte("not badger")
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
		// If the file doesnt exist and we are in read only mode then don't actually write anything
		// to the disk. Just create the registry in memory.
		registry := newKeyRegistry(opts)
		if opts.ReadOnly {
			return registry, nil
		}

		// If its not read only though then we can use this fresh registry to write a clean file to
		// the disk.
	}

	return nil, nil
}

func WriteKeyRegistry(registry *KeyRegistry, opts KeyRegistryOptions) error {
	buf := &bytes.Buffer{}
	iv, err := z.GenerateIV()
	z.Check(err)

	// Encrypt the sanity text if the encryption key is present.
	eSanity := sanityText
	if len(opts.EncryptionKey) > 0 {
		var err error
		eSanity, err = z.XORBlock(eSanity, opts.EncryptionKey, iv)
		if err != nil {
			return z.Wrapf(err, "error while encrypting sanity text in WriteKeyRegistry")
		}
	}

	// Write the IV and the sanity text to the buffer. If there was an encryption key then
	// eSanity will have been encrypted, but without it it will be the plain text.
	z.Check2(buf.Write(iv))
	z.Check2(buf.Write(eSanity))

	return nil
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
