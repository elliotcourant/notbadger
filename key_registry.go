package notbadger

import (
	"notbadger/pb"
	"os"
	"sync"
	"time"
)

type (
	// KeyRegistry used to maintain all the data keys.
	KeyRegistry struct {
		sync.RWMutex
		dataKeys    map[uint64]*pb.DataKey
		lastCreated int64 //lastCreated is the timestamp(seconds) of the last data key generated.
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
