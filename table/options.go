package table

import (
	"github.com/dgraph-io/ristretto"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/pb"
)

type (
	// Options contains configurable options for Table/TableBuilder.
	Options struct {
		// Options for Opening/Building Table.

		// ChkMode is the checksum verification mode for Table.
		ChkMode options.ChecksumVerificationMode

		// LoadingMode is the mode to be used for loading Table.
		LoadingMode options.FileLoadingMode

		// Options for Table builder.

		// BloomFalsePositive is the false positive probabiltiy of bloom filter.
		BloomFalsePositive float64

		// BlockSize is the size of each block inside SSTable in bytes.
		BlockSize int

		// DataKey is the key used to decrypt the encrypted text.
		DataKey *pb.DataKey

		// Compression indicates the compression algorithm used for block compression.
		Compression options.CompressionType

		Cache *ristretto.Cache

		// ZSTDCompressionLevel is the ZSTD compression level used for compressing blocks.
		ZSTDCompressionLevel int
	}
)
