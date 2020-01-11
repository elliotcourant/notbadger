package options

// FileLoadingMode specifies how data in LSM table files and value log files should
// be loaded.
type FileLoadingMode int

const (
	// FileIO indicates that files must be loaded using standard I/O
	FileIO FileLoadingMode = iota
	// LoadToRAM indicates that file must be loaded into RAM
	LoadToRAM
	// MemoryMap indicates that that the file must be memory-mapped
	MemoryMap
)

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
type ChecksumVerificationMode int

const (
	// NoVerification indicates DB should not verify checksum for SSTable blocks.
	NoVerification ChecksumVerificationMode = iota
	// OnTableRead indicates checksum should be verified while opening SSTtable.
	OnTableRead
	// OnBlockRead indicates checksum should be verified on every SSTable block read.
	OnBlockRead
	// OnTableAndBlockRead indicates checksum should be verified
	// on SSTable opening and on every block read.
	OnTableAndBlockRead
)

// CompressionType specifies how a block should be compressed.
type CompressionType uint8

const (
	// None mode indicates that a block is not compressed.
	None CompressionType = iota
	// Snappy mode indicates that a block is compressed using Snappy algorithm.
	Snappy
	// ZSTD mode indicates that a block is compressed using ZSTD algorithm.
	ZSTD
)
