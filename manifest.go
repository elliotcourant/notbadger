package notbadger

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/elliotcourant/notbadger/options"
	"github.com/elliotcourant/notbadger/pb"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"github.com/rotisserie/eris"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// ManifestFilename is the filename for the manifest file.
	ManifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 10000
	manifestDeletionsRatio            = 10

	// manifestVersion is included in the manifest file to indicate the version of the encoding and format that the
	// database is using to create it's manifest files.
	manifestVersion = 0x01092017
)

var (
	// magicalText is used to prefix the manifest file. It is used to verify that the file was created by the database
	// and not by something else.
	magicalText = [4]byte{'!', 'B', 'g', 'r'}
)

var (
	// errBadMagic is returned when a manifest file is missing a 4 byte prefix that is used as a signature of the
	// database.
	errBadMagic = eris.New("manifest has bad magic")
)

type (
	// Manifest represents the contents of the MANIFEST file in a Badger store.
	//
	// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're at.
	//
	// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically, and contains a
	// sequence of ManifestChange's (file creations/deletions) which we use to reconstruct the manifest at startup.
	Manifest struct {
		Partitions map[PartitionId]*partitionManifest

		// Contains total number of creation and deletion changes in the manifest -- used to determine whether or not it
		// would be useful to rewrite the manifest.
		Creations   int
		Deletions   int
		TotalTables int
	}

	// TableManifest contains information about a specific table in the LSM tree.
	TableManifest struct {
		Level       uint8
		KeyId       uint64
		Compression options.CompressionType
	}

	// levelManifest contains information about LSM tree levels in the MANIFEST file.
	levelManifest struct {
		Tables map[uint64]struct{}
	}

	// partitionManifest wraps all of the information for a specific partition and its levels and tables.
	partitionManifest struct {
		Levels []levelManifest
		Tables map[uint64]TableManifest
	}

	// TODO (elliotcourant) Add meaningful comment.
	manifestFile struct {
		file *os.File

		directory string

		// TODO (elliotcourant) Add unit tests.
		// We make this configurable so that unit tests can hit rewrite() code quickly.
		deletionsRewriteThreshold int

		// Guards appends, which includes access to the manifest field.
		appendLock sync.Mutex

		// Used to track the current state of the manifest, used when rewriting.
		manifest Manifest

		// Used to indicate whether or not the database was opened in InMemory mode.
		inMemory bool
	}

	// TODO (elliotcourant) Add meaningful comment.
	countingReader struct {
		wrapped *bufio.Reader
		count   int64
	}
)

// asChanges returns a sequence of changes that could be used to recreate the manifest in its present state.
func (m *Manifest) asChanges() []pb.ManifestChange {
	changes := make([]pb.ManifestChange, 0, m.TotalTables)

	for partitionId, partition := range m.Partitions {
		for tableId, tableManifest := range partition.Tables {
			changes = append(changes, newCreateChange(
				partitionId,
				tableId,
				tableManifest.Level,
				tableManifest.KeyId,
				tableManifest.Compression,
			))
		}
	}

	return changes
}

// TODO (elliotcourant) verify whether or not this is even necessary?
func (m *Manifest) clone() Manifest {
	changeSet := pb.ManifestChangeSet{
		Changes: m.asChanges(),
	}
	ret := createManifest()
	z.Check(applyChangeSet(&ret, changeSet))
	return ret
}

// addChanges writes a batch of changes, atomically, to the file.  By "atomically" that means when we replay the
// MANIFEST file, we'll either replay all the changes or none of them.
// (The truth of this depends on the filesystem -- some might append garbage data if a system crash happens at the wrong
// time.)
func (mf *manifestFile) addChanges(manifestChanges []pb.ManifestChange) error {
	// If we are keeping the manifest in memory then there is no need to write any of these changes. This manages the
	// disk itself so there is nothing to do here.
	if mf.inMemory {
		return nil
	}

	changes := pb.ManifestChangeSet{Changes: manifestChanges}
	buf := changes.Marshal()

	mf.appendLock.Lock()
	defer mf.appendLock.Unlock()
	if err := applyChangeSet(&mf.manifest, changes); err != nil {
		return err
	}

	// Rewrite the manifest if it'd shrunk by 1/10 and it's big enough to matter.
	if mf.manifest.Deletions > mf.deletionsRewriteThreshold &&
		mf.manifest.Deletions > manifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		// TODO (elliotcourant) Maybe the lenCrc buf could be broken into its own method?
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, z.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.file.Write(buf); err != nil {
			// TODO (elliotcourant) If an error happens while closing the file maybe the write error should get wrapped so
			//  that the close error is also included.
			return err
		}
	}

	return z.FileSync(mf.file)
}

// rewrite completely rebuilds the file, appendLock must be held to call this method.
func (mf *manifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.file.Close(); err != nil {
		return err
	}

	file, netCreations, err := helpRewrite(mf.directory, &mf.manifest)
	if err != nil {
		return err
	}

	mf.file = file
	mf.manifest.Creations = netCreations
	mf.manifest.Deletions = 0

	return nil
}

func (mf *manifestFile) close() error {
	if mf.inMemory {
		return nil
	}
	return mf.file.Close()
}

// TODO (elliotcourant) Add meaningful comment.
func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

// TODO (elliotcourant) Add meaningful comment.
func (r *countingReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, manifestRewriteFilename)

	// We don't need to enable sync here because we will explicitly be calling the sync method.
	file, err := z.OpenTruncFile(rewritePath, false)
	if err != nil {
		return nil, 0, err
	}

	// Create the first 8 bytes, this includes a special prefix to verify the file was created using this particular
	// version of the database.
	buf := make([]byte, 8)
	copy(buf[0:4], magicalText[:])
	binary.BigEndian.PutUint32(buf[4:8], manifestVersion)

	// Because we are breaking tables into partitions I'm using the totalTables variable to keep track of the total
	// current active tables. In Badger this is done by simply doing a len() on the map of tables.
	netCreations := m.TotalTables
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf := set.Marshal()

	// Build the size and checksum segment. This is 8 bytes and starts with the size of the the change buffer and ends
	// with a checksum of the change set.
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, z.CastagnoliCrcTable))

	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)

	// Write the data to the file.
	if _, err := file.Write(buf); err != nil {
		// TODO (elliotcourant) If an error happens while closing the file maybe the write error should get wrapped so
		//  that the close error is also included.
		_ = file.Close()
		return nil, 0, err
	}

	// Sync the changes to the disk.
	if err := z.FileSync(file); err != nil {
		// TODO (elliotcourant) If an error happens while closing the file maybe the sync error should get wrapped so
		//  that the close error is also included.
		_ = file.Close()
		return nil, 0, err
	}

	// TODO (elliotcourant) maybe lift renaming of files into it's own function. This way we could avoid closing a file
	//  if it is not necessary since we only need to do it on windows.
	// In windows the files should be closed before doing a rename.
	if err = file.Close(); err != nil {
		return nil, 0, err
	}

	manifestPath := filepath.Join(dir, ManifestFilename)

	// Rename the rewritten file to be the normal manifest file name.
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}

	file, err = z.OpenExistingFile(manifestPath, 0)
	if err != nil {
		return nil, 0, err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		// TODO (elliotcourant) If an error happens while closing the file maybe the seek error should get wrapped so
		//  that the close error is also included.
		_ = file.Close()
		return nil, 0, err
	}

	if err := syncDir(dir); err != nil {
		// TODO (elliotcourant) If an error happens while closing the file maybe the sync dir error should get wrapped
		//  so that the close error is also included.
		_ = file.Close()
		return nil, 0, err
	}

	return file, netCreations, nil
}

func applyManifestChange(build *Manifest, change pb.ManifestChange) error {
	// Because we are breaking things into partitions we need to have an extra check here to see if the partition
	// exists yet. If it does not then create it.
	partition, ok := build.Partitions[PartitionId(change.PartitionId)]
	if !ok {
		partition = &partitionManifest{
			Levels: make([]levelManifest, 0),
			Tables: map[uint64]TableManifest{},
		}
		build.Partitions[PartitionId(change.PartitionId)] = partition
	}

	switch change.Operation {
	case pb.ManifestChangeCreate:
		// A tableId can only appear once on a create change though. So if we already have a table for this specific
		// partition then there is something wrong.
		if _, ok := partition.Tables[change.TableId]; ok {
			return fmt.Errorf(
				"MANIFEST invalid, table %d already exists for partition %d",
				change.TableId,
				change.PartitionId,
			)
		}

		// We know that the table does not exist yet so we can now actually create it.
		partition.Tables[change.TableId] = TableManifest{
			Level:       change.Level,
			KeyId:       change.KeyId,
			Compression: options.CompressionType(change.Compression),
		}

		// If we are at a higher level then update the level array on the partition to match the new number of levels.
		for len(partition.Levels) <= int(change.Level) {
			partition.Levels = append(partition.Levels, levelManifest{
				Tables: make(map[uint64]struct{}),
			})
		}

		// Mark the level and the table on the partition.
		partition.Levels[change.Level].Tables[change.TableId] = struct{}{}

		build.Creations++
		build.TotalTables++
	case pb.ManifestChangeDelete:
		tableManifest, ok := partition.Tables[change.TableId]

		// If the table we are trying to remove does not exist then there is a problem and we need to stop here.
		if !ok {
			return fmt.Errorf(
				"MANIFEST removes non-existing table %d for partition %d",
				change.TableId,
				change.PartitionId,
			)
		}

		// Remove the table records.
		delete(partition.Levels[tableManifest.Level].Tables, change.TableId)
		delete(partition.Tables, change.TableId)

		build.Deletions++
		build.TotalTables--
	default:
		// TODO (elliotcourant) make this prebuilt error variable.
		return fmt.Errorf("MANIFEST file has invalid manifestChange operation")
	}

	return nil
}

func ReplayManifestFile(file *os.File) (Manifest, int64, error) {
	r := countingReader{
		wrapped: bufio.NewReader(file),
	}

	var magicalBuf [8]byte
	if _, err := io.ReadFull(&r, magicalBuf[:]); err != nil {
		return Manifest{}, 0, eris.Wrapf(errBadMagic, "could not read: %v", err)
	} else if !bytes.Equal(magicalBuf[0:4], magicalText[:]) {
		return Manifest{}, 0, eris.Wrap(errBadMagic, "missing magic prefix")
	}

	version := binary.BigEndian.Uint32(magicalBuf[4:8])

	if version != manifestVersion {
		// TODO (elliotcourant) Add a real error here.
		panic("bad version")
	}

	stat, err := file.Stat()
	if err != nil {
		// TODO (elliotcourant) Wrap this error with a message about what we were trying to do.
		return Manifest{}, 0, err
	}
	fileSize := uint32(stat.Size())

	build := createManifest()
	var offset int64
	for {
		offset = r.count
		// TODO (elliotcourant) break this into its own function.
		var lenCrcBuf [8]byte
		if _, err := io.ReadFull(&r, lenCrcBuf[:]); err != nil {
			// If we hit either of these then we've reached the end of the file. There is either no more data to be read
			// or the last entry was cut off and we cannot read it anyway.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			// If it wasn't an EOF error though then there was an actual problem with the reader that we should return.
			return Manifest{}, 0, eris.Wrap(err, "failed to replay manifest file")
		}

		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])

		// Sanity check to make sure we don't over-allocate memory.
		if length > fileSize {
			return Manifest{}, 0, eris.Wrapf(
				eris.New("buffer length for change set greater than file size, manifest might be corrupted"),
				"buffer length: %d file size: %d",
				length,
				fileSize,
			)
		}

		buf := make([]byte, length)
		// TODO (elliotcourant) add comments here.
		if _, err := io.ReadFull(&r, buf); err != nil {
			// If we hit either of these then we've reached the end of the file. There is either no more data to be read
			// or the last entry was cut off and we cannot read it anyway.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			// If it wasn't an EOF error though then there was an actual problem with the reader that we should return.
			return Manifest{}, 0, eris.Wrap(err, "failed to replay manifest file")
		}

		if crc32.Checksum(buf, z.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			// TODO (elliotcourant) real error here.
			panic("bad checksum")
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			// TODO (elliotcourant) real error here.
			panic(err)
		}

		if err := applyChangeSet(&build, changeSet); err != nil {
			return Manifest{}, 0, eris.Wrap(err, "failed to apply change set from manifest file")
		}
	}

	return build, offset, nil
}

// openOrCreateManifestFile opens a database manifest file if it exists, or creates one if doesnt exists.
func openOrCreateManifestFile(options Options) (*manifestFile, Manifest, error) {
	if options.InMemory {
		return &manifestFile{inMemory: true}, Manifest{}, nil
	}

	return helpOpenOrCreateManifestFile(options.Dir, options.ReadOnly, manifestDeletionsRewriteThreshold)
}

func helpOpenOrCreateManifestFile(directory string, readOnly bool, deletionsThreshold int) (
	*manifestFile,
	Manifest,
	error,
) {
	path := filepath.Join(directory, ManifestFilename)
	var flags uint32
	if readOnly {
		flags |= z.ReadOnly
	}

	file, err := z.OpenExistingFile(path, flags)
	// TODO (elliotcourant) add meaningful comment.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, Manifest{}, errors.Wrap(err, "failed to open existing manifest file")
		}

		if readOnly {
			return nil, Manifest{}, errors.New("no manifest found, required for read-only db")
		}

		m := createManifest()
		file, netCreations, err := helpRewrite(directory, &m)
		if err != nil {
			return nil, Manifest{}, errors.Wrap(err, "failed to write new manifest file")
		}

		z.AssertTrue(netCreations == 0)

		mf := &manifestFile{
			file:                      file,
			directory:                 directory,
			deletionsRewriteThreshold: deletionsThreshold,
			manifest:                  m.clone(),
			inMemory:                  false,
		}

		return mf, m, nil
	}

	manifest, truncOffset, err := ReplayManifestFile(file)
	if err != nil {
		_ = file.Close()
		return nil, Manifest{}, err
	}

	if !readOnly {
		// Truncate the file so we don't have a half-written entry at the end.
		if err := file.Truncate(truncOffset); err != nil {
			_ = file.Close()
			return nil, Manifest{}, err
		}
	}

	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, Manifest{}, err
	}

	mf := &manifestFile{
		file:                      file,
		directory:                 directory,
		deletionsRewriteThreshold: deletionsThreshold,
		manifest:                  manifest.clone(),
		inMemory:                  false,
	}

	return mf, manifest, nil
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *Manifest, changeSet pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		// TODO (elliotcourant) If one of the changes in the change set is invalid, it is possible for other changes
		//  in the set to get applied anyway. Or at least be applied to the memory. Find some way to test and make sure
		//  that it really isn't atomic. And if it is not find a way to make it atomic.
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}

	return nil
}

func createManifest() Manifest {
	return Manifest{
		Partitions:  map[PartitionId]*partitionManifest{},
		Creations:   0,
		Deletions:   0,
		TotalTables: 0,
	}
}

func newCreateChange(
	partitionId PartitionId,
	tableId uint64,
	level uint8,
	keyId uint64,
	compression options.CompressionType,
) pb.ManifestChange {
	return pb.ManifestChange{
		PartitionId:         uint32(partitionId),
		TableId:             tableId,
		Operation:           pb.ManifestChangeCreate,
		Level:               level,
		KeyId:               keyId,
		EncryptionAlgorithm: pb.EncryptionAlgorithmAES,
		Compression:         uint8(compression),
	}
}

func newDeleteChange(
	partitionId PartitionId,
	tableId uint64,
) pb.ManifestChange {
	return pb.ManifestChange{
		PartitionId: uint32(partitionId),
		TableId:     tableId,
		Operation:   pb.ManifestChangeDelete,
	}
}
