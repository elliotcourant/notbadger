package notbadger

import (
	"github.com/elliotcourant/notbadger/pb"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestManifestRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	deletionsThreshold := 10
	mf, m, err := helpOpenOrCreateManifestFile(dir, false, deletionsThreshold)
	defer func() {
		if mf != nil {
			mf.close()
		}
	}()
	require.NoError(t, err)
	require.Equal(t, 0, m.Creations)
	require.Equal(t, 0, m.Deletions)

	err = mf.addChanges([]pb.ManifestChange{
		newCreateChange(0, 0, 0, 0, 0),
	})
	require.NoError(t, err)

	for i := uint64(0); i < uint64(deletionsThreshold*3); i++ {
		ch := []pb.ManifestChange{
			newCreateChange(0, i+1, 0, 0, 0),
			newDeleteChange(0, i),
		}
		err := mf.addChanges(ch)
		require.NoError(t, err)
	}
	err = mf.close()
	require.NoError(t, err)
	mf = nil
	mf, m, err = helpOpenOrCreateManifestFile(dir, false, deletionsThreshold)
	require.NoError(t, err)
	require.Equal(t, map[uint64]TableManifest{
		uint64(deletionsThreshold * 3): {Level: 0},
	}, m.Partitions[0].Tables)
}
