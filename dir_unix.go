// +build !windows

package notbadger

import (
	"github.com/pkg/errors"
	"notbadger/z"
	"os"
)

type (
	// directoryLockGuard holds a lock on a directory and a pid file inside. The pid file isn't part of the locking
	// mechanism, it's just advisory.
	directoryLockGuard struct {
		// File handle on the directory, which we've flocked.
		file *os.File

		// The absolute path to our pid file.
		path string

		// Was this a shared lock for a read-only database.
		readOnly bool
	}
)

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) {
	return os.Open(path)
}

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = z.FileSync(f)
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}
