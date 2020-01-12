// +build !windows

package notbadger

import (
	"fmt"
	"github.com/elliotcourant/notbadger/z"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	"io/ioutil"
	"os"
	"path/filepath"
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

// acquireDirectoryLock gets a lock on the directory (using flock). If this is not read-only, it
// will also write our pid to dirPath/pidFileName for convenience.
func acquireDirectoryLock(
	directoryPath string,
	processIdFileName string,
	readOnly bool,
) (*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced chdir in the
	// meantime.
	absoluteProcessIdFilePath, err := filepath.Abs(filepath.Join(directoryPath, processIdFileName))
	if err != nil {
		return nil, errors.Wrap(err, "cannot get absolute path for process id lock file")
	}

	// Now that we have the path, try to open the directory.
	dir, err := os.Open(directoryPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open directory: %q", directoryPath)
	}

	options := unix.LOCK_EX | unix.LOCK_NB
	if readOnly {
		options = unix.LOCK_SH | unix.LOCK_NB
	}

	if err = unix.Flock(int(dir.Fd()), options); err != nil {
		_ = dir.Close()
		return nil, errors.Wrapf(
			err,
			"cannot acquire directory lock on: %q another process is using this database",
			directoryPath)
	}

	if !readOnly {
		if err := ioutil.WriteFile(
			absoluteProcessIdFilePath,
			[]byte(fmt.Sprintf("%d\n", os.Getpid())),
			0666,
		); err != nil {
			_ = dir.Close()
			return nil, errors.Wrapf(err,
				"cannot write process id file: %q", absoluteProcessIdFilePath)
		}
	}

	return &directoryLockGuard{
		file:     dir,
		path:     absoluteProcessIdFilePath,
		readOnly: readOnly,
	}, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() (err error) {
	if !guard.readOnly {
		// It's important that we remove the pid file first.
		err = os.Remove(guard.path)
	}

	if closeErr := guard.file.Close(); err == nil {
		err = closeErr
	}

	guard.path = ""
	guard.file = nil

	return err
}

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
