// +build !windows, !darwin

package z

import (
	"golang.org/x/sys/unix"
	"os"
)

// Mmap uses the mmap system call to memory-map a file. If writable is true, memory protection of the pages is set so
// that they may be written to as well.
func mmap(file *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}

	return unix.Mmap(int(file.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return unix.Munmap(b)
}

// Madvise uses the madvise system call to give advise about the use of memory when using a slice that is memory-mapped
// to a file. Set the readahead flag to false if page references are expected in random order.
func madvise(b []byte, readAhead bool) error {
	flags := unix.MADV_NORMAL
	if !readAhead {
		flags = unix.MADV_RANDOM
	}

	return unix.Madvise(b, flags)
}
