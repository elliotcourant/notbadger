// +build windows

package z

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmap(file *os.File, write bool, size int64) ([]byte, error) {
	protect := syscall.PAGE_READONLY
	access := syscall.FILE_MAP_READ

	if write {
		protect = syscall.PAGE_READWRITE
		access = syscall.FILE_MAP_WRITE
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// In windows, we cannot mmap a file more than it's actual size.
	// So truncate the file to the size of the mmap.
	if info.Size() < size {
		if err := file.Truncate(size); err != nil {
			return nil, fmt.Errorf("truncate: %s", err)
		}
	}

	// Open a file mapping handle.
	sizeLow := uint32(size >> 32)
	sizeHigh := uint32(size) & 0xffffffff

	handler, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil,
		uint32(protect), sizeLow, sizeHigh, nil)
	if err != nil {
		return nil, os.NewSyscallError("CreateFileMapping", err)
	}

	// Create the memory map.
	addr, err := syscall.MapViewOfFile(handler, uint32(access), 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(handler)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	// Slice memory layout
	// Copied this snippet from golang/sys package
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(size), int(size)}

	// Use unsafe to turn sl into a []byte.
	data := *(*[]byte)(unsafe.Pointer(&sl))

	return data, nil
}

func munmap(b []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
}

func madvise(b []byte, readAhead bool) error {
	// Do Nothing. We don’t care about this setting on Windows
	return nil
}
