package utils

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func reflink(dst, src *os.File) error {
	return unix.IoctlFileClone(int(dst.Fd()), int(src.Fd()))
}

func copyData(dst, src *os.File) error {
	info, err := src.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if err = dst.Truncate(size); err != nil {
		return err
	}
	for off := int64(0); off < size; {
		data, err := src.Seek(off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			return nil
		}
		if err != nil {
			return err
		}
		hole, err := src.Seek(data, unix.SEEK_HOLE)
		if err != nil {
			return err
		}
		if _, err = io.Copy(io.NewOffsetWriter(dst, data), io.NewSectionReader(src, data, hole-data)); err != nil {
			return err
		}
		off = hole
	}
	return nil
}
