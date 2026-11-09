package utils

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

const copyBlockSize = 1 << 20

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
	buf := make([]byte, copyBlockSize)
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
		if err = copyRange(dst, src, buf, data, hole); err != nil {
			return err
		}
		off = hole
	}
	return nil
}

func copyRange(dst, src *os.File, buf []byte, from, to int64) error {
	for pos := from; pos < to; {
		block := buf[:min(int64(len(buf)), to-pos)]
		if _, err := src.ReadAt(block, pos); err != nil {
			return err
		}
		if !IsZero(block) {
			if _, err := dst.WriteAt(block, pos); err != nil {
				return err
			}
		}
		pos += int64(len(block))
	}
	return nil
}
