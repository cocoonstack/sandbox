//go:build !linux

package utils

import (
	"errors"
	"io"
	"os"
)

func reflink(_, _ *os.File) error { return errors.ErrUnsupported }

func copyData(dst, src *os.File) error {
	_, err := io.Copy(dst, src)
	return err
}
