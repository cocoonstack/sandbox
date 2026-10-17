package utils

import (
	"fmt"
	"os"
)

// CloneFile writes a new file dst that holds src's bytes, sharing extents where the filesystem can and copying only data ranges where it cannot.
func CloneFile(src, dst string) (err error) {
	in, err := os.Open(src) //nolint:gosec // caller-owned paths
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // caller-owned paths
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(dst)
		}
	}()
	if err = reflink(out, in); err != nil {
		if err = copyData(out, in); err != nil {
			_ = out.Close()
			return fmt.Errorf("copy %s: %w", src, err)
		}
	}
	return out.Close()
}
