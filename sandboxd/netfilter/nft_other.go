//go:build !linux

package netfilter

import "errors"

// Lock and Unlock fail off Linux (nftables is Linux-only), so a caller cannot
// mistake a missing lock for an applied one.
func Lock(string) error { return errors.ErrUnsupported }

func Unlock(string) error { return errors.ErrUnsupported }

func EnsureLock(string) error { return errors.ErrUnsupported }

func SweepExcept(keep map[string]bool) error { return nil }
