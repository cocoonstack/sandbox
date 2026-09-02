//go:build !linux

package netfilter

import "errors"

// Lock fails off Linux, so a caller cannot mistake a missing lock for an applied one.
func Lock(string) error { return errors.ErrUnsupported }

func Unlock(string) error { return errors.ErrUnsupported }

func LockedTaps() (map[string]bool, error) { return nil, errors.ErrUnsupported }

func SweepExcept(keep map[string]bool) error { return nil }
