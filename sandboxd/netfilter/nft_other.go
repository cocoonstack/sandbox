//go:build !linux

package netfilter

import "fmt"

// Lock and Unlock fail off Linux (nftables is Linux-only), so a caller cannot
// mistake a missing lock for an applied one.
func Lock(tap string) error { return fmt.Errorf("nftables lockdown requires linux") }

func Unlock(tap string) error { return fmt.Errorf("nftables lockdown requires linux") }

func EnsureLock(tap string) error { return fmt.Errorf("nftables lockdown requires linux") }

func SweepExcept(keep map[string]bool) error { return nil }
