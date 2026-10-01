// Package netfilter locks an egress-lane tap to the proxy with a per-tap nftables table that Unlock removes, since the kernel keeps it after the device is gone.
package netfilter
