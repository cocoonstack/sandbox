// Package netfilter locks an egress-lane VM's NIC via nftables so a NIC-backed
// guest cannot bypass the host egress proxy. cocoon's bridge backend keeps the
// tap in the root netns, so the drop rule is applied there in a per-tap table
// and removed again (Unlock) when the sandbox is released — the kernel does not
// reap a netdev table when its device is deleted.
package netfilter
