//go:build linux

package netfilter

import (
	"fmt"
	"strings"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

const tablePrefix = "sandbox_egress_"

// Lock drops a tap's guest-initiated egress (DHCP renewal excepted) at its
// ingress hook, forcing all egress through the vsock proxy. One netdev table
// per tap in the root netns; idempotent.
func Lock(tap string) error {
	_ = Unlock(tap) // a prior table for this tap would block a clean re-lock
	c, err := nftables.New()
	if err != nil {
		return fmt.Errorf("nft conn: %w", err)
	}
	t := c.AddTable(&nftables.Table{Family: nftables.TableFamilyNetdev, Name: tablePrefix + tap})
	policy := nftables.ChainPolicyAccept
	ch := c.AddChain(&nftables.Chain{
		Name:     "ingress",
		Table:    t,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookIngress,
		Priority: nftables.ChainPriorityFilter,
		Policy:   &policy,
		Device:   tap,
	})
	// Accept only IPv4 broadcast DHCP (client sport 68 → server dport 67, dst
	// 255.255.255.255): the nfproto guard stops an IPv6 datagram whose header
	// happens to carry 0xffffffff at the IPv4-daddr offset from matching, and
	// any unicast dport-67 tunnel falls through to the drop below. The DHCP
	// payload is not inspected — a crafted broadcast in this shape reaches only
	// the local L2 segment (never routed off-link), so egress-lane VMs must not
	// share a broadcast domain with an untrusted peer.
	c.AddRule(&nftables.Rule{Table: t, Chain: ch, Exprs: []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.NFPROTO_IPV4}},
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.IPPROTO_UDP}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 0, Len: 2},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{0x00, 0x44}}, // sport 68 (DHCP client)
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{0x00, 0x43}}, // dport 67 (DHCP server)
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{0xff, 0xff, 0xff, 0xff}}, // dst broadcast
		&expr.Verdict{Kind: expr.VerdictAccept},
	}})
	c.AddRule(&nftables.Rule{Table: t, Chain: ch, Exprs: []expr.Any{
		&expr.Verdict{Kind: expr.VerdictDrop},
	}})
	if err := c.Flush(); err != nil {
		return fmt.Errorf("apply nft for %s: %w", tap, err)
	}
	return nil
}

// EnsureLock applies Lock only if the tap has no table yet, so a restart can
// re-adopt a live VM's surviving lock without a re-lock window.
func EnsureLock(tap string) error {
	c, err := nftables.New()
	if err != nil {
		return fmt.Errorf("nft conn: %w", err)
	}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyNetdev)
	if err != nil {
		return fmt.Errorf("list netdev tables: %w", err)
	}
	for _, t := range tables {
		if t.Name == tablePrefix+tap {
			return nil
		}
	}
	return Lock(tap)
}

// Unlock removes the tap's table; a no-op when it is already gone.
func Unlock(tap string) error {
	c, err := nftables.New()
	if err != nil {
		return fmt.Errorf("nft conn: %w", err)
	}
	c.DelTable(&nftables.Table{Family: nftables.TableFamilyNetdev, Name: tablePrefix + tap})
	_ = c.Flush() // ENOENT when absent — idempotent
	return nil
}

// SweepExcept drops every per-tap egress table whose tap is not in keep, clearing
// locks orphaned by VMs that went away while the daemon was down. It owns the
// whole sandbox_egress_* namespace in the root netns — one sandboxd per host.
func SweepExcept(keep map[string]bool) error {
	c, err := nftables.New()
	if err != nil {
		return fmt.Errorf("nft conn: %w", err)
	}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyNetdev)
	if err != nil {
		return fmt.Errorf("list netdev tables: %w", err)
	}
	for _, t := range tables {
		if tap, ok := strings.CutPrefix(t.Name, tablePrefix); ok && !keep[tap] {
			c.DelTable(t)
		}
	}
	return c.Flush()
}
