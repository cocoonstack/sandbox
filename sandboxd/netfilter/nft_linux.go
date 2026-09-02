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

// Lock drops a tap's guest egress (DHCP excepted) at its ingress hook; idempotent.
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
	// accept only IPv4 broadcast DHCP; egress VMs must not share a broadcast domain.
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

// LockedTaps reports the taps that already carry a table, so a restart re-adopts them.
func LockedTaps() (map[string]bool, error) {
	c, err := nftables.New()
	if err != nil {
		return nil, fmt.Errorf("nft conn: %w", err)
	}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyNetdev)
	if err != nil {
		return nil, fmt.Errorf("list netdev tables: %w", err)
	}
	locked := make(map[string]bool, len(tables))
	for _, t := range tables {
		if tap, ok := strings.CutPrefix(t.Name, tablePrefix); ok {
			locked[tap] = true
		}
	}
	return locked, nil
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

// SweepExcept drops every per-tap egress table whose tap is not in keep; one sandboxd per host.
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
