//go:build linux

package netfilter

import (
	"fmt"
	"strings"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
)

const tablePrefix = "sandbox_egress_"

// Lock drops a tap's guest egress at its ingress hook; idempotent.
func Lock(tap string) error {
	c, err := nftables.New()
	if err != nil {
		return fmt.Errorf("nft conn: %w", err)
	}
	dropTable(c, tap)
	t := c.AddTable(&nftables.Table{Family: nftables.TableFamilyNetdev, Name: tablePrefix + tap})
	ch := c.AddChain(&nftables.Chain{
		Name:     "ingress",
		Table:    t,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookIngress,
		Priority: nftables.ChainPriorityFilter,
		Policy:   new(nftables.ChainPolicyAccept),
		Device:   tap,
	})
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
	dropTable(c, tap)
	return c.Flush()
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

// dropTable queues the delete after an add, because a delete of a missing table aborts the batch and an abort waits one RCU grace period.
func dropTable(c *nftables.Conn, tap string) {
	t := &nftables.Table{Family: nftables.TableFamilyNetdev, Name: tablePrefix + tap}
	c.AddTable(t)
	c.DelTable(t)
}
