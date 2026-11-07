package pg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
)

func TestAssignIsUniqueAcrossNodesAndIdempotentPerClaim(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	nodes := []*Store{open(t, dsn, "c1", time.Hour), open(t, dsn, "c1", time.Hour)}
	var mu sync.Mutex
	got := map[netip.Addr]string{}
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Go(func() {
			id := fmt.Sprintf("sb_%d", i)
			addr, err := nodes[i%2].Assign(t.Context(), id)
			if err != nil {
				t.Errorf("assign %s: %v", id, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if other, dup := got[addr]; dup {
				t.Errorf("%s and %s share %s", id, other, addr)
			}
			got[addr] = id
		})
	}
	wg.Wait()
	for addr, id := range got {
		if again, err := nodes[1].Assign(t.Context(), id); err != nil || again != addr {
			t.Errorf("a second assign of %s = %s %v, want %s", id, again, err, addr)
		}
	}
}

func TestReleasedAddressWaitsOutTheReuseWindow(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	ctx := t.Context()
	addr, err := s.Assign(ctx, "sb_a")
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	if err = s.Release(ctx, "sb_b", addr); err != nil {
		t.Fatalf("release by another claim: %v", err)
	}
	rewind(t, s, addr)
	if b, assignErr := s.Assign(ctx, "sb_b"); assignErr != nil || b == addr {
		t.Fatalf("assign over a held address = %s %v, want another address", b, assignErr)
	}
	if err = s.Release(ctx, "sb_a", addr); err != nil {
		t.Fatalf("release: %v", err)
	}
	rewind(t, s, addr)
	if c, assignErr := s.Assign(ctx, "sb_c"); assignErr != nil || c == addr {
		t.Fatalf("assign inside the reuse window = %s %v, want another address", c, assignErr)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET released_at = now() - interval '2 hours' WHERE address = $1`, addr.String()); err != nil {
		t.Fatalf("age the release: %v", err)
	}
	rewind(t, s, addr)
	if d, assignErr := s.Assign(ctx, "sb_d"); assignErr != nil || d != addr {
		t.Errorf("assign past the reuse window = %s %v, want %s", d, assignErr, addr)
	}
}

func TestAssignGivesUpOnARunOfHeldAddresses(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	ctx := t.Context()
	first, err := s.Assign(ctx, "sb_0")
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	for i := 1; i < assignTries; i++ {
		if _, err = s.Assign(ctx, fmt.Sprintf("sb_%d", i)); err != nil {
			t.Fatalf("assign %d: %v", i, err)
		}
	}
	rewind(t, s, first)
	if addr, assignErr := s.Assign(ctx, "sb_over"); !errors.Is(assignErr, tunnet.ErrNoAddress) {
		t.Errorf("assign over %d held addresses = %s %v, want ErrNoAddress", assignTries, addr, assignErr)
	}
}

func TestCellsKeepTheirOwnAddresses(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	c1, c2 := open(t, dsn, "c1", time.Hour), open(t, dsn, "c2", time.Hour)
	ctx := t.Context()
	addr, err := c1.Assign(ctx, "sb_1")
	if err != nil {
		t.Fatalf("assign in c1: %v", err)
	}
	rewind(t, c2, addr)
	if other, assignErr := c2.Assign(ctx, "sb_2"); assignErr != nil || other != addr {
		t.Errorf("c2 assign = %s %v, want %s: cells do not share addresses", other, assignErr, addr)
	}
}

func TestADownDatabaseFailsClosed(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	s.Close()
	ctx := t.Context()
	if addr, err := s.Assign(ctx, "sb_1"); err == nil || addr.IsValid() {
		t.Errorf("assign on a closed pool = %s %v, want an error and no address", addr, err)
	}
	if err := s.Release(ctx, "sb_1", netip.MustParseAddr("240.0.0.2")); err == nil {
		t.Error("release on a closed pool succeeded")
	}
}

func TestOnlyAnAddressNoNodeConfirmsGoesUnconfirmed(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	ctx := t.Context()
	held, err := s.Assign(ctx, "sb_held")
	if err != nil {
		t.Fatalf("assign held: %v", err)
	}
	orphan, err := s.Assign(ctx, "sb_orphan")
	if err != nil {
		t.Fatalf("assign orphan: %v", err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET seen_at = now() - interval '2 days', allocated_at = now() - interval '2 days' WHERE cell = 'c1'`); err != nil {
		t.Fatalf("age the rows: %v", err)
	}
	if lost, confirmErr := s.Confirm(ctx, map[string]netip.Addr{"sb_held": held}); confirmErr != nil || len(lost) != 0 {
		t.Fatalf("confirm: lost %v, %v", lost, confirmErr)
	}
	stale, err := s.Unconfirmed(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("unconfirmed: %v", err)
	}
	if len(stale) != 1 || stale["sb_orphan"] != orphan {
		t.Errorf("unconfirmed %v, want only sb_orphan at %s (sb_held holds %s)", stale, orphan, held)
	}
	if _, err = s.Confirm(ctx, map[string]netip.Addr{"sb_orphan": orphan}); err != nil {
		t.Fatalf("a late confirm: %v", err)
	}
	if err = s.Reclaim(ctx, "sb_orphan", orphan, 24*time.Hour); err != nil {
		t.Fatalf("reclaim after a late confirm: %v", err)
	}
	if again, assignErr := s.Assign(ctx, "sb_orphan"); assignErr != nil || again != orphan {
		t.Errorf("a reclaim raced by a confirm released the address: assign = %s %v, want %s held", again, assignErr, orphan)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET seen_at = now() - interval '2 days', allocated_at = now() - interval '2 days' WHERE cell = 'c1'`); err != nil {
		t.Fatalf("age the rows again: %v", err)
	}
	if err = s.Reclaim(ctx, "sb_orphan", orphan, 24*time.Hour); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if stale, err = s.Unconfirmed(ctx, 24*time.Hour); err != nil || len(stale) != 1 || stale["sb_held"] != held {
		t.Errorf("unconfirmed after the reclaim %v %v, want only sb_held", stale, err)
	}
}

func TestConfirmReReservesAReleasedAddressAndReportsATakenOne(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", 0)
	ctx := t.Context()
	kept, err := s.Assign(ctx, "sb_kept")
	if err != nil {
		t.Fatalf("assign kept: %v", err)
	}
	taken, err := s.Assign(ctx, "sb_taken")
	if err != nil {
		t.Fatalf("assign taken: %v", err)
	}
	for claim, addr := range map[string]netip.Addr{"sb_kept": kept, "sb_taken": taken} {
		if err = s.Release(ctx, claim, addr); err != nil {
			t.Fatalf("release %s: %v", claim, err)
		}
	}
	rewind(t, s, taken)
	if again, assignErr := s.Assign(ctx, "sb_other"); assignErr != nil || again != taken {
		t.Fatalf("reassign = %s %v, want %s", again, assignErr, taken)
	}
	lost, err := s.Confirm(ctx, map[string]netip.Addr{"sb_kept": kept, "sb_taken": taken})
	if err != nil || !slices.Equal(lost, []string{"sb_taken"}) {
		t.Fatalf("confirm lost %v %v, want only sb_taken", lost, err)
	}
	if again, assignErr := s.Assign(ctx, "sb_kept"); assignErr != nil || again != kept {
		t.Errorf("assign after the re-reservation = %s %v, want %s held again", again, assignErr, kept)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET seen_at = now() - interval '2 days',
		allocated_at = CASE WHEN claim = 'sb_other' THEN now() ELSE now() - interval '2 days' END WHERE cell = 'c1'`); err != nil {
		t.Fatalf("age the rows: %v", err)
	}
	if stale, staleErr := s.Unconfirmed(ctx, 24*time.Hour); staleErr != nil || len(stale) != 1 || stale["sb_kept"] != kept {
		t.Errorf("unconfirmed %v %v, want only sb_kept: a fresh allocation counts as seen", stale, staleErr)
	}
}

func open(t *testing.T, dsn, cell string, reuse time.Duration) *Store {
	t.Helper()
	s, err := Open(t.Context(), dsn, cell, netip.MustParsePrefix("240.0.0.0/4"), reuse)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func rewind(t *testing.T, s *Store, addr netip.Addr) {
	t.Helper()
	off := binary.BigEndian.Uint32(addr.AsSlice()) - binary.BigEndian.Uint32(s.subnet.Addr().AsSlice()) - 1
	if _, err := s.pool.Exec(t.Context(), `SELECT setval('sandboxd_tun_addrs_cursor', $1, false)`, int64(off)); err != nil {
		t.Fatalf("rewind the cursor: %v", err)
	}
}
