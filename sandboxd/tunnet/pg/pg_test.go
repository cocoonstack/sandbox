package pg

import (
	"context"
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

func TestAssignWalksPastARunOfHeldAddresses(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	ctx := t.Context()
	first, err := s.Assign(ctx, "sb_0")
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	var last netip.Addr
	for i := 1; i < 2*assignTries; i++ {
		if last, err = s.Assign(ctx, fmt.Sprintf("sb_%d", i)); err != nil {
			t.Fatalf("assign %d: %v", i, err)
		}
	}
	rewind(t, s, first)
	if addr, assignErr := s.Assign(ctx, "sb_over"); assignErr != nil || addr != last.Next() {
		t.Errorf("assign over %d held addresses = %s %v, want %s", 2*assignTries, addr, assignErr, last.Next())
	}
}

func TestAssignWalksPastARunLongerThanAWindow(t *testing.T) {
	s := open(t, metastoretest.PGSchema(t), "c1", time.Hour)
	run := assignWindow + 476
	hold(t, s, run)
	want := tunnet.HostAt(s.subnet, uint64(run)+1)
	if addr, err := s.Assign(t.Context(), "sb_over"); err != nil || addr != want {
		t.Errorf("assign over %d held addresses = %s %v, want %s", run, addr, err, want)
	}
}

func TestAssignGivesUpAfterOneLapOrItsLastWindow(t *testing.T) {
	tests := []struct {
		subnet       string
		held, probes int64
	}{
		{"240.0.0.0/29", 6, 6},
		{"240.0.0.0/20", 4094, 4094},
		{"240.0.0.0/16", 65534, 65534},
		{"240.0.0.0/8", assignTries*assignWindow + 1, assignTries * assignWindow},
	}
	for _, tt := range tests {
		t.Run(tt.subnet, func(t *testing.T) {
			s := openSubnet(t, metastoretest.PGSchema(t), "c1", tt.subnet, time.Hour)
			hold(t, s, tt.held)
			if addr, err := s.Assign(t.Context(), "sb_over"); !errors.Is(err, tunnet.ErrNoAddress) {
				t.Fatalf("assign over %d held addresses = %s %v, want ErrNoAddress", tt.held, addr, err)
			}
			var probes int64
			if err := s.pool.QueryRow(t.Context(), `SELECT last_value FROM sandboxd_tun_addrs_cursor`).Scan(&probes); err != nil || probes != tt.probes {
				t.Errorf("cursor walked %d candidates %v, want %d", probes, err, tt.probes)
			}
		})
	}
}

func TestAssignReportsAFullSubnetAndFindsItsOnlyFreeAddress(t *testing.T) {
	ctx := t.Context()
	s := openSubnet(t, metastoretest.PGSchema(t), "c1", "240.0.0.0/29", time.Hour)
	held := map[string]netip.Addr{}
	var err error
	for i := range 6 {
		id := fmt.Sprintf("sb_%d", i)
		if held[id], err = s.Assign(ctx, id); err != nil {
			t.Fatalf("assign %s: %v", id, err)
		}
	}
	if addr, assignErr := s.Assign(ctx, "sb_over"); !errors.Is(assignErr, tunnet.ErrNoAddress) {
		t.Fatalf("assign on a full subnet = %s %v, want ErrNoAddress", addr, assignErr)
	}
	if err = s.Release(ctx, "sb_3", held["sb_3"]); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET released_at = now() - interval '2 hours' WHERE claim = 'sb_3'`); err != nil {
		t.Fatalf("age the release: %v", err)
	}
	if addr, assignErr := s.Assign(ctx, "sb_over"); assignErr != nil || addr != held["sb_3"] {
		t.Errorf("assign with one free address = %s %v, want %s", addr, assignErr, held["sb_3"])
	}
}

func TestAssignFindsAFreeAddressTheSharedCursorSkipped(t *testing.T) {
	s := openSubnet(t, metastoretest.PGSchema(t), "c1", "240.0.0.0/29", time.Hour)
	hold(t, s, 5)
	if _, err := s.pool.Exec(t.Context(), `ALTER SEQUENCE sandboxd_tun_addrs_cursor INCREMENT BY 2 RESTART WITH 1`); err != nil {
		t.Fatalf("skew the cursor: %v", err)
	}
	want := tunnet.HostAt(s.subnet, 0)
	if addr, err := s.Assign(t.Context(), "sb_last"); err != nil || addr != want {
		t.Errorf("assign with %s free behind a cursor that skips it = %s %v, want %s", want, addr, err, want)
	}
}

func TestASweepThatLosesItsAddressTakesTheNextFreeOne(t *testing.T) {
	ctx := t.Context()
	s := openSubnet(t, metastoretest.PGSchema(t), "c1", "240.0.0.0/29", time.Hour)
	for _, n := range []uint64{0, 2, 3, 4} {
		if _, err := s.pool.Exec(ctx, `INSERT INTO sandboxd_tun_addrs (cell, address, claim) VALUES ($1, $2, $3)`, s.cell, tunnet.HostAt(s.subnet, n).String(), fmt.Sprintf("held_%d", n)); err != nil {
			t.Fatalf("hold: %v", err)
		}
	}
	if _, err := s.pool.Exec(ctx, `ALTER SEQUENCE sandboxd_tun_addrs_cursor INCREMENT BY 2 RESTART WITH 0 MINVALUE 0`); err != nil {
		t.Fatalf("skew the cursor: %v", err)
	}
	racer, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = racer.Rollback(context.Background()) }()
	if _, err = racer.Exec(ctx, `INSERT INTO sandboxd_tun_addrs (cell, address, claim) VALUES ($1, $2, 'racer')`, s.cell, tunnet.HostAt(s.subnet, 1).String()); err != nil {
		t.Fatalf("race: %v", err)
	}
	type result struct {
		addr netip.Addr
		err  error
	}
	done := make(chan result, 1)
	go func() {
		addr, aerr := s.Assign(ctx, "sb_last")
		done <- result{addr, aerr}
	}()
	blocked := false
	for range 100 {
		if err = s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'WITH held%')`).Scan(&blocked); err != nil || blocked {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !blocked {
		t.Fatalf("the sweep never waited on the racer's address: %v", err)
	}
	if err = racer.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	want := tunnet.HostAt(s.subnet, 5)
	if got := <-done; got.err != nil || got.addr != want {
		t.Errorf("assign after the racer took %s = %s %v, want %s", tunnet.HostAt(s.subnet, 1), got.addr, got.err, want)
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
	if reclaimed, reclaimErr := s.Reclaim(ctx, "sb_orphan", orphan, 24*time.Hour); reclaimErr != nil || reclaimed {
		t.Fatalf("reclaim after a late confirm = %v %v, want nothing reclaimed", reclaimed, reclaimErr)
	}
	if again, assignErr := s.Assign(ctx, "sb_orphan"); assignErr != nil || again != orphan {
		t.Errorf("a reclaim raced by a confirm released the address: assign = %s %v, want %s held", again, assignErr, orphan)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE sandboxd_tun_addrs SET seen_at = now() - interval '2 days', allocated_at = now() - interval '2 days' WHERE cell = 'c1'`); err != nil {
		t.Fatalf("age the rows again: %v", err)
	}
	if reclaimed, reclaimErr := s.Reclaim(ctx, "sb_orphan", orphan, 24*time.Hour); reclaimErr != nil || !reclaimed {
		t.Fatalf("reclaim = %v %v, want the address reclaimed", reclaimed, reclaimErr)
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
	return openSubnet(t, dsn, cell, "240.0.0.0/4", reuse)
}

func openSubnet(t *testing.T, dsn, cell, subnet string, reuse time.Duration) *Store {
	t.Helper()
	s, err := Open(t.Context(), dsn, cell, netip.MustParsePrefix(subnet), reuse)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func hold(t *testing.T, s *Store, n int64) {
	t.Helper()
	hosts := int64(1)<<(32-s.subnet.Bits()) - 2
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO sandboxd_tun_addrs (cell, address, claim)
		SELECT $1, host($2::inet + (1 + i % $3)), 'held_' || i FROM generate_series(1, $4::bigint) i`,
		s.cell, s.subnet.Addr().String(), hosts, n); err != nil {
		t.Fatalf("hold %d addresses: %v", n, err)
	}
	rewind(t, s, tunnet.HostAt(s.subnet, 1))
}

func rewind(t *testing.T, s *Store, addr netip.Addr) {
	t.Helper()
	off := binary.BigEndian.Uint32(addr.AsSlice()) - binary.BigEndian.Uint32(s.subnet.Addr().AsSlice()) - 1
	if _, err := s.pool.Exec(t.Context(), `SELECT setval('sandboxd_tun_addrs_cursor', $1, false)`, int64(off)); err != nil {
		t.Fatalf("rewind the cursor: %v", err)
	}
}
