// Package archivestest is the contract every archives.Registry passes, and an in-memory registry for tests.
package archivestest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
)

const (
	racers = 8

	shortLease = 300 * time.Millisecond
	leaseWait  = 10 * time.Second
)

// Opener returns an empty registry; each call is a fresh table.
type Opener func(t *testing.T) archives.Registry

var _ archives.Registry = (*Mem)(nil)

// Mem is an in-memory registry.
type Mem struct {
	mu      sync.Mutex
	rows    map[string]archives.Record
	leases  map[string]time.Time
	seq     int64
	down    bool
	deletes int
}

// NewMem returns an empty in-memory registry.
func NewMem() *Mem {
	return &Mem{rows: map[string]archives.Record{}, leases: map[string]time.Time{}}
}

func (m *Mem) Put(_ context.Context, rec archives.Record) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return 0, archives.ErrUnavailable
	}
	if old, ok := m.rows[rec.ID]; ok && old.Owner != rec.Owner {
		return 0, archives.ErrLost
	}
	m.seq++
	rec.Epoch, rec.Consumed = m.seq, false
	m.rows[rec.ID] = rec
	return rec.Epoch, nil
}

func (m *Mem) Take(_ context.Context, id string, expect int64, owner string) (int64, error) {
	return m.advance(id, expect, func(rec *archives.Record) bool {
		if rec.Owner != owner && m.leased(rec.Owner) {
			return false
		}
		rec.Owner = owner
		return true
	})
}

func (m *Mem) Consume(_ context.Context, id string, expect int64) (int64, error) {
	return m.advance(id, expect, func(rec *archives.Record) bool {
		rec.Consumed = true
		return true
	})
}

func (m *Mem) Delete(_ context.Context, id string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return archives.ErrUnavailable
	}
	if rec, ok := m.rows[id]; !ok || rec.Epoch != epoch {
		return archives.ErrLost
	}
	delete(m.rows, id)
	m.deletes++
	return nil
}

func (m *Mem) Get(_ context.Context, id string) (archives.Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return archives.Record{}, false, archives.ErrUnavailable
	}
	rec, ok := m.rows[id]
	return rec, ok, nil
}

func (m *Mem) Sweepable(_ context.Context, now time.Time, owner string) ([]archives.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, archives.ErrUnavailable
	}
	var out []archives.Record
	for _, rec := range m.rows {
		if rec.Consumed || rec.Owner == owner || !rec.Deadline.IsZero() && rec.Deadline.Before(now) && !m.leased(rec.Owner) {
			rec.Claim = nil
			out = append(out, rec)
		}
	}
	return out, nil
}

func (m *Mem) Rows(_ context.Context, ids []string) (map[string]archives.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, archives.ErrUnavailable
	}
	rows := map[string]archives.Record{}
	for _, id := range ids {
		if rec, ok := m.rows[id]; ok {
			rec.Claim = nil
			rows[id] = rec
		}
	}
	return rows, nil
}

func (m *Mem) Renew(_ context.Context, node string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return archives.ErrUnavailable
	}
	m.leases[node] = time.Now().Add(ttl)
	return nil
}

// Expire ends node's lease now.
func (m *Mem) Expire(node string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, node)
}

// SetDown makes every call answer ErrUnavailable while down is set.
func (m *Mem) SetDown(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

// Deletes counts the rows Delete removed.
func (m *Mem) Deletes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deletes
}

func (m *Mem) leased(node string) bool {
	return time.Now().Before(m.leases[node])
}

func (m *Mem) advance(id string, expect int64, change func(*archives.Record) bool) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return 0, archives.ErrUnavailable
	}
	rec, ok := m.rows[id]
	if !ok || rec.Epoch != expect || rec.Consumed {
		return 0, archives.ErrLost
	}
	if !change(&rec) {
		return 0, archives.ErrLost
	}
	m.seq++
	rec.Epoch = m.seq
	m.rows[id] = rec
	return rec.Epoch, nil
}

// Run checks the behavior every backend shares.
func Run(t *testing.T, open Opener) {
	t.Run("put get", func(t *testing.T) { testPutGet(t, open(t)) })
	t.Run("put fences other owners", func(t *testing.T) { testPutFences(t, open(t)) })
	t.Run("take is a compare-and-set", func(t *testing.T) { testTake(t, open(t)) })
	t.Run("one of concurrent takes wins", func(t *testing.T) { testTakeRace(t, open(t)) })
	t.Run("consume leaves a tombstone", func(t *testing.T) { testConsume(t, open(t)) })
	t.Run("delete only at its epoch", func(t *testing.T) { testDelete(t, open(t)) })
	t.Run("epochs are never reused", func(t *testing.T) { testEpochsNeverRepeat(t, open(t)) })
	t.Run("sweepable lists expired, consumed and own rows", func(t *testing.T) { testSweepable(t, open(t)) })
	t.Run("rows lists the named ids", func(t *testing.T) { testRows(t, open(t)) })
	t.Run("take waits out a live lease", func(t *testing.T) { testTakeLease(t, open(t)) })
	t.Run("sweepable spares a live owner's expired row", func(t *testing.T) { testSweepableLease(t, open(t)) })
	t.Run("renew moves a lease forward", func(t *testing.T) { testRenewExtends(t, open(t)) })
}

func testPutGet(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	if _, ok, err := r.Get(ctx, "sb_1"); ok || err != nil {
		t.Fatalf("get before put: %v %v, want none", ok, err)
	}
	want := record("sb_1", "a", time.Now().Add(time.Hour).Truncate(time.Microsecond))
	epoch := mustPut(t, r, want)
	got, ok, err := r.Get(ctx, "sb_1")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if got.Tenant != want.Tenant || got.TokenSHA256 != want.TokenSHA256 || got.Owner != "a" || got.Epoch != epoch || got.Consumed ||
		got.Checkpoint != want.Checkpoint || !got.Deadline.Equal(want.Deadline) || string(got.Claim) != string(want.Claim) {
		t.Errorf("get %+v, want %+v at epoch %d", got, want, epoch)
	}
	mustPut(t, r, record("sb_2", "a", time.Time{}))
	if got, _, err = r.Get(ctx, "sb_2"); err != nil || !got.Deadline.IsZero() {
		t.Errorf("keep-forever deadline %v %v, want zero", got.Deadline, err)
	}
}

func testPutFences(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	again := mustPut(t, r, record("sb_1", "a", time.Time{}))
	if again <= first {
		t.Errorf("owner re-put at epoch %d, want above %d", again, first)
	}
	if _, err := r.Put(ctx, record("sb_1", "b", time.Time{})); !errors.Is(err, archives.ErrLost) {
		t.Errorf("put over another owner's row: %v, want ErrLost", err)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Owner != "a" || got.Epoch != again {
		t.Errorf("row after a fenced put: %s@%d, want a@%d", got.Owner, got.Epoch, again)
	}
}

func testTake(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	if _, err := r.Take(ctx, "sb_1", 1, "b"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("take of no row: %v, want ErrLost", err)
	}
	put := record("sb_1", "a", time.Time{})
	first := mustPut(t, r, put)
	epoch, err := r.Take(ctx, "sb_1", first, "b")
	if err != nil || epoch <= first {
		t.Fatalf("take: %d %v, want an epoch above %d", epoch, err, first)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Owner != "b" || got.Epoch != epoch || got.Checkpoint != put.Checkpoint || string(got.Claim) != string(put.Claim) {
		t.Fatalf("row after take: %+v, want b@%d with the row", got, epoch)
	}
	if _, err = r.Take(ctx, "sb_1", first, "a"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("take at a stale epoch: %v, want ErrLost", err)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Owner != "b" || got.Epoch != epoch {
		t.Errorf("row after a stale take: %s@%d, want b@%d", got.Owner, got.Epoch, epoch)
	}
}

func testTakeRace(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	var mu sync.Mutex
	var won []int64
	lost := 0
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			epoch, err := r.Take(ctx, "sb_1", first, "n"+strconv.Itoa(i))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won = append(won, epoch)
			case errors.Is(err, archives.ErrLost):
				lost++
			default:
				t.Errorf("take: %v", err)
			}
		})
	}
	wg.Wait()
	if len(won) != 1 || lost != racers-1 {
		t.Fatalf("%d takes won and %d lost, want 1 and %d", len(won), lost, racers-1)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Epoch != won[0] {
		t.Errorf("epoch after the race %d, want the winner's %d", got.Epoch, won[0])
	}
}

func testConsume(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	if _, err := r.Consume(ctx, "sb_1", first-1); !errors.Is(err, archives.ErrLost) {
		t.Errorf("consume at a stale epoch: %v, want ErrLost", err)
	}
	consumed, err := r.Consume(ctx, "sb_1", first)
	if err != nil || consumed <= first {
		t.Fatalf("consume: %d %v, want an epoch above %d", consumed, err, first)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); !got.Consumed || got.Epoch != consumed || got.Owner != "a" {
		t.Errorf("row after consume: %+v, want a's tombstone at %d", got, consumed)
	}
	if _, err = r.Consume(ctx, "sb_1", consumed); !errors.Is(err, archives.ErrLost) {
		t.Errorf("consume of a tombstone: %v, want ErrLost", err)
	}
	if _, err = r.Take(ctx, "sb_1", consumed, "b"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("take of a tombstone: %v, want ErrLost", err)
	}
	if _, err = r.Put(ctx, record("sb_1", "b", time.Time{})); !errors.Is(err, archives.ErrLost) {
		t.Errorf("another owner's put over a tombstone: %v, want ErrLost", err)
	}
	stale := record("sb_1", "a", time.Time{})
	stale.Consumed = true
	rearmed := mustPut(t, r, stale)
	if got, _, _ := r.Get(ctx, "sb_1"); got.Consumed || got.Epoch != rearmed || rearmed <= consumed {
		t.Errorf("row after the owner's put: %+v, want live at a new epoch above %d", got, consumed)
	}
}

func testDelete(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	if err := r.Delete(ctx, "sb_1", 1); !errors.Is(err, archives.ErrLost) {
		t.Errorf("delete of no row: %v, want ErrLost", err)
	}
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	taken, err := r.Take(ctx, "sb_1", first, "b")
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if err = r.Delete(ctx, "sb_1", first); !errors.Is(err, archives.ErrLost) {
		t.Errorf("delete at a stale epoch: %v, want ErrLost", err)
	}
	if err = r.Delete(ctx, "sb_1", taken); err != nil {
		t.Fatalf("delete at the epoch: %v", err)
	}
	if _, ok, err := r.Get(ctx, "sb_1"); ok || err != nil {
		t.Errorf("get after delete: %v %v, want none", ok, err)
	}
}

func testEpochsNeverRepeat(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	if err := r.Delete(ctx, "sb_1", first); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if again := mustPut(t, r, record("sb_1", "a", time.Time{})); again <= first {
		t.Errorf("re-insert after delete at epoch %d, want above %d", again, first)
	}
	if other := mustPut(t, r, record("sb_2", "b", time.Time{})); other <= first {
		t.Errorf("another row at epoch %d, want above %d", other, first)
	}
}

func testSweepable(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	now := time.Now()
	for _, rec := range []archives.Record{
		record("sb_past", "a", now.Add(-time.Minute)),
		record("sb_future", "a", now.Add(time.Hour)),
		record("sb_forever", "a", time.Time{}),
		record("sb_mine", "z", time.Time{}),
	} {
		mustPut(t, r, rec)
	}
	live := mustPut(t, r, record("sb_spent", "a", time.Time{}))
	if _, err := r.Consume(ctx, "sb_spent", live); err != nil {
		t.Fatalf("consume: %v", err)
	}
	got, err := r.Sweepable(ctx, now, "z")
	ids := map[string]bool{}
	for _, rec := range got {
		ids[rec.ID] = rec.Checkpoint != ""
	}
	if err != nil || len(got) != 3 || !ids["sb_past"] || !ids["sb_spent"] || !ids["sb_mine"] {
		t.Errorf("sweepable for z %+v %v, want sb_past, sb_spent and sb_mine", got, err)
	}
}

func testRows(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	mustPut(t, r, record("sb_a1", "a", time.Time{}))
	a2 := mustPut(t, r, record("sb_a2", "a", time.Time{}))
	mustPut(t, r, record("sb_b1", "b", time.Time{}))
	spent, err := r.Consume(ctx, "sb_a2", a2)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	got, err := r.Rows(ctx, []string{"sb_a2", "sb_b1", "sb_none"})
	if err != nil || len(got) != 2 || got["sb_a2"].Epoch != spent || !got["sb_a2"].Consumed || got["sb_a2"].Checkpoint != "ck_sb_a2" || got["sb_b1"].Owner != "b" {
		t.Errorf("rows: %+v %v, want a's sb_a2 tombstone and b's sb_b1", got, err)
	}
	if got, err = r.Rows(ctx, nil); err != nil || len(got) != 0 {
		t.Errorf("rows of no ids: %+v %v, want none", got, err)
	}
}

func testTakeLease(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	if err := r.Renew(ctx, "a", shortLease); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if _, err := r.Take(ctx, "sb_1", first, "b"); !errors.Is(err, archives.ErrLost) {
		t.Fatalf("take while the owner's lease is live: %v, want ErrLost", err)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Owner != "a" || got.Epoch != first {
		t.Fatalf("row after a refused take: %s@%d, want a@%d", got.Owner, got.Epoch, first)
	}
	own, err := r.Take(ctx, "sb_1", first, "a")
	if err != nil || own <= first {
		t.Fatalf("the owner's own take with a live lease: %d %v, want a new epoch", own, err)
	}
	eventually(t, "the owner's lease to expire", func() bool {
		_, err = r.Take(ctx, "sb_1", own, "b")
		return err == nil
	})
	if got, _, _ := r.Get(ctx, "sb_1"); got.Owner != "b" {
		t.Errorf("row owner after the expired-lease take: %s, want b", got.Owner)
	}
	never := mustPut(t, r, record("sb_2", "c", time.Time{}))
	if _, err = r.Take(ctx, "sb_2", never, "b"); err != nil {
		t.Errorf("take from a node that never held a lease: %v, want its row adoptable", err)
	}
}

func testSweepableLease(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	past := time.Now().Add(-time.Minute)
	mustPut(t, r, record("sb_live", "a", past))
	mustPut(t, r, record("sb_dead", "z", past))
	if err := r.Renew(ctx, "a", shortLease); err != nil {
		t.Fatalf("renew: %v", err)
	}
	got, err := r.Sweepable(ctx, time.Now(), "me")
	if err != nil || len(got) != 1 || got[0].ID != "sb_dead" {
		t.Fatalf("sweepable while a's lease is live: %+v %v, want sb_dead only", got, err)
	}
	eventually(t, "a's lease to expire", func() bool {
		got, err = r.Sweepable(ctx, time.Now(), "me")
		return err == nil && len(got) == 2
	})
}

func testRenewExtends(t *testing.T, r archives.Registry) {
	ctx := t.Context()
	first := mustPut(t, r, record("sb_1", "a", time.Time{}))
	if err := r.Renew(ctx, "a", shortLease); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := r.Renew(ctx, "a", time.Hour); err != nil {
		t.Fatalf("renew again: %v", err)
	}
	time.Sleep(2 * shortLease)
	if _, err := r.Take(ctx, "sb_1", first, "b"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("take after the first ttl but inside the renewed one: %v, want ErrLost", err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(leaseWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(shortLease / 10)
	}
}

func mustPut(t *testing.T, r archives.Registry, rec archives.Record) int64 {
	t.Helper()
	epoch, err := r.Put(t.Context(), rec)
	if err != nil || epoch <= 0 {
		t.Fatalf("put %s: %d %v", rec.ID, epoch, err)
	}
	return epoch
}

func record(id, owner string, deadline time.Time) archives.Record {
	return archives.Record{
		ID: id, Tenant: "u-1", TokenSHA256: "ab12", Owner: owner,
		Checkpoint: "ck_" + id, Deadline: deadline, Claim: []byte(`{"id": "` + id + `"}`),
	}
}
