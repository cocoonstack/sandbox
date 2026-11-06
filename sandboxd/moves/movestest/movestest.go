// Package movestest is the contract every moves.Registry passes, and an in-memory registry for tests.
package movestest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/moves"
)

const racers = 8

var states = []moves.State{moves.Pending, moves.Final, moves.Committed, moves.Started, moves.Aborted}

// Opener returns an empty registry; each call is a fresh table.
type Opener func(t *testing.T) moves.Registry

var _ moves.Registry = (*Mem)(nil)

// Mem is an in-memory registry.
type Mem struct {
	mu   sync.Mutex
	recs map[string]moves.Record
	seq  int64
	down bool
}

// NewMem returns an empty in-memory registry.
func NewMem() *Mem {
	return &Mem{recs: map[string]moves.Record{}}
}

func (m *Mem) Begin(_ context.Context, id, from, to string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return 0, moves.ErrUnavailable
	}
	if old, ok := m.recs[id]; ok && old.State != moves.Started && old.State != moves.Aborted {
		return 0, moves.ErrLost
	}
	return m.write(moves.Record{ID: id, From: from, To: to, State: moves.Pending}), nil
}

func (m *Mem) Advance(_ context.Context, id string, expect int64, from, to moves.State) (int64, error) {
	if err := moves.VerifyAdvance(from, to); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return 0, moves.ErrUnavailable
	}
	rec, ok := m.recs[id]
	if !ok || rec.Epoch != expect || rec.State != from {
		return 0, moves.ErrLost
	}
	rec.State = to
	return m.write(rec), nil
}

func (m *Mem) Get(_ context.Context, id string) (moves.Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return moves.Record{}, false, moves.ErrUnavailable
	}
	rec, ok := m.recs[id]
	return rec, ok, nil
}

func (m *Mem) Delete(_ context.Context, id string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return moves.ErrUnavailable
	}
	if rec, ok := m.recs[id]; !ok || rec.Epoch != epoch {
		return moves.ErrLost
	}
	delete(m.recs, id)
	return nil
}

func (m *Mem) Stale(_ context.Context, before time.Time) ([]moves.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return nil, moves.ErrUnavailable
	}
	var out []moves.Record
	for _, rec := range m.recs {
		if rec.UpdatedAt.Before(before) {
			out = append(out, rec)
		}
	}
	return out, nil
}

// SetDown makes every call answer ErrUnavailable while down is set.
func (m *Mem) SetDown(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

func (m *Mem) write(rec moves.Record) int64 {
	m.seq++
	rec.Epoch, rec.UpdatedAt = m.seq, time.Now()
	m.recs[rec.ID] = rec
	return rec.Epoch
}

// Run checks the behavior every backend shares.
func Run(t *testing.T, open Opener) {
	t.Run("begin get", func(t *testing.T) { testBeginGet(t, open(t)) })
	t.Run("every allowed advance", func(t *testing.T) { testAdvance(t, open(t)) })
	t.Run("advance is a compare-and-set", func(t *testing.T) { testAdvanceFences(t, open(t)) })
	t.Run("advance refuses a state change no move makes", func(t *testing.T) { testAdvanceRefusesPairs(t, open(t)) })
	t.Run("one of concurrent advances wins", func(t *testing.T) { testAdvanceRace(t, open(t)) })
	t.Run("begin replaces an ended record", func(t *testing.T) { testBeginOverEnded(t, open(t)) })
	t.Run("begin refuses a live record", func(t *testing.T) { testBeginOverLive(t, open(t)) })
	t.Run("delete only at its epoch", func(t *testing.T) { testDelete(t, open(t)) })
	t.Run("epochs are never reused", func(t *testing.T) { testEpochsNeverRepeat(t, open(t)) })
	t.Run("stale lists records written before a time", func(t *testing.T) { testStale(t, open(t)) })
}

// Reach advances a record begun at epoch to state and returns its last epoch.
func Reach(t *testing.T, r moves.Registry, id string, epoch int64, state moves.State) int64 {
	t.Helper()
	path := map[moves.State][]moves.State{
		moves.Final:     {moves.Final},
		moves.Committed: {moves.Final, moves.Committed},
		moves.Started:   {moves.Final, moves.Committed, moves.Started},
		moves.Aborted:   {moves.Aborted},
	}[state]
	from := moves.Pending
	for _, to := range path {
		epoch = mustAdvance(t, r, id, epoch, from, to)
		from = to
	}
	return epoch
}

func testBeginGet(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	if _, ok, err := r.Get(ctx, "sb_1"); ok || err != nil {
		t.Fatalf("get before begin: %v %v, want none", ok, err)
	}
	epoch := mustBegin(t, r, "sb_1")
	got, ok, err := r.Get(ctx, "sb_1")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if got.ID != "sb_1" || got.From != "s" || got.To != "t" || got.State != moves.Pending || got.Epoch != epoch || got.UpdatedAt.IsZero() {
		t.Errorf("get %+v, want s to t pending at epoch %d", got, epoch)
	}
}

func testAdvance(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	for i, path := range [][]moves.State{
		{moves.Final, moves.Committed, moves.Started},
		{moves.Aborted},
		{moves.Final, moves.Aborted},
	} {
		id := "sb_" + strconv.Itoa(i)
		epoch := mustBegin(t, r, id)
		from := moves.Pending
		for _, to := range path {
			next, err := r.Advance(ctx, id, epoch, from, to)
			if err != nil || next <= epoch {
				t.Fatalf("%s advance %s to %s: %d %v, want an epoch above %d", id, from, to, next, err, epoch)
			}
			epoch, from = next, to
			if got, _, _ := r.Get(ctx, id); got.State != to || got.Epoch != epoch || got.From != "s" || got.To != "t" {
				t.Fatalf("%s after advance: %+v, want %s at %d", id, got, to, epoch)
			}
		}
	}
}

func testAdvanceFences(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	if _, err := r.Advance(ctx, "sb_none", 1, moves.Pending, moves.Final); !errors.Is(err, moves.ErrLost) {
		t.Errorf("advance of no record: %v, want ErrLost", err)
	}
	first := mustBegin(t, r, "sb_1")
	if _, err := r.Advance(ctx, "sb_1", first-1, moves.Pending, moves.Final); !errors.Is(err, moves.ErrLost) {
		t.Errorf("advance at a stale epoch: %v, want ErrLost", err)
	}
	if _, err := r.Advance(ctx, "sb_1", first, moves.Final, moves.Committed); !errors.Is(err, moves.ErrLost) {
		t.Errorf("advance from a state the record is not in: %v, want ErrLost", err)
	}
	final := mustAdvance(t, r, "sb_1", first, moves.Pending, moves.Final)
	if _, err := r.Advance(ctx, "sb_1", first, moves.Pending, moves.Aborted); !errors.Is(err, moves.ErrLost) {
		t.Errorf("abort at the superseded epoch: %v, want ErrLost", err)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.State != moves.Final || got.Epoch != final {
		t.Errorf("record after refused advances: %s@%d, want final@%d", got.State, got.Epoch, final)
	}
	committed := mustAdvance(t, r, "sb_1", final, moves.Final, moves.Committed)
	if _, err := r.Advance(ctx, "sb_1", committed, moves.Final, moves.Aborted); !errors.Is(err, moves.ErrLost) {
		t.Errorf("abort of a committed move: %v, want ErrLost", err)
	}
	if _, ok, _ := r.Get(ctx, "sb_none"); ok {
		t.Errorf("a refused advance inserted a record")
	}
}

func testAdvanceRefusesPairs(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	for _, from := range states {
		for _, to := range states {
			if moves.VerifyAdvance(from, to) == nil {
				continue
			}
			id := "sb_" + string(from) + "_" + string(to)
			epoch := mustReach(t, r, id, from)
			_, err := r.Advance(ctx, id, epoch, from, to)
			if err == nil || errors.Is(err, moves.ErrLost) || errors.Is(err, moves.ErrUnavailable) {
				t.Errorf("advance %s to %s: %v, want a plain error", from, to, err)
			}
			if got, _, _ := r.Get(ctx, id); got.State != from || got.Epoch != epoch {
				t.Errorf("%s after a refused pair: %s@%d, want %s@%d", id, got.State, got.Epoch, from, epoch)
			}
		}
	}
}

func testAdvanceRace(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	final := mustAdvance(t, r, "sb_1", mustBegin(t, r, "sb_1"), moves.Pending, moves.Final)
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		won  []int64
		ends []moves.State
		lost int
	)
	for i := range racers {
		end := moves.Committed
		if i%2 == 1 {
			end = moves.Aborted
		}
		wg.Go(func() {
			epoch, err := r.Advance(ctx, "sb_1", final, moves.Final, end)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won, ends = append(won, epoch), append(ends, end)
			case errors.Is(err, moves.ErrLost):
				lost++
			default:
				t.Errorf("advance: %v", err)
			}
		})
	}
	wg.Wait()
	if len(won) != 1 || lost != racers-1 {
		t.Fatalf("%d advances won and %d lost, want 1 and %d", len(won), lost, racers-1)
	}
	if got, _, _ := r.Get(ctx, "sb_1"); got.Epoch != won[0] || got.State != ends[0] {
		t.Errorf("record after the race %s@%d, want the winner's %s@%d", got.State, got.Epoch, ends[0], won[0])
	}
}

func testBeginOverEnded(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	for _, end := range []moves.State{moves.Started, moves.Aborted} {
		id := "sb_" + string(end)
		ended := mustReach(t, r, id, end)
		again, err := r.Begin(ctx, id, "t", "u")
		if err != nil || again <= ended {
			t.Fatalf("begin over %s: %d %v, want an epoch above %d", end, again, err, ended)
		}
		if got, _, _ := r.Get(ctx, id); got.State != moves.Pending || got.Epoch != again || got.From != "t" || got.To != "u" {
			t.Errorf("record after begin over %s: %+v, want t to u pending at %d", end, got, again)
		}
	}
}

func testBeginOverLive(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	for _, live := range []moves.State{moves.Pending, moves.Final, moves.Committed} {
		id := "sb_" + string(live)
		epoch := mustReach(t, r, id, live)
		if _, err := r.Begin(ctx, id, "t", "u"); !errors.Is(err, moves.ErrLost) {
			t.Errorf("begin over %s: %v, want ErrLost", live, err)
		}
		if got, _, _ := r.Get(ctx, id); got.State != live || got.Epoch != epoch || got.From != "s" || got.To != "t" {
			t.Errorf("record after a refused begin over %s: %+v, want s to t %s at %d", live, got, live, epoch)
		}
	}
}

func testDelete(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	if err := r.Delete(ctx, "sb_1", 1); !errors.Is(err, moves.ErrLost) {
		t.Errorf("delete of no record: %v, want ErrLost", err)
	}
	first := mustBegin(t, r, "sb_1")
	aborted := mustAdvance(t, r, "sb_1", first, moves.Pending, moves.Aborted)
	if err := r.Delete(ctx, "sb_1", first); !errors.Is(err, moves.ErrLost) {
		t.Errorf("delete at a stale epoch: %v, want ErrLost", err)
	}
	if err := r.Delete(ctx, "sb_1", aborted); err != nil {
		t.Fatalf("delete at the epoch: %v", err)
	}
	if _, ok, err := r.Get(ctx, "sb_1"); ok || err != nil {
		t.Errorf("get after delete: %v %v, want none", ok, err)
	}
}

func testEpochsNeverRepeat(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	first := mustBegin(t, r, "sb_1")
	if err := r.Delete(ctx, "sb_1", first); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if again := mustBegin(t, r, "sb_1"); again <= first {
		t.Errorf("begin after delete at epoch %d, want above %d", again, first)
	}
	if other := mustBegin(t, r, "sb_2"); other <= first {
		t.Errorf("another record at epoch %d, want above %d", other, first)
	}
}

func testStale(t *testing.T, r moves.Registry) {
	ctx := t.Context()
	mustBegin(t, r, "sb_1")
	mustReach(t, r, "sb_2", moves.Committed)
	if got, err := r.Stale(ctx, time.Now().Add(-time.Hour)); err != nil || len(got) != 0 {
		t.Errorf("stale before the records were written: %+v %v, want none", got, err)
	}
	got, err := r.Stale(ctx, time.Now().Add(time.Hour))
	ids := map[string]bool{}
	for _, rec := range got {
		ids[rec.ID] = rec.From == "s" && rec.To == "t" && rec.Epoch > 0
	}
	if err != nil || len(got) != 2 || !ids["sb_1"] || !ids["sb_2"] {
		t.Errorf("stale after the records were written: %+v %v, want sb_1 and sb_2", got, err)
	}
}

func mustBegin(t *testing.T, r moves.Registry, id string) int64 {
	t.Helper()
	epoch, err := r.Begin(t.Context(), id, "s", "t")
	if err != nil || epoch <= 0 {
		t.Fatalf("begin %s: %d %v", id, epoch, err)
	}
	return epoch
}

func mustAdvance(t *testing.T, r moves.Registry, id string, expect int64, from, to moves.State) int64 {
	t.Helper()
	epoch, err := r.Advance(t.Context(), id, expect, from, to)
	if err != nil || epoch <= expect {
		t.Fatalf("advance %s from %s to %s: %d %v", id, from, to, epoch, err)
	}
	return epoch
}

func mustReach(t *testing.T, r moves.Registry, id string, state moves.State) int64 {
	t.Helper()
	return Reach(t, r, id, mustBegin(t, r, id), state)
}
