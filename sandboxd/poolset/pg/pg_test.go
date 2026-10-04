package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const fireWait = 5 * time.Second

func TestStoreVersionsTheCellsSet(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	a, other := openIn(t, dsn, "c1"), openIn(t, dsn, "c2")
	ctx := t.Context()
	if set, version, err := a.Load(ctx); set != nil || version != 0 || err != nil {
		t.Fatalf("load before any store: %+v %d %v, want none", set, version, err)
	}
	for want := range int64(2) {
		set := poolset.Set{ConfigSeed: "seed", Pools: []config.PoolSpec{{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Warm: int(want) + 1}}}
		version, err := a.Store(ctx, set)
		if err != nil || version != want+1 {
			t.Fatalf("store %d: version %d %v, want %d", want, version, err, want+1)
		}
		got, version, err := a.Load(ctx)
		if err != nil || version != want+1 || got.ConfigSeed != "seed" || len(got.Pools) != 1 || got.Pools[0].Warm != int(want)+1 {
			t.Fatalf("load after store %d: %+v %d %v", want, got, version, err)
		}
	}
	if version, err := a.Store(ctx, poolset.Set{}); err != nil || version != 3 {
		t.Fatalf("store of no pools: %d %v", version, err)
	}
	if got, _, err := a.Load(ctx); err != nil || got == nil || len(got.Pools) != 0 {
		t.Fatalf("an emptied set loads as %+v %v, want an empty set, not none", got, err)
	}
	if set, _, err := other.Load(ctx); set != nil || err != nil {
		t.Errorf("another cell sees %+v %v, want none", set, err)
	}
}

func TestWatchFiresOnConnectAndOnItsCellsWrites(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	a, b, other := openIn(t, dsn, "c1"), openIn(t, dsn, "c1"), openIn(t, dsn, "c2")
	ctx, cancel := context.WithCancel(t.Context())
	fired := make(chan struct{}, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Watch(ctx, func() { fired <- struct{}{} })
	}()
	defer func() { cancel(); <-done }()
	waitFire(t, fired, "the listener connect")
	if _, err := other.Store(t.Context(), poolset.Set{}); err != nil {
		t.Fatalf("store on c2: %v", err)
	}
	if _, err := b.Store(t.Context(), poolset.Set{}); err != nil {
		t.Fatalf("store on c1: %v", err)
	}
	waitFire(t, fired, "a write by another node of the cell")
	select {
	case <-fired:
		t.Error("a write to another cell fired the watch")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestADownDatabaseIsUnavailable(t *testing.T) {
	s := openIn(t, metastoretest.PGSchema(t), "c1")
	_ = s.Close()
	if _, _, err := s.Load(t.Context()); !errors.Is(err, poolset.ErrUnavailable) {
		t.Errorf("load on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := s.Store(t.Context(), poolset.Set{}); !errors.Is(err, poolset.ErrUnavailable) {
		t.Errorf("store on a closed pool: %v, want ErrUnavailable", err)
	}
}

func openIn(t *testing.T, dsn, cell string) *Source {
	t.Helper()
	s, err := Open(t.Context(), dsn, cell)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func waitFire(t *testing.T, fired <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(fireWait):
		t.Fatalf("no watch call after %s", what)
	}
}
