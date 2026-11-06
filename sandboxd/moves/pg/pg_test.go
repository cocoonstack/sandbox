package pg

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/moves/movestest"
)

func TestRegistryContract(t *testing.T) {
	movestest.Run(t, func(t *testing.T) moves.Registry { return open(t, metastoretest.PGSchema(t), "c1") })
}

func TestCellsKeepTheirOwnRecords(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	c1, c2 := open(t, dsn, "c1"), open(t, dsn, "c2")
	ctx := t.Context()
	epoch, err := c1.Begin(ctx, "sb_1", "s", "t")
	if err != nil {
		t.Fatalf("begin in c1: %v", err)
	}
	if _, ok, getErr := c2.Get(ctx, "sb_1"); ok || getErr != nil {
		t.Errorf("c2 sees c1's record: %v %v", ok, getErr)
	}
	if _, err = c2.Advance(ctx, "sb_1", epoch, moves.Pending, moves.Aborted); !errors.Is(err, moves.ErrLost) {
		t.Errorf("c2 aborts c1's move: %v, want ErrLost", err)
	}
	if err = c2.Delete(ctx, "sb_1", epoch); !errors.Is(err, moves.ErrLost) {
		t.Errorf("c2 deletes c1's record: %v, want ErrLost", err)
	}
	if recs, listErr := c2.Stale(ctx, time.Now().Add(time.Hour)); listErr != nil || len(recs) != 0 {
		t.Errorf("c2 lists c1's record as stale: %+v %v", recs, listErr)
	}
	if _, err = c2.Begin(ctx, "sb_1", "u", "v"); err != nil {
		t.Errorf("begin of the same id in c2: %v, want its own record", err)
	}
	if rec, _, _ := c1.Get(ctx, "sb_1"); rec.Epoch != epoch || rec.State != moves.Pending || rec.From != "s" {
		t.Errorf("c1's record after c2's begin: %+v, want s pending at %d", rec, epoch)
	}
}

func TestADownDatabaseIsUnavailable(t *testing.T) {
	r := open(t, metastoretest.PGSchema(t), "c1")
	r.Close()
	ctx := t.Context()
	if _, err := r.Begin(ctx, "sb_1", "s", "t"); !errors.Is(err, moves.ErrUnavailable) {
		t.Errorf("begin on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := r.Advance(ctx, "sb_1", 1, moves.Pending, moves.Final); !errors.Is(err, moves.ErrUnavailable) {
		t.Errorf("advance on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, _, err := r.Get(ctx, "sb_1"); !errors.Is(err, moves.ErrUnavailable) {
		t.Errorf("get on a closed pool: %v, want ErrUnavailable", err)
	}
	if err := r.Delete(ctx, "sb_1", 1); !errors.Is(err, moves.ErrUnavailable) {
		t.Errorf("delete on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := r.Stale(ctx, time.Now()); !errors.Is(err, moves.ErrUnavailable) {
		t.Errorf("stale on a closed pool: %v, want ErrUnavailable", err)
	}
}

func open(t *testing.T, dsn, cell string) *Registry {
	t.Helper()
	r, err := Open(t.Context(), dsn, cell)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(r.Close)
	return r
}
