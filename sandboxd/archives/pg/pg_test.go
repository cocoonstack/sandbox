package pg

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/archives/archivestest"
	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
)

func TestRegistryContract(t *testing.T) {
	archivestest.Run(t, func(t *testing.T) archives.Registry { return open(t, metastoretest.PGSchema(t), "c1") })
}

func TestCellsKeepTheirOwnRows(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	c1, c2 := open(t, dsn, "c1"), open(t, dsn, "c2")
	ctx := t.Context()
	past := time.Now().Add(-time.Minute)
	epoch, err := c1.Put(ctx, archives.Record{ID: "sb_1", TokenSHA256: "ab", Owner: "n1", Checkpoint: "ck_1", Deadline: past, Claim: []byte(`{}`)})
	if err != nil {
		t.Fatalf("put in c1: %v", err)
	}
	if _, ok, getErr := c2.Get(ctx, "sb_1"); ok || getErr != nil {
		t.Errorf("c2 sees c1's row: %v %v", ok, getErr)
	}
	if _, err = c2.Take(ctx, "sb_1", epoch, "n2"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("c2 takes c1's row: %v, want ErrLost", err)
	}
	if _, err = c2.Consume(ctx, "sb_1", epoch); !errors.Is(err, archives.ErrLost) {
		t.Errorf("c2 consumes c1's row: %v, want ErrLost", err)
	}
	if err = c2.Delete(ctx, "sb_1", epoch); !errors.Is(err, archives.ErrLost) {
		t.Errorf("c2 deletes c1's row: %v, want ErrLost", err)
	}
	if recs, listErr := c2.Sweepable(ctx, time.Now(), "n2"); listErr != nil || len(recs) != 0 {
		t.Errorf("c2 lists c1's expired row: %+v %v", recs, listErr)
	}
	if _, err = c2.Put(ctx, archives.Record{ID: "sb_1", TokenSHA256: "cd", Owner: "n2", Checkpoint: "ck_2", Claim: []byte(`{}`)}); err != nil {
		t.Errorf("put of the same id in c2 by another owner: %v, want its own row", err)
	}
	if rec, _, _ := c1.Get(ctx, "sb_1"); rec.Checkpoint != "ck_1" {
		t.Errorf("c1's row after c2's put names %s, want ck_1", rec.Checkpoint)
	}
}

func TestSchemaIndexesTheCellDeadline(t *testing.T) {
	r := open(t, metastoretest.PGSchema(t), "c1")
	var n int
	if err := r.pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes
		WHERE schemaname = current_schema() AND tablename = 'sandboxd_archives' AND indexname IN ('sandboxd_archives_deadline', 'sandboxd_archives_consumed')`).Scan(&n); err != nil || n != 2 {
		t.Errorf("deadline and consumed index count %d %v, want 2", n, err)
	}
}

func TestTwoNodesShareTheTable(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	a, b := open(t, dsn, "c1"), open(t, dsn, "c1")
	ctx := t.Context()
	epoch, err := a.Put(ctx, archives.Record{ID: "sb_1", TokenSHA256: "ab", Owner: "a", Checkpoint: "ck_1", Claim: []byte(`{}`)})
	if err != nil {
		t.Fatalf("put on a: %v", err)
	}
	if _, err = b.Take(ctx, "sb_1", epoch, "b"); err != nil {
		t.Fatalf("take on b: %v", err)
	}
	if _, err = a.Take(ctx, "sb_1", epoch, "a"); !errors.Is(err, archives.ErrLost) {
		t.Errorf("a's take at its old epoch: %v, want ErrLost", err)
	}
	if _, err = a.Put(ctx, archives.Record{ID: "sb_1", TokenSHA256: "ab", Owner: "a", Checkpoint: "ck_2", Claim: []byte(`{}`)}); !errors.Is(err, archives.ErrLost) {
		t.Errorf("a's put over b's row: %v, want ErrLost", err)
	}
}

func TestADownDatabaseIsUnavailable(t *testing.T) {
	r := open(t, metastoretest.PGSchema(t), "c1")
	r.Close()
	ctx := t.Context()
	if _, _, err := r.Get(ctx, "sb_1"); !errors.Is(err, archives.ErrUnavailable) {
		t.Errorf("get on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := r.Take(ctx, "sb_1", 1, "a"); !errors.Is(err, archives.ErrUnavailable) {
		t.Errorf("take on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := r.Consume(ctx, "sb_1", 1); !errors.Is(err, archives.ErrUnavailable) {
		t.Errorf("consume on a closed pool: %v, want ErrUnavailable", err)
	}
	if err := r.Delete(ctx, "sb_1", 1); !errors.Is(err, archives.ErrUnavailable) {
		t.Errorf("delete on a closed pool: %v, want ErrUnavailable", err)
	}
	if _, err := r.Sweepable(ctx, time.Now(), "a"); !errors.Is(err, archives.ErrUnavailable) {
		t.Errorf("sweepable on a closed pool: %v, want ErrUnavailable", err)
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
