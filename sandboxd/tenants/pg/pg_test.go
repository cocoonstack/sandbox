package pg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/tenantstest"
)

func TestContract(t *testing.T) {
	tenantstest.Run(t, func(t *testing.T) tenants.Source { return openIn(t, metastoretest.PGSchema(t)) })
}

func TestAWriteOnOneNodeReachesTheOtherNodesCache(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	a, b := openIn(t, dsn), openIn(t, dsn)
	ctx := t.Context()
	if _, err := b.Resolve(ctx, tenantstest.Sum("late")); !errors.Is(err, tenants.ErrUnknown) {
		t.Fatalf("an unknown token on b: %v", err)
	}
	if _, err := a.Put(ctx, tenantstest.Record("u-1", "late", 1, "desk")); err != nil {
		t.Fatalf("put on a: %v", err)
	}
	if !tenantstest.Eventually(t, func() bool { r, err := b.Resolve(ctx, tenantstest.Sum("late")); return err == nil && r.Name == "u-1" }) {
		t.Fatal("b kept serving its cached miss after a's put")
	}
	if _, err := a.Put(ctx, tenantstest.Record("u-1", "late", 4, "desk")); err != nil {
		t.Fatalf("change on a: %v", err)
	}
	if !tenantstest.Eventually(t, func() bool { r, err := b.Resolve(ctx, tenantstest.Sum("late")); return err == nil && r.MaxClaims == 4 }) {
		t.Error("b kept serving the cached record after a's change")
	}
	if _, err := a.Delete(ctx, "u-1"); err != nil {
		t.Fatalf("delete on a: %v", err)
	}
	if !tenantstest.Eventually(t, func() bool {
		_, err := b.Resolve(ctx, tenantstest.Sum("late"))
		return errors.Is(err, tenants.ErrUnknown)
	}) {
		t.Error("b still authenticates a tenant a deleted")
	}
	if !tenantstest.Eventually(t, func() bool { _, p := b.Peek("u-1"); return p == tenants.Absent }) {
		t.Error("b's peek never saw the deletion")
	}
	if b.Records() != nil || b.Digest() != "" {
		t.Error("a shared source reported node-local records or a digest")
	}
}

func TestACachedTenantOutlivesTheDatabase(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	s := openIn(t, dsn)
	ctx := t.Context()
	if _, err := s.Put(ctx, tenantstest.Record("u-1", "warm", 0, "")); err != nil {
		t.Fatalf("put: %v", err)
	}
	s.stop()
	<-s.done
	if r, err := s.Resolve(ctx, tenantstest.Sum("warm")); err != nil || r.Name != "u-1" {
		t.Fatalf("resolve before the database goes: %+v %v", r, err)
	}
	s.pool.Close()
	if r, err := s.Resolve(ctx, tenantstest.Sum("warm")); err != nil || r.Name != "u-1" {
		t.Errorf("a cached tenant with the database gone: %+v %v", r, err)
	}
	if _, err := s.Resolve(ctx, tenantstest.Sum("cold")); !errors.Is(err, tenants.ErrUnavailable) {
		t.Errorf("an uncached token with the database gone: %v, want ErrUnavailable", err)
	}
	if _, err := s.Put(ctx, tenantstest.Record("u-2", "t2", 0, "")); !errors.Is(err, tenants.ErrUnavailable) {
		t.Errorf("a write with the database gone: %v, want ErrUnavailable", err)
	}
}

func TestARoleWithoutCreateServesAPreCreatedTable(t *testing.T) {
	dsn := metastoretest.PGSchema(t)
	owner := openIn(t, dsn)
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner: %v", err)
	}
	_, schema, _ := strings.CutLast(dsn, "=")
	role := "r" + schema
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = admin.Close(t.Context()) }()
	for _, sql := range []string{
		"CREATE ROLE " + role + " LOGIN PASSWORD 'p'",
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON " + schema + ".sandboxd_tenants TO " + role,
	} {
		if _, err = admin.Exec(t.Context(), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		if c, cerr := pgx.Connect(context.Background(), dsn); cerr == nil {
			_, _ = c.Exec(context.Background(), "DROP OWNED BY "+role)
			_, _ = c.Exec(context.Background(), "DROP ROLE "+role)
			_ = c.Close(context.Background())
		}
	})
	s := openIn(t, restricted(dsn, role))
	if _, err = s.Put(t.Context(), tenantstest.Record("u-1", "t1", 0, "")); err != nil {
		t.Errorf("a write as the restricted role: %v", err)
	}
}

func TestAnUnrelatedEvictionKeepsALookupCacheable(t *testing.T) {
	s := &Source{evicted: map[string]uint64{}}
	start := s.seq
	s.seq++
	s.evicted["other"] = s.seq
	if !s.fresh("u-1", start) {
		t.Error("another tenant's eviction discarded a read of u-1")
	}
	s.seq++
	s.evicted["u-1"] = s.seq
	if s.fresh("u-1", start) {
		t.Error("a read that raced u-1's own eviction stays cacheable")
	}
	s.seq++
	s.floor = s.seq
	if s.fresh("u-2", start) {
		t.Error("a read that raced a full flush stays cacheable")
	}
}

func openIn(t *testing.T, dsn string) *Source {
	t.Helper()
	s, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func restricted(dsn, role string) string {
	scheme, rest, _ := strings.Cut(dsn, "://")
	_, host, _ := strings.Cut(rest, "@")
	return scheme + "://" + role + ":p@" + host
}
