package pg

import (
	"errors"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/tenantstest"
)

func TestContract(t *testing.T) {
	tenantstest.Run(t, func(t *testing.T) tenants.Source { return openIn(t, tenantstest.PGSchema(t)) })
}

func TestAWriteOnOneNodeReachesTheOtherNodesCache(t *testing.T) {
	dsn := tenantstest.PGSchema(t)
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
	dsn := tenantstest.PGSchema(t)
	s := openIn(t, dsn)
	ctx := t.Context()
	if _, err := s.Put(ctx, tenantstest.Record("u-1", "warm", 0, "")); err != nil {
		t.Fatalf("put: %v", err)
	}
	cached := func() bool {
		_, err := s.Resolve(ctx, tenantstest.Sum("warm"))
		s.mu.RLock()
		defer s.mu.RUnlock()
		return err == nil && s.byToken[tenantstest.Sum("warm")] == "u-1"
	}
	if !tenantstest.Eventually(t, cached) {
		t.Fatal("u-1 never landed in the cache")
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

func openIn(t *testing.T, dsn string) *Source {
	t.Helper()
	s, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
