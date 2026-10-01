// Package tenantstest is the contract every tenants.Source passes.
package tenantstest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

const (
	tenantOne = "u-1"
	classDesk = "desk"
)

// Opener returns an empty source; each call is a fresh set.
type Opener func(t *testing.T) tenants.Source

// Run checks the behavior every backend shares.
func Run(t *testing.T, open Opener) {
	t.Run("put resolve peek", func(t *testing.T) { testPutResolvePeek(t, open(t)) })
	t.Run("keep and rotate tokens", func(t *testing.T) { testTokens(t, open(t)) })
	t.Run("list pages by name", func(t *testing.T) { testList(t, open(t)) })
	t.Run("delete and replace", func(t *testing.T) { testDeleteReplace(t, open(t)) })
}

// Record builds a tenant record whose token is token.
func Record(name, token string, maxClaims int, class string) config.TenantRecord {
	return config.TenantRecord{Name: name, TokenSHA256: config.TokenSHA256(token), MaxClaims: maxClaims, EgressClass: class}
}

// Sum is the SHA-256 Resolve takes for token.
func Sum(token string) [sha256.Size]byte {
	return sha256.Sum256([]byte(token))
}

// PGSchema returns a DSN on SANDBOXD_TEST_PG_URL whose search_path is a new schema dropped after the test, or skips without the env.
func PGSchema(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SANDBOXD_TEST_PG_URL")
	if url == "" {
		t.Skip("SANDBOXD_TEST_PG_URL unset")
	}
	raw := make([]byte, 6)
	_, _ = rand.Read(raw)
	name := "t_" + hex.EncodeToString(raw)
	exec := func(ctx context.Context, sql string) error {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		_, err = conn.Exec(ctx, sql)
		return err
	}
	if err := exec(t.Context(), "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _ = exec(context.Background(), "DROP SCHEMA "+name+" CASCADE") })
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	return url + sep + "search_path=" + name
}

// Eventually polls cond for up to five seconds; a cached source settles its Peek in the background.
func Eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	for range 100 {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func testPutResolvePeek(t *testing.T, s tenants.Source) {
	ctx := t.Context()
	if _, err := s.Put(ctx, config.TenantRecord{Name: tenantOne}); !errors.Is(err, tenants.ErrInvalid) {
		t.Errorf("a new tenant without a token: %v, want ErrInvalid", err)
	}
	c, err := s.Put(ctx, Record(tenantOne, "t1", 2, classDesk))
	if err != nil || !slices.Equal(c.Added, []string{tenantOne}) {
		t.Fatalf("put u-1: %+v %v, want added", c, err)
	}
	if c, err = s.Put(ctx, Record(tenantOne, "t1", 2, classDesk)); err != nil || !c.Empty() {
		t.Errorf("an identical put: %+v %v, want no change", c, err)
	}
	r, err := s.Resolve(ctx, Sum("t1"))
	if err != nil || r.Name != tenantOne || r.MaxClaims != 2 || r.EgressClass != classDesk {
		t.Errorf("resolve t1: %+v %v", r, err)
	}
	if _, err = s.Resolve(ctx, Sum("nobody")); !errors.Is(err, tenants.ErrUnknown) {
		t.Errorf("resolve an unknown token: %v, want ErrUnknown", err)
	}
	if !Eventually(t, func() bool { _, p := s.Peek(tenantOne); return p == tenants.Present }) {
		t.Error("peek u-1 never answered present")
	}
	if !Eventually(t, func() bool { _, p := s.Peek("u-9"); return p == tenants.Absent }) {
		t.Error("peek of a name never put never answered absent")
	}
	if _, err = s.Put(ctx, Record("u-2", "t1", 0, "")); !errors.Is(err, tenants.ErrInvalid) {
		t.Errorf("a second tenant reusing t1: %v, want ErrInvalid", err)
	}
	if _, err = s.Put(ctx, Record("u-2", "t2", 0, classDesk)); err != nil {
		t.Fatalf("put u-2: %v", err)
	}
	classes, err := s.Classes(ctx)
	if err != nil || classes[classDesk] != 2 || len(classes) != 1 {
		t.Errorf("classes %v %v, want desk:2", classes, err)
	}
}

func testTokens(t *testing.T, s tenants.Source) {
	ctx := t.Context()
	if _, err := s.Put(ctx, Record(tenantOne, "old", 1, "")); err != nil {
		t.Fatalf("put: %v", err)
	}
	c, err := s.Put(ctx, config.TenantRecord{Name: tenantOne, MaxClaims: 5, EgressClass: classDesk})
	if err != nil || !slices.Equal(c.Changed, []string{tenantOne}) {
		t.Fatalf("change u-1 keeping its token: %+v %v", c, err)
	}
	if r, rerr := s.Resolve(ctx, Sum("old")); rerr != nil || r.MaxClaims != 5 || r.EgressClass != classDesk {
		t.Errorf("the kept token after a change: %+v %v, want the new fields", r, rerr)
	}
	if c, err = s.Put(ctx, config.TenantRecord{Name: tenantOne, MaxClaims: 5, EgressClass: classDesk}); err != nil || !c.Empty() {
		t.Errorf("an identical keep-token put: %+v %v, want no change", c, err)
	}
	if _, err = s.Put(ctx, Record(tenantOne, "new", 5, classDesk)); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err = s.Resolve(ctx, Sum("old")); !errors.Is(err, tenants.ErrUnknown) {
		t.Errorf("the rotated-out token: %v, want ErrUnknown", err)
	}
	if r, rerr := s.Resolve(ctx, Sum("new")); rerr != nil || r.Name != tenantOne {
		t.Errorf("the new token: %+v %v", r, rerr)
	}
}

func testList(t *testing.T, s tenants.Source) {
	ctx := t.Context()
	for i := range 5 {
		if _, err := s.Put(ctx, Record(fmt.Sprintf("u-%d", i), fmt.Sprintf("t%d", i), i, "")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	var names []string
	after := ""
	for range 4 {
		page, err := s.List(ctx, after, 2)
		if err != nil {
			t.Fatalf("list after %q: %v", after, err)
		}
		for _, r := range page {
			if r.TokenSHA256 != "" {
				t.Errorf("list returned %s's token hash", r.Name)
			}
			names = append(names, r.Name)
		}
		if len(page) < 2 {
			break
		}
		after = page[len(page)-1].Name
	}
	if want := []string{"u-0", tenantOne, "u-2", "u-3", "u-4"}; !slices.Equal(names, want) {
		t.Errorf("paged names %v, want %v", names, want)
	}
}

func testDeleteReplace(t *testing.T, s tenants.Source) {
	ctx := t.Context()
	for _, r := range []config.TenantRecord{Record("a", "ta", 0, ""), Record("b", "tb", 0, ""), Record("c", "tc", 0, "")} {
		if _, err := s.Put(ctx, r); err != nil {
			t.Fatalf("put %s: %v", r.Name, err)
		}
	}
	c, err := s.Delete(ctx, "b")
	if err != nil || !slices.Equal(c.Removed, []string{"b"}) {
		t.Fatalf("delete b: %+v %v", c, err)
	}
	if _, err = s.Delete(ctx, "b"); !errors.Is(err, tenants.ErrUnknown) {
		t.Errorf("delete b again: %v, want ErrUnknown", err)
	}
	if _, err = s.Resolve(ctx, Sum("tb")); !errors.Is(err, tenants.ErrUnknown) {
		t.Errorf("a deleted tenant's token: %v, want ErrUnknown", err)
	}
	if !Eventually(t, func() bool { _, p := s.Peek("b"); return p == tenants.Absent }) {
		t.Error("peek of a deleted tenant never answered absent")
	}
	if _, err = s.Replace(ctx, []config.TenantRecord{{Name: "a"}, {Name: "fresh"}}); !errors.Is(err, tenants.ErrInvalid) {
		t.Errorf("replace naming a new tenant without a token: %v, want ErrInvalid", err)
	}
	if _, err = s.Resolve(ctx, Sum("tc")); err != nil {
		t.Errorf("a refused replace changed the set: %v", err)
	}
	c, err = s.Replace(ctx, []config.TenantRecord{{Name: "a", MaxClaims: 3}, Record("d", "td", 0, "")})
	if err != nil || !slices.Equal(c.Added, []string{"d"}) || !slices.Equal(c.Changed, []string{"a"}) || !slices.Equal(c.Removed, []string{"c"}) {
		t.Fatalf("replace: %+v %v, want d added, a changed, c removed", c, err)
	}
	if r, rerr := s.Resolve(ctx, Sum("ta")); rerr != nil || r.MaxClaims != 3 {
		t.Errorf("a's kept token after the replace: %+v %v", r, rerr)
	}
	if _, err = s.Resolve(ctx, Sum("tc")); !errors.Is(err, tenants.ErrUnknown) {
		t.Errorf("a tenant the replace dropped: %v, want ErrUnknown", err)
	}
}
