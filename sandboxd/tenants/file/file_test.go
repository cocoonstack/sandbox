package file

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/tenantstest"
)

func TestContract(t *testing.T) {
	tenantstest.Run(t, func(t *testing.T) tenants.Source {
		s, err := Open(t.Context(), t.TempDir(), config.TokenSHA256("root"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return s
	})
}

func TestTheSetSurvivesAReopen(t *testing.T) {
	dir, root := t.TempDir(), config.TokenSHA256("root")
	s, err := Open(t.Context(), dir, root)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err = s.Resolve(t.Context(), tenantstest.Sum("beta-tok")); !errors.Is(err, tenants.ErrUnknown) {
		t.Fatalf("a node without tenants.json: %v, want no tenants", err)
	}
	if _, err = s.Put(t.Context(), tenantstest.Record("beta", "beta-tok", 3, "desk")); err != nil {
		t.Fatalf("put: %v", err)
	}
	reopened, err := Open(t.Context(), dir, root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if r, rerr := reopened.Resolve(t.Context(), tenantstest.Sum("beta-tok")); rerr != nil || r.EgressClass != "desk" || r.MaxClaims != 3 {
		t.Errorf("beta after a reopen: %+v %v", r, rerr)
	}
	if reopened.Digest() != s.Digest() {
		t.Error("the reopened set's digest differs from the one that wrote it")
	}
	info, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("tenants file %v %v, want mode 0600", info, err)
	}
}

func TestATenantsFileNeedsTheRootToken(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(t.Context(), dir, config.TokenSHA256("root"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err = s.Put(t.Context(), tenantstest.Record("acme", "t", 0, "")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err = Open(t.Context(), dir, ""); err == nil {
		t.Error("a tenants file on a node without api_token opened")
	}
	if _, err = s.Put(t.Context(), tenantstest.Record("beta", "root", 0, "")); !errors.Is(err, tenants.ErrInvalid) {
		t.Errorf("a tenant holding the root token: %v, want ErrInvalid", err)
	}
}
