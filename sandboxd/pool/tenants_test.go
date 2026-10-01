package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeTenantAuthenticatesAndIsCapped(t *testing.T) {
	m := tenantManager(t, t.TempDir())
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "acme", Token: "acme-tok", MaxClaims: 1}); err != nil {
		t.Fatalf("PutTenant: %v", err)
	}
	if name, ok := m.TenantByToken("acme-tok"); !ok || name != "acme" {
		t.Fatalf("TenantByToken = %q, %v; want acme without a restart", name, ok)
	}
	claim := func() error {
		_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
		return err
	}
	if err := claim(); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := claim(); !errors.Is(err, ErrQuota) {
		t.Fatalf("claim past the cap: %v, want ErrQuota", err)
	}
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "acme", MaxClaims: 2}); err != nil {
		t.Fatalf("raise the cap: %v", err)
	}
	if _, ok := m.TenantByToken("acme-tok"); !ok {
		t.Fatal("a put without a token dropped the stored one")
	}
	if err := claim(); err != nil {
		t.Fatalf("claim under the raised cap: %v", err)
	}
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "acme", MaxClaims: 1}); err != nil {
		t.Fatalf("lower the cap: %v", err)
	}
	if err := claim(); !errors.Is(err, ErrQuota) {
		t.Errorf("claim under a lowered cap: %v, want ErrQuota", err)
	}
	if got := m.Sandboxes("acme", "", nil); len(got) != 2 {
		t.Errorf("a lowered cap left %d claims, want the 2 live ones kept", len(got))
	}
}

func TestRotatedTenantTokenStopsTheOldOne(t *testing.T) {
	m := tenantManager(t, t.TempDir(), config.TenantSpec{Name: "acme", Token: "old"})
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "acme", Token: "new"}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, ok := m.TenantByToken("old"); ok {
		t.Error("the old token still authenticates")
	}
	if name, ok := m.TenantByToken("new"); !ok || name != "acme" {
		t.Errorf("the new token resolves to %q, %v", name, ok)
	}
}

func TestRemovedTenantRunsToItsDeadlineButCannotExtend(t *testing.T) {
	m := tenantManager(t, t.TempDir(), config.TenantSpec{Name: "acme", Token: "acme-tok"})
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme", OnExpire: types.ExpireArchive})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if _, ok := m.TenantByToken("acme-tok"); ok {
		t.Error("a removed tenant's token still authenticates")
	}
	if got := m.Sandboxes("", "", nil); len(got) != 1 || got[0].Tenant != "acme" {
		t.Fatalf("root listing %+v, want the claim kept under acme", got)
	}
	infos, _ := m.Tenants()
	if !slices.Contains(infos, TenantInfo{Name: "acme", Claims: 1, Removed: true}) {
		t.Errorf("Tenants %+v, want acme listed as removed with its claim", infos)
	}
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("renew by the sandbox token: %v, want ErrTenantRemoved", err)
	}
	if _, err := m.Renew(t.Context(), sb.ID, Cred{Operator: true}, time.Hour, ""); err != nil {
		t.Errorf("renew by the operator: %v", err)
	}
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	m.mu.Lock()
	_, live := m.claimed[sb.ID]
	m.mu.Unlock()
	if live {
		t.Error("an expired claim of a removed tenant was archived instead of destroyed")
	}
}

func TestRemovedTenantsClaimIsNeitherArchivedNorWokenNorForked(t *testing.T) {
	m, err := NewManager(t.Context(), &config.Config{
		DataDir: t.TempDir(), APIToken: "root",
		Tenants: []config.TenantSpec{{Name: "acme", Token: "acme-tok"}},
		Pools:   []config.PoolSpec{archivePool(0)},
	}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	claim := func() *types.Sandbox {
		t.Helper()
		sb, claimErr := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
		if claimErr != nil {
			t.Fatalf("claim: %v", claimErr)
		}
		return sb
	}
	archived, idle, running := claim(), claim(), claim()
	mustArchive(t, m, archived)
	if err = m.Hibernate(t.Context(), idle.ID, Cred{Token: idle.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if err = m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}

	backdate(m, idle, 5*time.Second)
	m.archiveOnce(t.Context())
	m.mu.Lock()
	_, marked := m.archiving[idle.ID]
	ck := idle.ArchiveCk
	m.mu.Unlock()
	if marked || ck != "" {
		t.Errorf("the idle sweep archived a removed tenant's claim: marked %t archive %q", marked, ck)
	}
	if _, _, err = m.WakeAgentSocket(t.Context(), archived.ID, archived.Token); !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("wake from the archive: %v, want ErrTenantRemoved", err)
	}
	if _, err = m.Fork(t.Context(), running.ID, Cred{Token: running.Token}, 1, time.Hour, "", ""); !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("fork: %v, want ErrTenantRemoved", err)
	}
}

func TestTenantSetSurvivesARestartAndOverridesTheConfig(t *testing.T) {
	dir := t.TempDir()
	m := tenantManager(t, dir, config.TenantSpec{Name: "acme", Token: "acme-tok"})
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "beta", Token: "beta-tok", MaxClaims: 3}); err != nil {
		t.Fatalf("PutTenant: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, tenantsFileName))
	if err != nil {
		t.Fatalf("read tenants file: %v", err)
	}
	if strings.Contains(string(raw), "beta-tok") || strings.Contains(string(raw), "acme-tok") {
		t.Fatalf("%s holds a plaintext token: %s", tenantsFileName, raw)
	}
	restarted := tenantManager(t, dir, config.TenantSpec{Name: "acme", Token: "acme-tok"}, config.TenantSpec{Name: "gamma", Token: "gamma-tok"})
	if name, ok := restarted.TenantByToken("beta-tok"); !ok || name != "beta" {
		t.Errorf("after restart beta resolves to %q, %v", name, ok)
	}
	if _, ok := restarted.TenantByToken("gamma-tok"); ok {
		t.Error("a tenant added to config.json after an API apply authenticates")
	}
	if err := os.Remove(filepath.Join(dir, tenantsFileName)); err != nil {
		t.Fatalf("remove tenants file: %v", err)
	}
	configOwned := tenantManager(t, dir, config.TenantSpec{Name: "gamma", Token: "gamma-tok"})
	if _, ok := configOwned.TenantByToken("gamma-tok"); !ok {
		t.Error("deleting the tenants file did not return the node to config-owned tenants")
	}
}

func TestConcurrentTenantPutsKeepEveryTenant(t *testing.T) {
	m := tenantManager(t, t.TempDir())
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			if err := m.PutTenant(t.Context(), config.TenantSpec{Name: fmt.Sprintf("u-%d", i), Token: fmt.Sprintf("tok-%d", i)}); err != nil {
				t.Errorf("PutTenant %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if n := len(m.TenantRecords()); n != 32 {
		t.Errorf("%d tenants after 32 concurrent puts, want 32", n)
	}
}

func TestTenantChangesAreValidatedAndAudited(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dir, APIToken: "root", AuditLog: true, Tenants: []config.TenantSpec{{Name: "acme", Token: "acme-tok"}}}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	for name, spec := range map[string]config.TenantSpec{
		"bad name":     {Name: "-bad", Token: "x"},
		"root scope":   {Name: types.TemplateRootScope, Token: "x"},
		"no token":     {Name: "new"},
		"root token":   {Name: "new", Token: "root"},
		"reused token": {Name: "new", Token: "acme-tok"},
		"egress":       {Name: "new", Token: "x", Egress: &egress.Policy{}},
		"upstream":     {Name: "new", Token: "x", EgressUpstreamEnv: "UP"},
		"negative cap": {Name: "acme", MaxClaims: -1},
	} {
		if err = m.PutTenant(t.Context(), spec); !errors.Is(err, ErrBadTenant) {
			t.Errorf("%s: %v, want ErrBadTenant", name, err)
		}
	}
	if err = m.DeleteTenant(t.Context(), "nobody"); !errors.Is(err, ErrUnknownTenant) {
		t.Errorf("delete an unknown tenant: %v, want ErrUnknownTenant", err)
	}
	if err = m.SetTenants(t.Context(), []config.TenantSpec{{Name: "acme", MaxClaims: 2}, {Name: "beta", Token: "beta-tok"}}); err != nil {
		t.Fatalf("SetTenants: %v", err)
	}
	if err = m.DeleteTenant(t.Context(), "beta"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	log := string(raw)
	for _, want := range []string{`"op":"tenants","added":["beta"],"changed":["acme"]`, `"op":"tenants","removed":["beta"]`} {
		if !strings.Contains(log, want) {
			t.Errorf("audit lacks %s:\n%s", want, log)
		}
	}
	if strings.Contains(log, "tok") {
		t.Errorf("audit names a token:\n%s", log)
	}

	open, err := NewManager(t.Context(), &config.Config{DataDir: t.TempDir()}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("setup open manager: %v", err)
	}
	if err := open.PutTenant(t.Context(), config.TenantSpec{Name: "acme", Token: "t"}); !errors.Is(err, ErrBadTenant) {
		t.Errorf("tenant on a node without api_token: %v, want ErrBadTenant", err)
	}
}

func tenantManager(t *testing.T, dir string, tenants ...config.TenantSpec) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dir, APIToken: "root", Tenants: tenants}, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func setTenantCaps(t *testing.T, m *Manager, caps map[string]int) {
	t.Helper()
	records := make([]config.TenantRecord, 0, len(caps))
	for name, limit := range caps {
		records = append(records, config.TenantRecord{Name: name, TokenSHA256: config.TokenSHA256("tok-" + name), MaxClaims: limit})
	}
	set, err := newTenantSet(records, "")
	if err != nil {
		t.Fatalf("tenant set: %v", err)
	}
	m.tenants.Store(set)
}
