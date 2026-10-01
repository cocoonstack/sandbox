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
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/tenantstest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRuntimeTenantAuthenticatesAndIsCapped(t *testing.T) {
	m := tenantManager(t, t.TempDir())
	if err := m.PutTenant(t.Context(), config.TenantSpec{Name: "acme", Token: "acme-tok", MaxClaims: 1}); err != nil {
		t.Fatalf("PutTenant: %v", err)
	}
	if name, ok := tenantOf(t, m, "acme-tok"); !ok || name != "acme" {
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
	if _, ok := tenantOf(t, m, "acme-tok"); !ok {
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
	if _, ok := tenantOf(t, m, "old"); ok {
		t.Error("the old token still authenticates")
	}
	if name, ok := tenantOf(t, m, "new"); !ok || name != "acme" {
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
	if _, ok := tenantOf(t, m, "acme-tok"); ok {
		t.Error("a removed tenant's token still authenticates")
	}
	if got := m.Sandboxes("", "", nil); len(got) != 1 || got[0].Tenant != "acme" {
		t.Fatalf("root listing %+v, want the claim kept under acme", got)
	}
	infos, _ := listTenants(t, m)
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
	raw, err := os.ReadFile(filepath.Join(dir, "tenants.json"))
	if err != nil {
		t.Fatalf("read tenants file: %v", err)
	}
	if strings.Contains(string(raw), "beta-tok") || strings.Contains(string(raw), "acme-tok") {
		t.Fatalf("tenants.json holds a plaintext token: %s", raw)
	}
	restarted := tenantManager(t, dir, config.TenantSpec{Name: "acme", Token: "acme-tok"}, config.TenantSpec{Name: "gamma", Token: "gamma-tok"})
	if name, ok := tenantOf(t, restarted, "beta-tok"); !ok || name != "beta" {
		t.Errorf("after restart beta resolves to %q, %v", name, ok)
	}
	if _, ok := tenantOf(t, restarted, "gamma-tok"); ok {
		t.Error("a tenant added to config.json after an API apply authenticates")
	}
	if err := os.Remove(filepath.Join(dir, "tenants.json")); err != nil {
		t.Fatalf("remove tenants file: %v", err)
	}
	configOwned := tenantManager(t, dir, config.TenantSpec{Name: "gamma", Token: "gamma-tok"})
	if _, ok := tenantOf(t, configOwned, "gamma-tok"); !ok {
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
		"bad name":      {Name: "-bad", Token: "x"},
		"root scope":    {Name: types.TemplateRootScope, Token: "x"},
		"no token":      {Name: "new"},
		"root token":    {Name: "new", Token: "root"},
		"reused token":  {Name: "new", Token: "acme-tok"},
		"unknown class": {Name: "new", Token: "x", EgressClass: "nope"},
		"negative cap":  {Name: "acme", MaxClaims: -1},
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

func TestARuntimeTenantTakesItsEgressClassWithoutARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, APIToken: "root", EgressClasses: []config.EgressClass{{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "b.test"}}}}}}
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	m, err := NewManager(t.Context(), cfg, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	for _, spec := range []config.TenantSpec{{Name: "u-1", Token: "u1-tok", EgressClass: "desk"}, {Name: "u-2", Token: "u2-tok"}} {
		if err = m.PutTenant(t.Context(), spec); err != nil {
			t.Fatalf("PutTenant %s: %v", spec.Name, err)
		}
	}
	unpooled := types.PoolKey{Template: "promoted-name", Net: types.NetNone, Size: types.SizeSmall}
	claimAs := func(token string) (*types.Sandbox, error) {
		r, rerr := m.TenantByToken(t.Context(), token)
		if rerr != nil {
			return nil, rerr
		}
		return m.ClaimProvision(t.Context(), unpooled, ClaimOptions{TTL: time.Hour, Tenant: r.Name, EgressClass: r.EgressClass})
	}
	classed, err := claimAs("u1-tok")
	if err != nil {
		t.Fatalf("claim u-1: %v", err)
	}
	eval, ok := m.effectivePolicy(m.view.Load(), classed)
	if !ok || classed.EgressClass != "desk" || dtoOf(classed).EgressClass != "desk" {
		t.Fatalf("u-1 claim class %q armed %v, want the desk class stamped and journaled", classed.EgressClass, ok)
	}
	if _, d := eval.Eval("b.test", "GET", 443); d != egress.DecisionAllow {
		t.Error("the class's allowed host is refused")
	}
	if _, d := eval.Eval("a.test", "GET", 443); d == egress.DecisionAllow {
		t.Error("a host outside the class is allowed")
	}
	live := newLivePolicy(m, classed, m.view.Load(), eval)
	next := *m.cfg
	next.EgressClasses = []config.EgressClass{{Name: "desk", Egress: &egress.Policy{Allow: []egress.Rule{{Host: "a.test"}}}}}
	if _, err = m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload the class: %v", err)
	}
	if _, d := live.Eval("a.test", "GET", 443); d != egress.DecisionAllow {
		t.Error("a reload that widens the class did not reach the live claim")
	}
	if _, d := live.Eval("b.test", "GET", 443); d == egress.DecisionAllow {
		t.Error("a reload that narrows the class did not reach the live claim")
	}
	bare, err := claimAs("u2-tok")
	if err != nil {
		t.Fatalf("claim u-2: %v", err)
	}
	if _, ok = m.effectivePolicy(m.view.Load(), bare); ok {
		t.Error("a tenant without a class gained egress")
	}

	restarted, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t))
	if err != nil {
		t.Fatalf("restart manager: %v", err)
	}
	if r, _ := restarted.tenants.Peek("u-1"); r.EgressClass != "desk" {
		t.Errorf("after a restart u-1 has class %q, want desk from tenants.json", r.EgressClass)
	}

	if err = m.PutTenant(t.Context(), config.TenantSpec{Name: "u-1"}); err != nil {
		t.Fatalf("drop u-1's class: %v", err)
	}
	later, err := claimAs("u1-tok")
	if err != nil {
		t.Fatalf("second claim u-1: %v", err)
	}
	if classed.EgressClass != "desk" || later.EgressClass != "" {
		t.Errorf("classes live %q new %q: a class change reaches only claims made after it", classed.EgressClass, later.EgressClass)
	}
	infos, _ := listTenants(t, m)
	if i := slices.IndexFunc(infos, func(ti TenantInfo) bool { return ti.Name == "u-1" }); i < 0 || infos[i].EgressClass != "" {
		t.Errorf("tenant list %+v, want u-1 without a class", infos)
	}
}

func TestTwoNodesOnOneMetaStoreShareTheTenantSet(t *testing.T) {
	t.Setenv("SANDBOX_TEST_DSN", tenantstest.PGSchema(t))
	node := func() *Manager {
		cfg := &config.Config{DataDir: t.TempDir(), APIToken: "root", MetaStore: &config.MetaStoreConfig{Kind: "pg", DSNEnv: "SANDBOX_TEST_DSN"}}
		m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t))
		if err != nil {
			t.Fatalf("setup manager: %v", err)
		}
		t.Cleanup(func() { _ = m.tenants.Close() })
		return m
	}
	a, b := node(), node()
	if err := a.PutTenant(t.Context(), config.TenantSpec{Name: "u-1", Token: "u1-tok", MaxClaims: 1}); err != nil {
		t.Fatalf("put on a: %v", err)
	}
	if name, ok := tenantOf(t, b, "u1-tok"); !ok || name != "u-1" {
		t.Errorf("b resolves a's tenant: %q %v, want u-1 without a fan-out", name, ok)
	}
	if err := b.DeleteTenant(t.Context(), "u-1"); err != nil {
		t.Fatalf("delete on b: %v", err)
	}
	if !tenantstest.Eventually(t, func() bool { _, err := a.TenantByToken(t.Context(), "u1-tok"); return errors.Is(err, ErrUnknownTenant) }) {
		t.Error("a still authenticates a tenant b deleted")
	}
	if a.TenantRecords() != nil {
		t.Error("a shared tenant set entered the cluster digest")
	}
	if _, err := os.Stat(filepath.Join(a.dataDir, "tenants.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a node on meta_store wrote tenants.json: %v", err)
	}
}

func tenantOf(t *testing.T, m *Manager, token string) (string, bool) {
	t.Helper()
	r, err := m.TenantByToken(t.Context(), token)
	if err != nil {
		return "", false
	}
	return r.Name, true
}

func listTenants(t *testing.T, m *Manager) ([]TenantInfo, string) {
	t.Helper()
	page, err := m.Tenants(t.Context(), "", 1000)
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	return page.Tenants, page.Digest
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
	src, err := file.Open(t.Context(), t.TempDir(), records, "")
	if err != nil {
		t.Fatalf("tenant set: %v", err)
	}
	m.tenants = src
}
