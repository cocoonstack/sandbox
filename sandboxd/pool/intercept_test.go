package pool

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var interceptKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall}

func TestGoldenBuildInstallsCAForInterceptPool(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	if m.egressCA == nil {
		t.Fatal("egress CA not loaded for an intercept pool")
	}
	final := filepath.Join(m.goldensDir(), interceptKey.Hash())
	if err := m.buildGoldenSteps(t.Context(), interceptKey, "sbx-gb", "snap", final); err != nil {
		t.Fatalf("buildGoldenSteps: %v", err)
	}
	if len(eng.caInstalls) != 1 {
		t.Fatalf("InstallCACert calls = %d, want 1", len(eng.caInstalls))
	}
	stamp, err := os.ReadFile(final + goldenStampSuffix)
	if err != nil {
		t.Fatalf("read golden stamp: %v", err)
	}
	if want := m.goldenStamp(interceptKey, true, nil); string(stamp) != want {
		t.Errorf("stamp = %q, want %q", stamp, want)
	}
}

func TestGoldenBuildSkipsCAForPlainPool(t *testing.T) {
	eng := newFakeEngine()
	plain := &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: plain})
	if m.egressCA != nil {
		t.Error("egress CA loaded though no pool intercepts")
	}
	final := filepath.Join(m.goldensDir(), interceptKey.Hash())
	if err := m.buildGoldenSteps(t.Context(), interceptKey, "sbx-gb", "snap", final); err != nil {
		t.Fatalf("buildGoldenSteps: %v", err)
	}
	if len(eng.caInstalls) != 0 {
		t.Errorf("InstallCACert called %d times for a plain pool", len(eng.caInstalls))
	}
	if stamp, err := os.ReadFile(final + goldenStampSuffix); err != nil || string(stamp) != m.goldenStamp(interceptKey, false, nil) {
		t.Errorf("stamp = %q (%v), want one without a CA fingerprint", stamp, err)
	}
}

func TestGoldenCAStampRebuildsOnMismatch(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	p := m.pools[interceptKey]
	final := filepath.Join(m.goldensDir(), interceptKey.Hash())
	if err := os.MkdirAll(final, 0o750); err != nil {
		t.Fatalf("stage golden: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted an intercept golden with no stamp; want rebuild")
	}
	baked := m.goldenStamp(interceptKey, true, nil)
	stale := strings.Replace(baked, m.egressCA.Fingerprint(), "deadbeef", 1)
	if err := os.WriteFile(final+goldenStampSuffix, []byte(stale), 0o644); err != nil {
		t.Fatalf("write stale stamp: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted an intercept golden with a stale CA fingerprint; want rebuild")
	}
	if err := os.WriteFile(final+goldenStampSuffix, []byte(baked), 0o644); err != nil {
		t.Fatalf("write matching stamp: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != final {
		t.Error("rejected an intercept golden whose CA fingerprint matches")
	}
	p.goldenDir = ""
	m.poolEgress[interceptKey] = &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh"}}}
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted a CA-baked golden for a now-plain pool; want rebuild")
	}
}

func TestColdProvisionInstallsCAForInterceptPool(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	sb, err := m.provision(t.Context(), interceptKey, "")
	if err != nil {
		t.Fatalf("cold provision: %v", err)
	}
	m.destroy(t.Context(), sb.VMName)
	if n := len(eng.caInstalls); n != 1 {
		t.Errorf("InstallCACert calls = %d, want 1 (pre-golden cold claim must trust the root)", n)
	}
}

func TestColdProvisionSkipsCAForPlainPool(t *testing.T) {
	eng := newFakeEngine()
	plain := &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh"}}}
	m := egressManager(t, eng, config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: plain})
	sb, err := m.provision(t.Context(), interceptKey, "")
	if err != nil {
		t.Fatalf("cold provision: %v", err)
	}
	m.destroy(t.Context(), sb.VMName)
	if n := len(eng.caInstalls); n != 0 {
		t.Errorf("InstallCACert called %d times for a plain pool", n)
	}
}

func TestColdProvisionFailsClosedOnCAInstallError(t *testing.T) {
	eng := newFakeEngine()
	eng.installCAErr = errors.New("silkd down")
	m := egressManager(t, eng, config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	if _, err := m.provision(t.Context(), interceptKey, ""); err == nil {
		t.Error("cold provision succeeded though the guest never got the root; want fail-closed")
	}
}

func TestInterceptPoolAllowsPromote(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	sb := &types.Sandbox{ID: "sb_i", Key: interceptKey, Token: "tok", VMName: "sbx-i"}
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()

	if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: "tok"}, "tpl:x", ""); err != nil {
		t.Errorf("Promote of an interception-pool sandbox: %v, want success", err)
	}
}

func interceptPolicy() *egress.Policy {
	return &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh", Intercept: true}}}
}
