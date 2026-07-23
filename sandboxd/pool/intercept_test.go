package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var interceptKey = types.PoolKey{Template: "rt:24.04", Net: types.NetNone, Size: types.SizeSmall, Engine: types.EngineCH}

func interceptPolicy() *egress.Policy {
	return &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh", Intercept: true}}}
}

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
	fp, err := os.ReadFile(final + caSidecarSuffix)
	if err != nil {
		t.Fatalf("read ca sidecar: %v", err)
	}
	if string(fp) != m.egressCA.Fingerprint() {
		t.Errorf("sidecar fingerprint = %q, want %q", fp, m.egressCA.Fingerprint())
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
	if _, err := os.Stat(final + caSidecarSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ca sidecar present for a plain pool: %v", err)
	}
}

func TestGoldenCAMatchRebuildsOnMismatch(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: interceptKey, Warm: 1, Egress: interceptPolicy()})
	final := filepath.Join(m.goldensDir(), interceptKey.Hash())
	if err := os.MkdirAll(final, 0o750); err != nil {
		t.Fatalf("stage golden: %v", err)
	}
	if m.goldenCAMatches(final, true) {
		t.Error("adopted an intercept golden with no CA sidecar; want rebuild")
	}
	if err := os.WriteFile(final+caSidecarSuffix, []byte("deadbeef"), 0o644); err != nil {
		t.Fatalf("write stale sidecar: %v", err)
	}
	if m.goldenCAMatches(final, true) {
		t.Error("adopted an intercept golden with a stale CA fingerprint; want rebuild")
	}
	if err := os.WriteFile(final+caSidecarSuffix, []byte(m.egressCA.Fingerprint()), 0o644); err != nil {
		t.Fatalf("write matching sidecar: %v", err)
	}
	if !m.goldenCAMatches(final, true) {
		t.Error("rejected an intercept golden whose CA fingerprint matches")
	}
	if m.goldenCAMatches(final, false) {
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
	// The cluster root is shared, so an interception sandbox's disk carries no
	// node-private material: promote/checkpoint are unrestricted.
	if _, err := m.Promote(t.Context(), sb.ID, "tok", "tpl:x", ""); err != nil {
		t.Errorf("Promote of an interception-pool sandbox: %v, want success", err)
	}
}
