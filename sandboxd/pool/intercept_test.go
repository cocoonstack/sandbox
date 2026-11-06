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

func TestGoldenBuildInstallsCAOnlyForAnInterceptPool(t *testing.T) {
	tests := []struct {
		name     string
		policy   *egress.Policy
		caLoaded bool
		installs int
	}{
		{"intercept pool", interceptPolicy(), true, 1},
		{"plain pool", plainPolicy(), false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: tt.policy})
			if loaded := m.out.CAFingerprint() != ""; loaded != tt.caLoaded {
				t.Fatalf("egress CA loaded = %v, want %v", loaded, tt.caLoaded)
			}
			final := buildGoldenAt(t, m, testKey, "")
			if len(eng.caInstalls) != tt.installs {
				t.Fatalf("InstallCACert calls = %d, want %d", len(eng.caInstalls), tt.installs)
			}
			stamp, err := os.ReadFile(final + goldenStampSuffix)
			if err != nil {
				t.Fatalf("read golden stamp: %v", err)
			}
			if want := m.goldenStamp(m.view.Load(), testKey, ""); string(stamp) != want {
				t.Errorf("stamp = %q, want %q", stamp, want)
			}
		})
	}
}

func TestGoldenCAStampRebuildsOnMismatch(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()})
	p := m.pools[testKey]
	final := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(final, 0o750); err != nil {
		t.Fatalf("stage golden: %v", err)
	}
	m.adoptGolden(p, "")
	if p.goldenDir != "" {
		t.Error("adopted an intercept golden with no stamp; want rebuild")
	}
	baked := m.goldenStamp(m.view.Load(), testKey, "")
	stale := strings.Replace(baked, m.out.CAFingerprint(), "deadbeef", 1)
	if err := os.WriteFile(final+goldenStampSuffix, []byte(stale), 0o644); err != nil {
		t.Fatalf("write stale stamp: %v", err)
	}
	m.adoptGolden(p, "")
	if p.goldenDir != "" {
		t.Error("adopted an intercept golden with a stale CA fingerprint; want rebuild")
	}
	if err := os.WriteFile(final+goldenStampSuffix, []byte(baked), 0o644); err != nil {
		t.Fatalf("write matching stamp: %v", err)
	}
	m.adoptGolden(p, "")
	if p.goldenDir != final {
		t.Error("rejected an intercept golden whose CA fingerprint matches")
	}
	p.goldenDir = ""
	editEgress(t, m, func(cfg *config.Config) {
		cfg.Pools = []config.PoolSpec{{PoolKey: testKey, Warm: 1, Egress: plainPolicy()}}
	})
	m.adoptGolden(p, "")
	if p.goldenDir != "" {
		t.Error("adopted a CA-baked golden for a now-plain pool; want rebuild")
	}
}

func TestColdProvisionInstallsCAOnlyForAnInterceptPool(t *testing.T) {
	tests := []struct {
		name     string
		policy   *egress.Policy
		installs int
	}{
		{"intercept pool", interceptPolicy(), 1},
		{"plain pool", plainPolicy(), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: tt.policy})
			sb, err := m.provision(t.Context(), testKey, "")
			if err != nil {
				t.Fatalf("cold provision: %v", err)
			}
			m.destroy(t.Context(), sb.VMName)
			if n := len(eng.caInstalls); n != tt.installs {
				t.Errorf("InstallCACert calls = %d, want %d", n, tt.installs)
			}
		})
	}
}

func TestColdProvisionFailsClosedOnCAInstallError(t *testing.T) {
	eng := newFakeEngine()
	eng.installCAErr = errors.New("silkd down")
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()})
	if _, err := m.provision(t.Context(), testKey, ""); err == nil {
		t.Error("cold provision succeeded though the guest never got the root; want fail-closed")
	}
}

func TestInterceptPoolAllowsPromote(t *testing.T) {
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: interceptPolicy()})
	sb := &types.Sandbox{ID: "sb_i", Key: testKey, Token: "tok", VMName: "sbx-i"}
	m.mu.Lock()
	m.addClaimLocked(sb)
	m.mu.Unlock()

	if _, _, err := m.Promote(t.Context(), sb.ID, Cred{Token: "tok"}, "tpl:x", ""); err != nil {
		t.Errorf("Promote of an interception-pool sandbox: %v, want success", err)
	}
}

func plainPolicy() *egress.Policy {
	return &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh"}}}
}

func interceptPolicy() *egress.Policy {
	return &egress.Policy{Allow: []egress.Rule{{Host: "api.github.com", Secret: "gh", Intercept: egress.InterceptAlways}}}
}
