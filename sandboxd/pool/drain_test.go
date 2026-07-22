package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestDrainRefusesClaimsTrimsWarmAndUncordonRefills(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	goldenDir := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(goldenDir, 0o750); err != nil {
		t.Fatalf("setup golden: %v", err)
	}
	markGoldenRuntime(t, goldenDir)
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 2}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	waitFor(t, func() bool {
		infos, _ := m.Info()
		return len(infos) == 1 && infos[0].Warm == 2
	})

	m.Drain(t.Context())

	if _, err := m.ClaimWarm(t.Context(), testKey, 0, ""); !errors.Is(err, ErrQuota) {
		t.Fatalf("ClaimWarm during drain: %v, want ErrQuota", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, ""); !errors.Is(err, ErrQuota) {
		t.Fatalf("ClaimProvision during drain: %v, want ErrQuota", err)
	}
	infos, g := m.Info()
	if !g.Draining || infos[0].Warm != 0 {
		t.Fatalf("draining=%v warm=%d, want true/0", g.Draining, infos[0].Warm)
	}
	if removed := eng.removedNames(); len(removed) != 2 {
		t.Fatalf("removed=%v, want both warm VMs destroyed", removed)
	}
	m.refillOnce(t.Context())
	if infos, _ = m.Info(); infos[0].Warm != 0 || infos[0].Refilling != 0 {
		t.Fatalf("refill ran during drain: %+v", infos)
	}

	m.Uncordon(t.Context())
	waitFor(t, func() bool {
		infos, g := m.Info()
		return !g.Draining && len(infos) == 1 && infos[0].Warm == 2
	})
	if _, err := claimAny(t.Context(), m, testKey, 0); err != nil {
		t.Fatalf("claim after uncordon: %v", err)
	}
}
