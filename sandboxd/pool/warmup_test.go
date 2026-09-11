package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestGoldenBuildRunsWarmupBeforeSnapshot(t *testing.T) {
	eng := newFakeEngine()
	argv := []string{"node", "-e", "0"}
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: argv})
	final := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := m.buildGoldenSteps(t.Context(), testKey, "sbx-gb", "snap", final); err != nil {
		t.Fatalf("buildGoldenSteps: %v", err)
	}
	if len(eng.warmups) != 1 || !slices.Equal(eng.warmups[0], argv) {
		t.Fatalf("warmups = %v, want [%v]", eng.warmups, argv)
	}
	if eng.warmupAfterSnap {
		t.Fatal("golden warmup ran after the snapshot save")
	}
	stamp, err := os.ReadFile(final + warmupSidecarSuffix)
	if err != nil {
		t.Fatalf("read warmup sidecar: %v", err)
	}
	if string(stamp) != warmupStamp(argv) {
		t.Errorf("sidecar = %q, want %q", stamp, warmupStamp(argv))
	}
}

func TestGoldenBuildSkipsWarmupWhenUnset(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	final := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := m.buildGoldenSteps(t.Context(), testKey, "sbx-gb", "snap", final); err != nil {
		t.Fatalf("buildGoldenSteps: %v", err)
	}
	if len(eng.warmups) != 0 {
		t.Errorf("Warmup called %d times for a pool without one", len(eng.warmups))
	}
	if _, err := os.Stat(final + warmupSidecarSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("warmup sidecar present for a pool without one: %v", err)
	}
}

func TestAdoptGoldenRequiresMatchingWarmup(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: []string{"node", "-e", "0"}})
	final := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(final, 0o755); err != nil {
		t.Fatalf("mkdir golden: %v", err)
	}
	p := m.pools[testKey]
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted a golden built without the warmup")
	}
	if err := os.WriteFile(final+warmupSidecarSuffix, []byte(warmupStamp([]string{"python3", "-c", "0"})), 0o644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != "" {
		t.Error("adopted a golden built with a different warmup")
	}
	if err := os.WriteFile(final+warmupSidecarSuffix, []byte(warmupStamp([]string{"node", "-e", "0"})), 0o644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	m.adoptGolden(p)
	if p.goldenDir != final {
		t.Errorf("goldenDir = %q, want %q", p.goldenDir, final)
	}
}

func TestSetPoolsRejectsWarmup(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: testKey, Warm: 1, Warmup: []string{"true"}}})
	if !errors.Is(err, ErrBadKey) || !strings.Contains(err.Error(), "warmup is set in the config file") {
		t.Errorf("SetPools error = %v, want ErrBadKey naming warmup as config-owned", err)
	}
}

func TestPoolSpecRejectsEmptyWarmupArgument(t *testing.T) {
	spec := config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: []string{"node", ""}}
	if err := spec.ValidateLimits(); err == nil || !strings.Contains(err.Error(), "warmup") {
		t.Errorf("ValidateLimits error = %v, want an empty-argument rejection", err)
	}
}

func TestRefillWarmsEveryClone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		argv := []string{"node", "-e", "0"}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2, Warmup: argv})
		m.pools[testKey].goldenDir = "/goldens/x"

		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 2 && infos[0].Refilling == 0
		})
		eng.mu.Lock()
		warmups, socks := slices.Clone(eng.warmups), slices.Clone(eng.warmupSocks)
		eng.mu.Unlock()
		if len(warmups) != 2 || !slices.Equal(warmups[0], argv) || !slices.Equal(warmups[1], argv) {
			t.Fatalf("warmups = %v, want %v once per clone", warmups, argv)
		}
		if socks[0] == "" || socks[0] == socks[1] {
			t.Errorf("warmup sockets = %v, want one per clone", socks)
		}
	})
}

func TestRefillCloneWarmupFailureCleansUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.warmupErr = errors.New("node: not found")
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Warmup: []string{"node", "-e", "0"}})
		m.pools[testKey].goldenDir = "/goldens/x"

		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 0 && infos[0].Refilling == 0 && len(eng.removedNames()) == 1
		})
		eng.mu.Lock()
		eng.warmupErr = nil
		eng.mu.Unlock()
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 1 && infos[0].Refilling == 0
		})
	})
}
