package pool

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const unsizedStamp = "\x00\x00"

func TestGoldenBuildSizesItsDisk(t *testing.T) {
	eng := newFakeEngine()
	other := tplKey("py:3.12")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"}, config.PoolSpec{PoolKey: other, Warm: 1})

	for _, key := range []types.PoolKey{testKey, other} {
		buildGoldenAt(t, m, key, "")
	}

	if !slices.Equal(eng.coldStorage, []string{"40G", ""}) {
		t.Errorf("cold boot disks %v, want the sized pool at 40 and the other at cocoon's default", eng.coldStorage)
	}
	if got := m.goldenStamp(m.view.Load(), other, ""); got != unsizedStamp {
		t.Errorf("unsized stamp %q changed, so goldens built before the field would rebuild", got)
	}
	if !strings.Contains(m.goldenStamp(m.view.Load(), testKey, ""), "storage=42949672960") {
		t.Error("sized stamp does not carry its disk size in bytes")
	}
}

func TestEquivalentSizesStampTheSameGolden(t *testing.T) {
	decimal := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"})
	binary := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40GiB"})
	if a, b := decimal.goldenStamp(decimal.view.Load(), testKey, ""), binary.goldenStamp(binary.view.Load(), testKey, ""); a != b {
		t.Errorf("40G stamps %q, 40GiB stamps %q; the same disk would rebuild its golden", a, b)
	}
}

func TestSizedPoolRebuildsAnUnsizedGolden(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"})
	g := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(g, 0o750); err != nil {
		t.Fatalf("seed golden: %v", err)
	}
	if err := os.WriteFile(g+goldenStampSuffix, []byte(unsizedStamp), 0o644); err != nil {
		t.Fatalf("seed stamp: %v", err)
	}

	m.mu.Lock()
	adopted := m.adoptGolden(m.pools[testKey], "")
	m.mu.Unlock()

	if adopted {
		t.Error("a pool sized to 40 GiB adopted a golden built at cocoon's default disk")
	}
}

func TestNoBalloonRebuildsABalloonGolden(t *testing.T) {
	spec := config.PoolSpec{PoolKey: testKey, Warm: 1}
	m := newTestManager(t, newFakeEngine(), spec)
	seedGolden(t, m, testKey, "")
	m.view.Store(newConfigView(&config.Config{NoBalloon: true, Pools: []config.PoolSpec{spec}}, testSecrets(t)))

	m.mu.Lock()
	adopted := m.adoptGolden(m.pools[testKey], "")
	m.mu.Unlock()

	if adopted {
		t.Error("a no_balloon node adopted a golden booted with a balloon")
	}
}
