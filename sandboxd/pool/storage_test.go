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

func TestGoldenBuildSizesItsDisk(t *testing.T) {
	eng := newFakeEngine()
	other := types.PoolKey{Template: "py:3.12", Net: types.NetNone, Size: types.SizeSmall}
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"}, config.PoolSpec{PoolKey: other, Warm: 1})

	for _, key := range []types.PoolKey{testKey, other} {
		final := filepath.Join(m.goldensDir(), key.Hash())
		if err := m.buildGoldenSteps(t.Context(), key, "sbx-gb-"+key.Hash(), "snap", final, ""); err != nil {
			t.Fatalf("buildGoldenSteps %s: %v", key.Template, err)
		}
	}

	if !slices.Equal(eng.coldStorage, []string{"40G", ""}) {
		t.Errorf("cold boot disks %v, want the sized pool at 40 and the other at cocoon's default", eng.coldStorage)
	}
	if got := m.goldenStamp(other, false, nil, ""); got != strings.Join([]string{"", "", ""}, "\x00") {
		t.Errorf("unsized stamp %q changed, so goldens built before the field would rebuild", got)
	}
	if !strings.Contains(m.goldenStamp(testKey, false, nil, ""), "storage=42949672960") {
		t.Error("sized stamp does not carry its disk size in bytes")
	}
}

func TestEquivalentSizesStampTheSameGolden(t *testing.T) {
	decimal := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"})
	binary := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40GiB"})
	if a, b := decimal.goldenStamp(testKey, false, nil, ""), binary.goldenStamp(testKey, false, nil, ""); a != b {
		t.Errorf("40G stamps %q, 40GiB stamps %q; the same disk would rebuild its golden", a, b)
	}
}

func TestSizedPoolRebuildsAnUnsizedGolden(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Storage: "40G"})
	g := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(g, 0o750); err != nil {
		t.Fatalf("seed golden: %v", err)
	}
	if err := os.WriteFile(g+goldenStampSuffix, []byte(strings.Join([]string{"", "", ""}, "\x00")), 0o644); err != nil {
		t.Fatalf("seed stamp: %v", err)
	}

	m.mu.Lock()
	adopted := m.adoptGolden(m.pools[testKey], "")
	m.mu.Unlock()

	if adopted {
		t.Error("a pool sized to 40 GiB adopted a golden built at cocoon's default disk")
	}
}
