package pool

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var (
	seedKey = types.PoolKey{Template: "seed:24.04", Net: types.NetNone, Size: types.SizeSmall}
	apiKey  = types.PoolKey{Template: "api:24.04", Net: types.NetNone, Size: types.SizeSmall}
)

func TestPersistedPoolsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	m := newTestManagerAt(t, newFakeEngine(), dir, config.PoolSpec{PoolKey: seedKey, Warm: 1})
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}

	m2 := newTestManagerAt(t, newFakeEngine(), dir, config.PoolSpec{PoolKey: seedKey, Warm: 1})
	m2.mu.Lock()
	_, hasSeed := m2.pools[seedKey]
	restored, hasAPI := m2.pools[apiKey]
	m2.mu.Unlock()
	if hasSeed || !hasAPI {
		t.Fatalf("restart did not restore API pools: seed=%v api=%v", hasSeed, hasAPI)
	}
	if restored.floor != 3 {
		t.Errorf("restored warm floor = %d, want 3", restored.floor)
	}
}

func TestDeletingPoolsFileRestoresConfigSeed(t *testing.T) {
	dir := t.TempDir()
	m := newTestManagerAt(t, newFakeEngine(), dir, config.PoolSpec{PoolKey: seedKey, Warm: 1})
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}); err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "pools.json")); err != nil {
		t.Fatalf("remove pools.json: %v", err)
	}
	m2 := newTestManagerAt(t, newFakeEngine(), dir, config.PoolSpec{PoolKey: seedKey, Warm: 1})
	m2.mu.Lock()
	_, hasSeed := m2.pools[seedKey]
	_, hasAPI := m2.pools[apiKey]
	m2.mu.Unlock()
	if !hasSeed || hasAPI {
		t.Fatalf("deleting pools.json did not restore the config seed: seed=%v api=%v", hasSeed, hasAPI)
	}
}

func TestPoolSeedHashMatchesTheV1Bytes(t *testing.T) {
	specs := []config.PoolSpec{
		{Template: "rt:24.04&<x>", Net: types.NetNone, Size: "small", Warm: 2},
		{Template: "python:3.12", Net: types.NetEgress, Size: "large", WarmMax: 4},
	}
	if got, want := poolSeedHash(nil), "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b"; got != want {
		t.Errorf("seed of no pools %s, want the encoding/json v1 seed %s", got, want)
	}
	if got, want := poolSeedHash(specs), "8f01958ec3e7ca20e71032d3c857073614887c045887d53a8e93776a2826aad7"; got != want {
		t.Errorf("seed %s, want the encoding/json v1 seed %s", got, want)
	}
}

func TestConcurrentSetPoolsPersistLatest(t *testing.T) {
	dir := t.TempDir()
	m := newTestManagerAt(t, newFakeEngine(), dir, config.PoolSpec{PoolKey: seedKey, Warm: 1})
	var wg sync.WaitGroup
	for warm := 1; warm <= 8; warm++ {
		wg.Go(func() {
			if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: apiKey, Warm: warm}}); err != nil {
				t.Errorf("SetPools(warm=%d): %v", warm, err)
			}
		})
	}
	wg.Wait()

	m.mu.Lock()
	want := m.pools[apiKey].floor
	m.mu.Unlock()
	pf, err := m.poolStore.load()
	if err != nil || pf == nil {
		t.Fatalf("load pools.json: pf=%v err=%v", pf, err)
	}
	if len(pf.Pools) != 1 || pf.Pools[0].Warm != want {
		t.Fatalf("persisted %+v, want the last-applied warm %d", pf.Pools, want)
	}
}

func TestRestoredEgressPoolNeedsAttachment(t *testing.T) {
	dir := t.TempDir()
	pf := poolsFile{Pools: []config.PoolSpec{
		{Template: "eg:24.04", Net: types.NetEgress, Size: types.SizeSmall, Warm: 1},
	}}
	raw, err := json.Marshal(pf)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pools.json"), raw, 0o600); err != nil {
		t.Fatalf("write pools.json: %v", err)
	}

	if _, err := NewManager(t.Context(), &config.Config{DataDir: dir}, newFakeEngine(), testSecrets(t)); err == nil {
		t.Error("NewManager accepted a restored egress-lane pool without an attachment")
	}
}
