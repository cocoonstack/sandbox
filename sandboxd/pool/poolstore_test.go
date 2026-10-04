package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/metastore/metastoretest"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/tenantstest"
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

func TestPoolSeedHashBytesArePinned(t *testing.T) {
	specs := []config.PoolSpec{
		{Template: "rt:24.04&<x>", Net: types.NetNone, Size: "small", Warm: 2},
		{Template: "python:3.12", Net: types.NetEgress, Size: "large", WarmMax: 4},
	}
	if got, want := poolSeedHash(nil), "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"; got != want {
		t.Errorf("seed of no pools %s, want the pinned seed %s", got, want)
	}
	if got, want := poolSeedHash(specs), "3b7a82ff7910e88edcc6ec7bfd05142c647900f950d1a8cc785ee0ed344e1f08"; got != want {
		t.Errorf("seed %s, want the pinned seed %s", got, want)
	}
}

func TestPoolSeedHashIgnoresConfigOwnedFields(t *testing.T) {
	base := config.PoolSpec{PoolKey: seedKey, Warm: 2}
	want := poolSeedHash([]config.PoolSpec{base})
	for name, edit := range map[string]func(*config.PoolSpec){
		"egress":              func(s *config.PoolSpec) { s.Egress = &egress.Policy{Allow: []egress.Rule{{Host: "api.example.com"}}} },
		"warmup":              func(s *config.PoolSpec) { s.Warmup = []string{"true"} },
		"capture_trim":        func(s *config.PoolSpec) { s.CaptureTrim = true },
		"storage":             func(s *config.PoolSpec) { s.Storage = "20G" },
		"egress_upstream_env": func(s *config.PoolSpec) { s.EgressUpstreamEnv = "POOL_UPSTREAM" },
	} {
		spec := base
		edit(&spec)
		if got := poolSeedHash([]config.PoolSpec{spec}); got != want {
			t.Errorf("a %s edit changed the seed: the API cannot carry it, so a restart would warn of an override", name)
		}
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
	pf := poolset.Set{Pools: []config.PoolSpec{
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

func TestNodesOfACellShareThePoolSet(t *testing.T) {
	t.Setenv("SANDBOX_TEST_DSN", metastoretest.PGSchema(t))
	node := func(cell string) *Manager {
		cfg := &config.Config{
			DataDir:   t.TempDir(),
			Pools:     []config.PoolSpec{{PoolKey: seedKey, Warm: 1}},
			MetaStore: &config.MetaStoreConfig{Kind: "pg", DSNEnv: "SANDBOX_TEST_DSN", Cell: cell},
		}
		m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t))
		if err != nil {
			t.Fatalf("setup manager: %v", err)
		}
		t.Cleanup(func() { _, _ = m.tenants.Close(), m.poolShared.Close() })
		go m.followCellPools(t.Context())
		return m
	}
	a, b, other := node("c1"), node("c1"), node("c2")
	if err := a.SetPools(t.Context(), []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}); err != nil {
		t.Fatalf("put on a: %v", err)
	}
	if !tenantstest.Eventually(t, func() bool { return hasAPISet(b) }) {
		t.Fatal("b never applied the pool set put on a")
	}
	if hasAPISet(other) {
		t.Error("a node of another cell applied the pool set")
	}
	cached := func(m *Manager) bool {
		set, err := m.poolStore.load()
		return err == nil && set != nil && len(set.Pools) == 1 && set.Pools[0].PoolKey == apiKey
	}
	if !tenantstest.Eventually(t, func() bool { return cached(b) }) {
		t.Error("b never cached the cell set in pools.json")
	}
	if joiner := node("c1"); !hasAPISet(joiner) || !cached(joiner) {
		t.Error("a node joining the cell did not boot with its pool set and cache it")
	}
}

func TestAFailedCellLoadIsRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: seedKey, Warm: 1})
		m.poolShared = &fakeShared{set: &poolset.Set{Pools: []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}}, version: 1, failLoads: 1}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go m.followCellPools(ctx)
		synctest.Wait()
		if hasAPISet(m) {
			t.Fatal("applied a set its load could not read")
		}
		time.Sleep(cellRetry)
		synctest.Wait()
		if !hasAPISet(m) {
			t.Error("a failed cell load was not retried while the listener stayed up")
		}
	})
}

func TestAStaleCellVersionIsNotApplied(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: seedKey, Warm: 1})
	m.poolShared = &fakeShared{}
	for _, apply := range []struct {
		warm    int
		version int64
	}{{3, 5}, {2, 4}, {1, 5}} {
		if err := m.applyPools(t.Context(), map[types.PoolKey]config.PoolSpec{apiKey: {PoolKey: apiKey, Warm: apply.warm}}, apply.version); err != nil {
			t.Fatalf("apply version %d: %v", apply.version, err)
		}
	}
	m.mu.Lock()
	floor := m.pools[apiKey].floor
	m.mu.Unlock()
	if floor != 3 {
		t.Errorf("warm floor %d after an older and a repeated version, want 3 from version 5", floor)
	}
}

func TestAPutTheCellCannotStoreChangesNothing(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: seedKey, Warm: 1})
	m.poolShared = &fakeShared{err: fmt.Errorf("%w: connection refused", poolset.ErrUnavailable)}
	if err := m.SetPools(t.Context(), []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}); !errors.Is(err, ErrPoolStoreDown) {
		t.Fatalf("put with the cell store down: %v, want ErrPoolStoreDown", err)
	}
	if hasAPISet(m) {
		t.Error("a put the cell did not store was applied")
	}
	if set, err := m.poolStore.load(); set != nil || err != nil {
		t.Errorf("a put the cell did not store reached pools.json: %+v %v", set, err)
	}
}

func TestBootPrefersTheCellSetOverPoolsJSON(t *testing.T) {
	cached := poolset.Set{Pools: []config.PoolSpec{{PoolKey: apiKey, Warm: 3}}}
	unfit := poolset.Set{Pools: []config.PoolSpec{{Template: "eg:24.04", Net: types.NetEgress, Size: types.SizeSmall, Warm: 1}}}
	for name, tc := range map[string]struct {
		shared   *fakeShared
		wantAPI  bool
		version  int64
		recached bool
	}{
		"unreadable cell set serves the cache":    {shared: &fakeShared{err: poolset.ErrUnavailable}, wantAPI: true},
		"empty cell keeps the config seed":        {shared: &fakeShared{}},
		"cell set wins and is cached":             {shared: &fakeShared{set: &cached, version: 7}, wantAPI: true, version: 7, recached: true},
		"a cell set that does not fit warns only": {shared: &fakeShared{set: &unfit, version: 7}, wantAPI: true},
	} {
		t.Run(name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: seedKey, Warm: 1})
			if err := m.poolStore.commit(m.poolStore.seq.Add(1), cached); err != nil {
				t.Fatalf("seed pools.json: %v", err)
			}
			m.poolShared = tc.shared
			if err := m.adoptPools(t.Context()); err != nil {
				t.Fatalf("adoptPools: %v", err)
			}
			if got := hasAPISet(m); got != tc.wantAPI {
				t.Errorf("API set adopted = %v, want %v", got, tc.wantAPI)
			}
			if m.poolVersion != tc.version {
				t.Errorf("applied version %d, want %d", m.poolVersion, tc.version)
			}
			if set, err := m.poolStore.load(); err != nil || (set.ConfigSeed == m.configSeedHash) != tc.recached {
				t.Errorf("pools.json %+v %v, want recached=%v", set, err, tc.recached)
			}
		})
	}
}

func hasAPISet(m *Manager) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, api := m.activePool(apiKey)
	_, seed := m.activePool(seedKey)
	return api && p.floor == 3 && !seed
}

type fakeShared struct {
	set       *poolset.Set
	version   int64
	err       error
	failLoads int
}

func (f *fakeShared) Load(context.Context) (*poolset.Set, int64, error) {
	if f.failLoads > 0 {
		f.failLoads--
		return nil, 0, poolset.ErrUnavailable
	}
	return f.set, f.version, f.err
}

func (f *fakeShared) Store(_ context.Context, set poolset.Set) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.set = &set
	f.version++
	return f.version, nil
}

func (f *fakeShared) Watch(ctx context.Context, changed func()) {
	changed()
	<-ctx.Done()
}

func (f *fakeShared) Close() error {
	return nil
}
