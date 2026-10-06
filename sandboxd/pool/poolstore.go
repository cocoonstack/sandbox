package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

type poolStore struct {
	path string

	seq     atomic.Uint64 // sequence source; bumped under the manager mutex (apply order)
	mu      sync.Mutex
	written uint64 // highest sequence on disk; guarded by mu
}

func newPoolStore(dataDir string) *poolStore {
	return &poolStore{path: filepath.Join(dataDir, "pools.json")}
}

// load returns the last applied set, or nil when the node has never applied one.
func (s *poolStore) load() (*poolset.Set, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pools file: %w", err)
	}
	var set poolset.Set
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, fmt.Errorf("parse pools file: %w", err)
	}
	return &set, nil
}

// commit durably writes the applied set; a seq no newer than the last written is a no-op.
func (s *poolStore) commit(seq uint64, set poolset.Set) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.written {
		return nil
	}
	raw, err := json.Marshal(set)
	if err != nil {
		return fmt.Errorf("encode pools file: %w", err)
	}
	if err := utils.WriteFileSync(s.path, raw, 0o600); err != nil {
		return err
	}
	s.written = seq
	return nil
}

// adoptPools lays the API-applied set over the config seed: the cell's set with a meta store, else pools.json.
func (m *Manager) adoptPools(ctx context.Context) error {
	logger := log.WithFunc("pool.adoptPools")
	set, err := m.poolStore.load()
	if err != nil {
		return err
	}
	from := m.poolStore.path
	if m.poolShared != nil {
		shared, version, err := m.poolShared.Load(ctx)
		switch {
		case err != nil:
			logger.Warnf(ctx, "cell pool set unreadable (%v); serving %s until it syncs", err, from)
		case shared == nil:
			if set != nil {
				logger.Warnf(ctx, "%s is ignored: the cell has no API-applied pool set yet; PUT /v1/pools to share one", from)
			}
			return nil
		default:
			if _, err := m.desiredPools(shared.Pools); err != nil {
				logger.Warnf(ctx, "the cell pool set does not fit this node (%v); serving %s", err, from)
				break
			}
			if err := m.poolStore.commit(m.poolStore.seq.Add(1), poolset.Set{ConfigSeed: m.configSeedHash, Pools: shared.Pools}); err != nil {
				return err
			}
			set, m.poolVersion, from = shared, version, "the cell pool set"
		}
	}
	if set == nil {
		return nil
	}
	if set.ConfigSeed != m.configSeedHash {
		logger.Warnf(ctx, "config.json pools differ from the API-applied set in %s, which overrides them", from)
	}
	clear(m.pools)
	for _, spec := range set.Pools {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return fmt.Errorf("restore pool %q from %s: %w", spec.Template, from, err)
		}
		if err := spec.ValidateLimits(); err != nil {
			return fmt.Errorf("restore pool %q from %s: %w", spec.Template, from, err)
		}
		p := newPool(spec.PoolKey)
		p.applySpec(spec)
		m.pools[spec.PoolKey] = p
	}
	logger.Infof(ctx, "restored %d API-applied pools from %s", len(set.Pools), from)
	return nil
}

// poolSeedHash digests a pool set's warm-target shape, order-independent, without the config-owned egress, warmup, capture_trim, storage and egress_upstream_env.
func poolSeedHash(specs []config.PoolSpec) string {
	shaped := slices.Clone(specs)
	for i := range shaped {
		shaped[i].Egress = nil
		shaped[i].Warmup = nil
		shaped[i].CaptureTrim = false
		shaped[i].Storage = ""
		shaped[i].EgressUpstreamEnv = ""
	}
	slices.SortFunc(shaped, func(a, b config.PoolSpec) int { return a.Compare(b.PoolKey) })
	return utils.DigestHex(shaped)
}
