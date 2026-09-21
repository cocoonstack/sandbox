package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// SetPools replaces the node's desired warm targets; existing claims are unaffected.
func (m *Manager) SetPools(ctx context.Context, specs []config.PoolSpec) error {
	desired := make(map[types.PoolKey]config.PoolSpec, len(specs))
	for _, spec := range specs {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return err
		}
		if err := spec.ValidateLimits(); err != nil {
			return fmt.Errorf("%w: %w", ErrBadCount, err)
		}
		// egress is config-owned; accepting it here would silently drop it
		if spec.Egress != nil {
			return fmt.Errorf("%w: pool %q: egress is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if spec.Warmup != nil {
			return fmt.Errorf("%w: pool %q: warmup is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if _, ok := desired[spec.PoolKey]; ok {
			return fmt.Errorf("%w: duplicate pool %q", ErrBadKey, spec.Template)
		}
		desired[spec.PoolKey] = spec
	}

	var trim []string
	m.mu.Lock()
	// sequenced under the mutex (apply order) so commit drops a write a later apply superseded.
	seq := m.poolStore.seq.Add(1)
	now := time.Now()
	for key, p := range m.pools {
		spec, ok := desired[key]
		// a removed key gets the zero spec, so a lingering pool sheds a stale archiveAfter
		p.applySpec(spec)
		p.removed = !ok
		trim = append(trim, p.trimWarm(p.effectiveTarget(now))...)
		if !ok && !p.building && p.refilling == 0 {
			delete(m.pools, key)
		}
	}
	for key, spec := range desired {
		if p := m.pools[key]; p != nil {
			continue
		}
		p := newPool(key)
		p.applySpec(spec)
		m.adoptGolden(p)
		m.pools[key] = p
	}
	m.mu.Unlock()

	runCtx := context.WithoutCancel(ctx)
	m.destroyAll(runCtx, trim).Wait()
	m.refillOnce(runCtx)
	// persist the applied set so a restart rebuilds from it, not the config seed.
	persisted := slices.Collect(maps.Values(desired))
	return m.poolStore.commit(seq, poolsFile{ConfigSeed: m.configSeedHash, Pools: persisted})
}

// normalizePoolSpec fills the wire defaults; config files stay explicit.
func normalizePoolSpec(spec config.PoolSpec) config.PoolSpec {
	spec.PoolKey = spec.Defaulted()
	return spec
}
