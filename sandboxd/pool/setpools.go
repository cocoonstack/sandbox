package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/poolset"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// SetPools replaces the warm targets of the node, or of its whole cell with a meta store; claims are unaffected.
func (m *Manager) SetPools(ctx context.Context, specs []config.PoolSpec) error {
	desired, err := m.desiredPools(specs)
	if err != nil {
		return err
	}
	var version int64
	if m.poolShared != nil {
		set := poolset.Set{ConfigSeed: m.configSeedHash, Pools: slices.Collect(maps.Values(desired))}
		if version, err = m.poolShared.Store(ctx, set); err != nil {
			return err
		}
	}
	return m.applyPools(ctx, desired, version)
}

func (m *Manager) desiredPools(specs []config.PoolSpec) (map[types.PoolKey]config.PoolSpec, error) {
	desired := make(map[types.PoolKey]config.PoolSpec, len(specs))
	for _, spec := range specs {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return nil, err
		}
		if err := spec.ValidateLimits(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrBadCount, err)
		}
		// egress is config-owned; accepting it here would silently drop it
		if spec.Egress != nil {
			return nil, fmt.Errorf("%w: pool %q: egress is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if spec.Warmup != nil {
			return nil, fmt.Errorf("%w: pool %q: warmup is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if spec.CaptureTrim {
			return nil, fmt.Errorf("%w: pool %q: capture_trim is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if spec.Storage != "" {
			return nil, fmt.Errorf("%w: pool %q: storage is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if spec.EgressUpstreamEnv != "" {
			return nil, fmt.Errorf("%w: pool %q: egress_upstream_env is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if _, ok := desired[spec.PoolKey]; ok {
			return nil, fmt.Errorf("%w: duplicate pool %q", ErrBadKey, spec.Template)
		}
		desired[spec.PoolKey] = spec
	}
	return desired, nil
}

// applyPools makes desired the node's pools unless the cell set already moved past version.
func (m *Manager) applyPools(ctx context.Context, desired map[types.PoolKey]config.PoolSpec, version int64) error {
	var trim []string
	m.mu.Lock()
	if m.poolShared != nil && version <= m.poolVersion {
		m.mu.Unlock()
		return nil
	}
	m.poolVersion = version
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
		m.pools[key] = p
	}
	m.mu.Unlock()

	runCtx := context.WithoutCancel(ctx)
	m.destroyAll(runCtx, trim).Wait()
	m.refillOnce(runCtx)
	// persist the applied set so a restart rebuilds from it, not the config seed.
	return m.poolStore.commit(seq, poolset.Set{ConfigSeed: m.configSeedHash, Pools: slices.Collect(maps.Values(desired))})
}

func (m *Manager) followCellPools(ctx context.Context) {
	kick := make(chan struct{}, 1)
	changed := func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	}
	go m.poolShared.Watch(ctx, changed)
	logger := log.WithFunc("pool.followCellPools")
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		}
		set, version, err := m.poolShared.Load(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			logger.Warnf(ctx, "load the cell pool set: %v; retrying in %s", err, cellRetry)
			time.AfterFunc(cellRetry, changed)
			continue
		case set == nil:
			continue
		}
		desired, err := m.desiredPools(set.Pools)
		if err == nil {
			err = m.applyPools(ctx, desired, version)
		}
		if err != nil {
			logger.Warnf(ctx, "cell pool set not applied (%v); the current pools keep serving until it next changes", err)
		}
	}
}

// normalizePoolSpec fills the wire defaults; config files stay explicit.
func normalizePoolSpec(spec config.PoolSpec) config.PoolSpec {
	spec.PoolKey = spec.Defaulted()
	return spec
}
