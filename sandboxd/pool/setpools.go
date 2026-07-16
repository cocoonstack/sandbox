package pool

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// SetPools replaces the node's desired warm targets. Existing claims are not
// affected; only unclaimed warm VMs are trimmed or refilled.
func (m *Manager) SetPools(ctx context.Context, specs []config.PoolSpec) error {
	desired := make(map[types.PoolKey]config.PoolSpec, len(specs))
	hashes := make(map[string]types.PoolKey, len(specs))
	for _, spec := range specs {
		spec = normalizePoolSpec(spec)
		if err := m.validate(spec.PoolKey); err != nil {
			return err
		}
		if err := spec.ValidateLimits(); err != nil {
			return fmt.Errorf("%w: %v", ErrBadCount, err)
		}
		// Egress is config-owned (validated at load); accepting it here would
		// silently drop it.
		if spec.Egress != nil {
			return fmt.Errorf("%w: pool %q: egress is set in the config file, not via the API", ErrBadKey, spec.Template)
		}
		if existing, ok := hashes[spec.Hash()]; ok && existing != spec.PoolKey {
			return fmt.Errorf("%w: pool key hash collision between %q and %q", ErrBadKey, existing.Template, spec.Template)
		}
		if _, ok := desired[spec.PoolKey]; ok {
			return fmt.Errorf("%w: duplicate pool %q", ErrBadKey, spec.Template)
		}
		hashes[spec.Hash()] = spec.PoolKey
		desired[spec.PoolKey] = spec
	}

	var trim []string
	m.mu.Lock()
	// Sequenced under the mutex (apply order) so commit drops a write a later apply superseded.
	seq := m.poolStore.seq.Add(1)
	now := time.Now()
	for key, p := range m.pools {
		spec, ok := desired[key]
		// A removed key gets the zero spec: a lingering pool sheds its whole
		// policy — a stale archiveAfter would keep archiving its claims.
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
		p := &pool{key: key}
		p.applySpec(spec)
		// A golden already on disk (from this pool's earlier life) is
		// adopted when its baked-CA state still fits; buildGolden covers the rest.
		if g := filepath.Join(m.goldensDir(), key.Hash()); dirExists(g) && m.goldenCAMatches(g, m.poolEgress[key].Intercepts()) {
			p.goldenDir = g
		}
		m.pools[key] = p
	}
	// Recompute rather than latch: removing every idle pool turns the
	// sweep off again.
	m.idleEnabled = m.idleDefault > 0
	m.archiveEnabled = m.archiveAfterDefault > 0
	for _, p := range m.pools {
		if p.idle > 0 {
			m.idleEnabled = true
		}
		if p.archiveAfter > 0 {
			m.archiveEnabled = true
		}
	}
	m.mu.Unlock()

	runCtx := context.WithoutCancel(ctx)
	m.runBounded(runCtx, len(trim), func(ctx context.Context, i int) {
		m.destroy(ctx, trim[i])
	}).Wait()
	m.refillOnce(runCtx)
	// Persist the applied set so a restart rebuilds from it, not the config seed.
	persisted := make([]config.PoolSpec, 0, len(desired))
	for _, spec := range desired {
		persisted = append(persisted, spec)
	}
	return m.poolStore.commit(seq, poolsFile{ConfigSeed: m.configSeedHash, Pools: persisted})
}

// normalizePoolSpec fills the wire defaults, mirroring ClaimRequest.Key():
// API requests default net/size; config files stay explicit (Validate
// rejects empty there).
func normalizePoolSpec(spec config.PoolSpec) config.PoolSpec {
	if spec.Net == "" {
		spec.Net = types.NetNone
	}
	if spec.Size == "" {
		spec.Size = types.SizeSmall
	}
	return spec
}
