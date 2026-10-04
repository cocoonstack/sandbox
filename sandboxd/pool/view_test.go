package pool

import (
	"maps"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func editView(m *Manager, edit func(v *configView)) {
	cur := m.view.Load()
	next := *cur
	next.poolWarmups = maps.Clone(cur.poolWarmups)
	next.poolTrims = maps.Clone(cur.poolTrims)
	next.poolStorage = maps.Clone(cur.poolStorage)
	edit(&next)
	m.view.Store(&next)
}

// editEgress swaps in an egress view built from m.cfg as edit changes it; the view has no secrets.
func editEgress(t *testing.T, m *Manager, edit func(cfg *config.Config)) {
	t.Helper()
	cfg := *m.cfg
	cfg.Pools = slices.Clone(cfg.Pools)
	cfg.EgressClasses = slices.Clone(cfg.EgressClasses)
	edit(&cfg)
	out := outbound.NewView(&cfg, testSecrets(t))
	editView(m, func(v *configView) { v.out = out })
}

func stampWithWarmup(m *Manager, key types.PoolKey, warmup []string, imageID string) string {
	v := *m.view.Load()
	v.poolWarmups = map[types.PoolKey][]string{key: warmup}
	return m.goldenStamp(&v, key, imageID)
}
