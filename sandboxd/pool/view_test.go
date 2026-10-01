package pool

import (
	"maps"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func editView(m *Manager, edit func(v *configView)) {
	cur := m.view.Load()
	next := *cur
	next.poolEgress = maps.Clone(cur.poolEgress)
	next.tenantEgress = maps.Clone(cur.tenantEgress)
	next.poolWarmups = maps.Clone(cur.poolWarmups)
	next.poolTrims = maps.Clone(cur.poolTrims)
	next.poolStorage = maps.Clone(cur.poolStorage)
	next.poolUpstream = maps.Clone(cur.poolUpstream)
	next.tenantUpstream = maps.Clone(cur.tenantUpstream)
	edit(&next)
	m.view.Store(&next)
}

func stampWithWarmup(m *Manager, key types.PoolKey, warmup []string, imageID string) string {
	v := *m.view.Load()
	v.poolWarmups = map[types.PoolKey][]string{key: warmup}
	return m.goldenStamp(&v, key, imageID)
}
