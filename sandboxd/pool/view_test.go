package pool

import (
	"maps"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func editView(m *Manager, edit func(v *configView)) {
	cur := m.view.Load()
	next := *cur
	next.poolEgress = maps.Clone(cur.poolEgress)
	next.classEgress = maps.Clone(cur.classEgress)
	next.poolWarmups = maps.Clone(cur.poolWarmups)
	next.poolTrims = maps.Clone(cur.poolTrims)
	next.poolStorage = maps.Clone(cur.poolStorage)
	next.poolUpstream = maps.Clone(cur.poolUpstream)
	next.classUpstream = maps.Clone(cur.classUpstream)
	edit(&next)
	m.view.Store(&next)
}

func stampWithWarmup(m *Manager, key types.PoolKey, warmup []string, imageID string) string {
	v := *m.view.Load()
	v.poolWarmups = map[types.PoolKey][]string{key: warmup}
	return m.goldenStamp(&v, key, imageID)
}
