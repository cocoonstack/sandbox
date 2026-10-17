package pool

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type leaseTiming struct {
	ttl, every, margin time.Duration
}

// keepLease renews this node's lease until ctx ends and fences its replicated claims when a renewal is overdue.
func (m *Manager) keepLease(ctx context.Context) {
	fence := time.AfterFunc(m.lease.ttl-m.lease.margin, func() { m.fence(ctx) })
	defer fence.Stop()
	tick := time.NewTicker(m.lease.every)
	defer tick.Stop()
	for {
		m.renewLease(ctx, fence)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (m *Manager) renewLease(ctx context.Context, fence *time.Timer) {
	start := time.Now()
	if err := m.archives.Renew(ctx, m.nodeID, m.lease.ttl); err != nil {
		log.WithFunc("pool.renewLease").Warnf(ctx, "renew node lease: %v", err)
		return
	}
	fence.Reset(m.lease.ttl - m.lease.margin - time.Since(start))
	if m.fenced.Load() {
		m.unfence(ctx)
	}
}

func (m *Manager) fence(ctx context.Context) {
	if !m.fenced.Swap(true) {
		log.WithFunc("pool.fence").Warnf(ctx, "node lease not renewed for %s; replicated claims answer 503 until it is", m.lease.ttl-m.lease.margin)
	}
}

func (m *Manager) unfence(ctx context.Context) {
	logger := log.WithFunc("pool.unfence")
	m.mu.Lock()
	held := map[string]*types.Sandbox{}
	for id, sb := range m.claimed {
		if sb.ReplicaCk != "" {
			held[id] = sb
		}
	}
	m.mu.Unlock()
	var rows map[string]archives.Record
	if len(held) > 0 {
		var err error
		if rows, err = m.archives.Rows(ctx, slices.Collect(maps.Keys(held))); err != nil {
			logger.Warnf(ctx, "replicated claims stay fenced: %v", err)
			return
		}
	}
	for id, sb := range held {
		rec, ok := rows[id]
		if ok && rec.Owner != m.nodeID || !ok && m.replicaConfirmed(sb) {
			sb.Transition.Lock()
			m.dropAdopted(ctx, sb)
			sb.Transition.Unlock()
		}
	}
	m.fenced.Store(false)
	logger.Info(ctx, "node lease renewed; replicated claims are served again")
}

func (m *Manager) leaseFenced(sb *types.Sandbox) bool {
	return m.fenced.Load() && m.replicaOf(sb) != ""
}
