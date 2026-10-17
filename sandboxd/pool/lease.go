package pool

import (
	"context"
	"io"
	"maps"
	"slices"
	"sync"
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
		close(*m.fenceCh.Load())
		log.WithFunc("pool.fence").Warnf(ctx, "node lease not renewed for %s; replicated claims answer 503 until it is", m.lease.ttl-m.lease.margin)
	}
}

func (m *Manager) unfence(ctx context.Context) {
	logger := log.WithFunc("pool.unfence")
	m.mu.Lock()
	held := map[string]*types.Sandbox{}
	for id, sb := range m.claimed {
		if sb.Replicated() {
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
	m.fenceCh.Store(new(make(chan struct{})))
	m.fenced.Store(false)
	logger.Info(ctx, "node lease renewed; replicated claims are served again")
}

// fenceWatch gates sb at resolution and returns the channel that closes when its streams must end; nil for an unreplicated claim.
func (m *Manager) fenceWatch(sb *types.Sandbox) (<-chan struct{}, error) {
	if !sb.Replicated() {
		return nil, nil
	}
	if m.fenced.Load() {
		return nil, ErrLeaseLost
	}
	return *m.fenceCh.Load(), nil
}

// CloseOnFence closes c when fenced closes; stop ends the watch.
func CloseOnFence(fenced <-chan struct{}, c io.Closer) (stop func()) {
	if fenced == nil {
		return func() {}
	}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-fenced:
			_ = c.Close()
		case <-stopped:
		}
	}()
	return sync.OnceFunc(func() { close(stopped) })
}
