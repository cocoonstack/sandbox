package pool

import (
	"context"
	"errors"
	"fmt"
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

// fenceState is the whole fence: while fenced no replicated claim is served, and ch closes when it trips.
type fenceState struct {
	ch     chan struct{}
	fenced bool
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

// keepLease renews this node's lease until ctx ends and fences its replicated claims when a renewal is overdue.
func (m *Manager) keepLease(ctx context.Context) {
	fence := time.AfterFunc(m.lease.ttl-m.lease.margin, func() { m.trip(ctx) })
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
	if m.isFenced() && m.unfencing.CompareAndSwap(false, true) {
		go func() {
			defer m.unfencing.Store(false)
			m.unfence(ctx, start)
		}()
	}
}

func (m *Manager) isFenced() bool {
	return m.fence.Load().fenced
}

func (m *Manager) trip(ctx context.Context) {
	for {
		old := m.fence.Load()
		if old.fenced {
			return
		}
		if !m.fence.CompareAndSwap(old, &fenceState{ch: old.ch, fenced: true}) {
			continue
		}
		close(old.ch)
		log.WithFunc("pool.fence").Warnf(ctx, "node lease not renewed for %s; replicated claims answer 503 until it is", m.lease.ttl-m.lease.margin)
		return
	}
}

// unfence confirms each replicated claim against its row, then lifts the fence if that finished inside the window the renewal at start still covers.
func (m *Manager) unfence(ctx context.Context, start time.Time) {
	logger := log.WithFunc("pool.unfence")
	fenced := m.fence.Load()
	m.mu.Lock()
	held := map[string]*types.Sandbox{}
	for id, sb := range m.claimed {
		if sb.HasReplica() {
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
		keep, err := m.confirmOwnership(ctx, sb, rows[id])
		if err != nil {
			logger.Warnf(ctx, "replicated claims stay fenced: %v", err)
			return
		}
		if keep {
			continue
		}
		if td := m.forgetAdopted(ctx, sb); td != nil {
			go func() {
				sb.Transition.Lock()
				defer sb.Transition.Unlock()
				td()
			}()
		}
	}
	m.lift(ctx, fenced, start)
}

// lift replaces the fenced state this renewal saw, and trips again when the window closed before the lift landed.
func (m *Manager) lift(ctx context.Context, fenced *fenceState, start time.Time) {
	if !m.fence.CompareAndSwap(fenced, &fenceState{ch: make(chan struct{})}) {
		return
	}
	if time.Since(start) > m.lease.ttl-m.lease.margin {
		log.WithFunc("pool.unfence").Warn(ctx, "replicated claims stay fenced: confirming ownership outlasted the lease window")
		m.trip(ctx)
		return
	}
	log.WithFunc("pool.unfence").Info(ctx, "node lease renewed; replicated claims are served again")
}

// confirmOwnership takes sb's row for this node in one compare-and-set; false means another node holds the claim.
func (m *Manager) confirmOwnership(ctx context.Context, sb *types.Sandbox, rec archives.Record) (bool, error) {
	for attempt := range 3 {
		switch {
		case rec.ID == "":
			gone, err := m.replicaGone(ctx, sb)
			return !gone, err
		case rec.Owner != m.nodeID:
			return false, nil
		case rec.Consumed:
			return true, nil
		}
		if attempt == 2 {
			break
		}
		epoch, err := m.archives.Take(ctx, sb.ID, rec.Epoch, m.nodeID)
		if err == nil {
			_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = max(sb.ArchiveEpoch, epoch) })
			return true, nil
		}
		if !errors.Is(err, archives.ErrLost) {
			return false, err
		}
		var found bool
		if rec, found, err = m.archives.Get(ctx, sb.ID); err != nil {
			return false, err
		}
		if !found {
			rec = archives.Record{}
		}
	}
	return false, fmt.Errorf("confirm %s: its row kept moving under this node", sb.ID)
}

// fenceWatch gates sb at resolution and returns the channel that closes when its streams must end; nil for an unreplicated claim.
func (m *Manager) fenceWatch(sb *types.Sandbox) (<-chan struct{}, error) {
	if !sb.Replicated() {
		return nil, nil
	}
	st := m.fence.Load()
	if st.fenced {
		return nil, ErrLeaseLost
	}
	return st.ch, nil
}
