package pool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type evacuation struct {
	pending map[string]*types.Sandbox // guarded by m.mu
	stop    context.CancelFunc
}

// Drain cordons the node: claims answer 429 and warm VMs are destroyed.
func (m *Manager) Drain(ctx context.Context) {
	var trim []string
	m.mu.Lock()
	m.draining = true
	for _, p := range m.pools {
		trim = append(trim, p.trimWarm(0)...)
	}
	m.mu.Unlock()
	m.destroyAll(context.WithoutCancel(ctx), trim).Wait()
}

// Evacuate drains the node, then archives each claim it can and releases its row, so a peer adopts it at once.
func (m *Manager) Evacuate(ctx context.Context) {
	m.Drain(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.evac != nil && len(m.evac.pending) > 0 {
		return
	}
	ctx, stop := context.WithCancel(context.WithoutCancel(ctx))
	m.evac = &evacuation{pending: maps.Clone(m.claimed), stop: stop}
	go m.runEvacuation(ctx, m.evac)
}

// Uncordon lifts a drain, stops an evacuation, and kicks an immediate refill.
func (m *Manager) Uncordon(ctx context.Context) {
	m.mu.Lock()
	m.draining = false
	e := m.evac
	m.evac = nil
	m.mu.Unlock()
	if e != nil {
		e.stop()
	}
	m.refillOnce(context.WithoutCancel(ctx))
}

// runEvacuation repeats its pass while a claim waits for an archive the sweeps run.
func (m *Manager) runEvacuation(ctx context.Context, e *evacuation) {
	defer e.stop()
	for {
		m.mu.Lock()
		batch := slices.SortedFunc(maps.Values(e.pending), func(a, b *types.Sandbox) int { return cmp.Compare(moveCost(a), moveCost(b)) })
		m.mu.Unlock()
		// One budget's worth at a time leaves crash recovery and reaps their FIFO turn.
		for chunk := range slices.Chunk(batch, cap(m.maintSem)) {
			m.runBounded(ctx, len(chunk), func(ctx context.Context, i int) { m.evacuateOne(ctx, e, chunk[i]) }).Wait()
		}
		m.mu.Lock()
		left := len(e.pending)
		m.mu.Unlock()
		if left == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reapInterval):
		}
	}
}

func (m *Manager) evacuateOne(ctx context.Context, e *evacuation, sb *types.Sandbox) {
	err := m.evacuateClaim(ctx, sb)
	if errors.Is(err, errArchiveBusy) {
		return
	}
	m.mu.Lock()
	skip := err != nil && m.claimed[sb.ID] == sb
	delete(e.pending, sb.ID)
	m.mu.Unlock()
	if skip {
		log.WithFunc("pool.evacuateOne").Warnf(ctx, "evacuation skips %s: %v", sb.ID, err)
	}
}

func (m *Manager) evacuateClaim(ctx context.Context, sb *types.Sandbox) error {
	if m.archives == nil {
		return errNoFailover
	}
	m.mu.Lock()
	_, busy := m.archiving[sb.ID]
	gone := m.claimed[sb.ID] != sb || !m.evacuatingLocked(sb.ID)
	archived, tenantGone := sb.ArchiveCk != "", m.tenantGone(sb.Tenant)
	pause := !gone && !tenantGone && !busy && !archived
	if pause {
		m.archiving[sb.ID] = struct{}{}
	}
	m.mu.Unlock()
	switch {
	case gone:
		return nil
	case tenantGone:
		return ErrTenantRemoved
	case busy:
		return errArchiveBusy
	case pause:
		if err := m.pauseArchive(ctx, sb); err != nil {
			return err
		}
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return m.handOff(ctx, sb)
}

// Caller holds Transition.
func (m *Manager) handOff(ctx context.Context, sb *types.Sandbox) error {
	m.mu.Lock()
	live, archived := m.claimed[sb.ID] == sb && m.evacuatingLocked(sb.ID), sb.ArchiveCk != ""
	epoch, token := sb.ArchiveEpoch, sb.Token
	m.mu.Unlock()
	switch {
	case !live:
		return nil
	case !archived:
		return errArchiveBusy
	case epoch == 0:
		return errNoArchiveRow
	}
	ctx = context.WithoutCancel(ctx)
	released, err := m.archives.Release(ctx, sb.ID, epoch, m.nodeID)
	if errors.Is(err, archives.ErrLost) {
		return m.resumeHandOff(ctx, sb, token)
	}
	if err != nil {
		return fmt.Errorf("hand off %s: %w", sb.ID, err)
	}
	if !m.peerAdopt(ctx, sb.ID, token) {
		return m.takeBack(ctx, sb, released)
	}
	m.dropAdopted(ctx, sb)
	return nil
}

// A row with no owner may hold a later owner's writes, so it outranks this claim and only a peer may take it.
func (m *Manager) resumeHandOff(ctx context.Context, sb *types.Sandbox, token string) error {
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	switch {
	case err != nil:
		return fmt.Errorf("hand off %s: %w", sb.ID, err)
	case !ok || rec.Owner != "":
		if err = m.refusedReplace(ctx, sb); err == nil {
			err = archives.ErrLost
		}
		return fmt.Errorf("hand off %s: %w", sb.ID, err)
	case !m.peerAdopt(ctx, sb.ID, token):
		return errNoAdopter
	}
	m.dropAdopted(ctx, sb)
	return nil
}

func (m *Manager) takeBack(ctx context.Context, sb *types.Sandbox, released int64) error {
	epoch, err := m.archives.Take(ctx, sb.ID, released, m.nodeID)
	if errors.Is(err, archives.ErrLost) {
		return m.refusedReplace(ctx, sb)
	}
	if err != nil {
		return fmt.Errorf("take back %s: %w", sb.ID, err)
	}
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
	return errNoAdopter
}

func (m *Manager) evacuatingLocked(id string) bool {
	return m.evac != nil && m.evac.pending[id] != nil
}

// A parked claim answers 503 until its turn, so it moves first.
func moveCost(sb *types.Sandbox) int {
	switch {
	case sb.ArchiveCk != "":
		return 0
	case sb.HibernateSnap != "":
		return 1
	}
	return 2
}
