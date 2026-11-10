package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// kickRefill nudges Run past its refill ticker without blocking the claim path.
func (m *Manager) kickRefill() {
	select {
	case m.refillKick <- struct{}{}:
	default:
	}
}

func (m *Manager) kickBalloon() {
	select {
	case m.balloonKick <- struct{}{}:
	default:
	}
}

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	// Spawning VMs into a full node still takes the global rtnl lock even when doomed to fail
	parked := m.closedLocked() || m.capacityReasonLocked(now) != ""
	inFlight := 0
	for _, p := range m.pools {
		inFlight += p.refilling
		// applyPools leaves a removed pool in place while a build, refill or claim clone is in flight
		if p.removed {
			if p.settled() {
				m.dropPoolLocked(ctx, p)
			}
			continue
		}
		if !parked && p.goldenDir == "" && !p.building && now.After(p.nextBuild) {
			p.building = true
			go m.buildGolden(ctx, p)
		}
	}
	if parked {
		return
	}
	limit := cap(m.refillSem) + cap(m.probeSem)
	for spawned := true; spawned && inFlight < limit; {
		spawned = false
		for _, p := range m.pools {
			if inFlight >= limit {
				return
			}
			if p.removed || p.goldenDir == "" || p.refillGated(now) ||
				len(p.warm)+p.refilling >= p.effectiveTarget(now) {
				continue
			}
			select {
			case m.refillSem <- struct{}{}:
				p.refilling++
				inFlight++
				go m.refillOne(ctx, p, p.goldenDir, p.goldenGen)
				spawned = true
			default:
				return
			}
		}
	}
}

// shrinkOnce destroys the warm VMs a decayed target no longer wants; refill only ever fills.
func (m *Manager) shrinkOnce(ctx context.Context) {
	var trim []string
	m.mu.Lock()
	now := time.Now()
	for _, p := range m.pools {
		trim = append(trim, p.shrink(now)...)
	}
	m.mu.Unlock()
	m.destroyAll(ctx, trim)
}

func (m *Manager) refillOne(ctx context.Context, p *pool, golden string, gen uint64) {
	start := time.Now()
	sb, err := m.startVM(ctx, p.key, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, p.key)
	})
	cloneErr := err
	<-m.refillSem
	if err == nil {
		if ctx.Err() == nil {
			m.refillOnce(ctx)
		}
		sb, err = m.readyBounded(ctx, sb, time.Now().Add(warmProbeTimeout))
	}
	if err == nil {
		err = m.warmClone(ctx, sb)
	}
	if err == nil {
		m.out.Prebind(ctx, sb)
	}
	keep := false
	var fails int
	var wait time.Duration
	capReason := engine.CapacitySignature(err)
	m.mu.Lock()
	now := time.Now()
	p.refilling--
	if err == nil && !m.closedLocked() && p.goldenDir != "" && p.goldenGen == gen && len(p.warm) < p.effectiveTarget(now) {
		p.warm = append(p.warm, sb)
		p.noteLead(time.Since(start))
		keep = true
	}
	// A canceled context or a capacity failure says nothing about this pool's health
	backedOff := false
	if p.countClone(ctx, gen, cloneErr, capReason) {
		backedOff = p.noteRefillResult(now, err != nil)
	}
	if backedOff {
		fails, wait = p.refillFails, time.Until(p.nextRefill)
	}
	retired := m.retireUnclonableLocked(p, gen, now)
	entered := capReason != "" && m.noteCapacityLocked(now, capReason)
	parked := m.capacityReasonLocked(now) != ""
	m.mu.Unlock()
	if err != nil {
		logger := log.WithFunc("pool.refillOne")
		switch {
		case ctx.Err() != nil:
		case entered:
			logger.Errorf(ctx, err, "node is at capacity; parking refill for %s", capacityBackoff)
		case parked:
		default:
			hash := p.hash
			logger.Errorf(ctx, err, "refill %s", hash)
			if backedOff {
				logger.Warnf(ctx,
					"refill %s failed %d times running; pausing refill for %s",
					hash, fails, wait.Round(time.Millisecond))
			}
			if retired {
				m.rebuildRetiredGolden(ctx, hash)
			}
		}
		return
	}
	if ctx.Err() == nil {
		m.refillOnce(ctx)
	}
	if !keep {
		m.destroy(ctx, sb.VMName)
	}
}

// noteCapacityLocked parks refill node-wide, reporting whether this call parked it.
func (m *Manager) noteCapacityLocked(now time.Time, reason string) bool {
	first := !now.Before(m.atCapacityUntil)
	m.atCapacityUntil = now.Add(capacityBackoff)
	m.atCapacityReason = reason
	return first
}

func (m *Manager) capacityReasonLocked(now time.Time) string {
	switch {
	case m.diskLow.Load():
		return diskLowReason
	case m.memoryLow.Load():
		return memoryLowReason
	case now.Before(m.atCapacityUntil):
		return m.atCapacityReason
	}
	return ""
}

// A restore maps the golden memory lazily, so the warmup's pages fault in here instead of under the first claim
func (m *Manager) warmClone(ctx context.Context, sb *types.Sandbox) error {
	if err := m.runWarmup(ctx, m.view.Load().poolWarmups[sb.Key], sb.VsockSocket); err != nil {
		m.destroy(ctx, sb.VMName)
		return fmt.Errorf("clone warmup: %w", err)
	}
	return nil
}
