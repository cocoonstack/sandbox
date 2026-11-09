package pool

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	defaultBalloonPercent = 50

	balloonSupported int32 = 1
	balloonMissing   int32 = 2
)

// balloonOnce inflates the balloon of a claim idle past balloon.idle_seconds and deflates one active again, off every request path.
func (m *Manager) balloonOnce(ctx context.Context) {
	if m.view.Load().balloonIdle > 0 {
		m.balloonSeen.Store(true)
	}
	if !m.balloonSeen.Load() || !m.sweeps.balloon.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.sweeps.balloon.Store(false)
		if !m.balloonReady(ctx) {
			return
		}
		v, now := m.view.Load(), time.Now()
		var due []*types.Sandbox
		m.mu.Lock()
		for _, sb := range m.claimed {
			idle := balloonIdle(sb, v, now)
			if !m.balloonSkipLocked(sb) && (idle != (sb.Balloon == types.BalloonInflated) || !idle && sb.Balloon == types.BalloonUnknown) {
				due = append(due, sb)
			}
		}
		m.mu.Unlock()
		m.runBounded(ctx, len(due), func(ctx context.Context, i int) {
			m.balloonTo(ctx, due[i], balloonIdle(due[i], m.view.Load(), time.Now()))
		}).Wait()
	}()
}

// inflateForHibernate shrinks an idle claim's hibernate snapshot; the caller holds sb.Transition.
func (m *Manager) inflateForHibernate(ctx context.Context, sb *types.Sandbox) {
	if m.view.Load().balloonIdle > 0 && m.balloonReady(ctx) {
		m.balloonTo(ctx, sb, true)
	}
}

// balloonTo moves sb's balloon to the idle target or back to its boot default without sb.Transition, which every relay and exec takes; cocoon serializes it with captures.
func (m *Manager) balloonTo(ctx context.Context, sb *types.Sandbox, inflate bool) {
	logger := log.WithFunc("pool.balloonTo")
	v := m.view.Load()
	m.mu.Lock()
	skip, state, def, vm := m.balloonSkipLocked(sb), sb.Balloon, sb.BalloonDefault, sb.VMName
	m.mu.Unlock()
	switch {
	case skip, inflate && state == types.BalloonInflated, !inflate && state == "":
		return
	case !inflate && state == types.BalloonUnknown:
		rec, ok, err := m.eng.Inspect(ctx, vm)
		if err != nil || !ok {
			m.counters.balloonFailures.Add(1)
			logger.Warnf(ctx, "inspect balloon of %s: found %t, %v", sb.ID, ok, err)
			return
		}
		if rec.Balloon == nil || rec.Balloon.Target <= rec.Balloon.Default {
			m.setBalloon(sb, "", 0)
			return
		}
		def = rec.Balloon.Default
	}
	target := strconv.FormatInt(def, 10)
	if inflate {
		target = v.balloonTarget
	}
	b, err := m.eng.Balloon(ctx, vm, target)
	switch {
	case errors.Is(err, engine.ErrBalloonUnsupported):
		m.setBalloon(sb, types.BalloonUnsupported, 0)
	case err != nil:
		m.counters.balloonFailures.Add(1)
		logger.Warnf(ctx, "balloon %s to %s: %v", sb.ID, target, err)
	case inflate:
		m.counters.balloonInflations.Add(1)
		m.setBalloon(sb, types.BalloonInflated, b.Default)
	default:
		m.counters.balloonDeflations.Add(1)
		m.setBalloon(sb, "", 0)
	}
}

// forgetBalloon marks a claim another run or node may have inflated, so the sweep reads its target; callers hold m.mu.
func (m *Manager) forgetBalloon(sb *types.Sandbox) {
	if m.view.Load().balloonIdle > 0 {
		sb.Balloon = types.BalloonUnknown
	}
}

func (m *Manager) balloonReady(ctx context.Context) bool {
	switch m.balloonSupport.Load() {
	case balloonSupported:
		return true
	case balloonMissing:
		return false
	}
	if err := m.eng.CheckBalloonSupport(ctx); err != nil {
		m.balloonSupport.Store(balloonMissing)
		log.WithFunc("pool.balloonReady").Error(ctx, err, "balloon reclaim is off")
		return false
	}
	m.balloonSupport.Store(balloonSupported)
	return true
}

func (m *Manager) setBalloon(sb *types.Sandbox, state types.BalloonState, def int64) {
	m.mu.Lock()
	sb.Balloon, sb.BalloonDefault = state, def
	m.mu.Unlock()
}

func (m *Manager) balloonSkipLocked(sb *types.Sandbox) bool {
	return m.claimed[sb.ID] != sb || !holdsVM(sb) || sb.Waking || sb.PendingSnap != "" || sb.SaveSnap != "" ||
		sb.Balloon == types.BalloonUnsupported || m.midMoveLocked(sb)
}

func balloonIdle(sb *types.Sandbox, v *configView, now time.Time) bool {
	return v.balloonIdle > 0 && !sb.Busy() && now.Sub(sb.LastSeen()) >= v.balloonIdle
}
