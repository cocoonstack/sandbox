package pool

import (
	"cmp"
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
	balloonRetryDelay     = time.Minute
	bootBalloonTarget     = "25%"
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
		if !m.balloonReady() {
			return
		}
		v, now := m.view.Load(), time.Now()
		var due []*types.Sandbox
		pending := false
		m.mu.Lock()
		for _, sb := range m.claimed {
			pending = pending || sb.Balloon == types.BalloonInflated || sb.Balloon == types.BalloonUnknown
			if balloonDue(sb, balloonWant(sb, v, now)) && !m.balloonSkipLocked(sb) {
				due = append(due, sb)
			}
		}
		m.mu.Unlock()
		if v.balloonIdle == 0 && !pending {
			m.balloonSeen.Store(false)
		}
		m.runBounded(ctx, len(due), func(ctx context.Context, i int) {
			m.balloonTo(ctx, due[i], balloonWant(due[i], m.view.Load(), time.Now()))
		}).Wait()
	}()
}

// inflateForHibernate shrinks an idle claim's hibernate snapshot; the caller holds sb.Transition.
func (m *Manager) inflateForHibernate(ctx context.Context, sb *types.Sandbox) {
	if v := m.view.Load(); v.balloonIdle > 0 && m.balloonReady() {
		m.balloonTo(ctx, sb, v.balloonTarget)
	}
}

// balloonTo moves sb's balloon to want, an idle target, or back to its boot target when want is empty, without sb.Transition, which every relay and exec takes; cocoon serializes it with captures.
func (m *Manager) balloonTo(ctx context.Context, sb *types.Sandbox, want string) {
	logger := log.WithFunc("pool.balloonTo")
	m.mu.Lock()
	skip, due, state, def, vm := m.balloonSkipLocked(sb), balloonDue(sb, want), sb.Balloon, sb.BalloonDefault, sb.VMName
	m.mu.Unlock()
	if skip || !due {
		return
	}
	target := cmp.Or(want, strconv.FormatInt(def, 10))
	if want == "" && state == types.BalloonUnknown {
		rec, ok, err := m.eng.Inspect(ctx, vm)
		switch {
		case err != nil || !ok:
			m.balloonFailed(sb)
			logger.Warnf(ctx, "inspect balloon of %s: found %t, %v", sb.ID, ok, err)
			return
		case rec.Balloon == nil:
			target = bootBalloonTarget
		case rec.Balloon.Target <= rec.Balloon.Default:
			m.setBalloon(sb, "", "", 0)
			return
		default:
			target = strconv.FormatInt(rec.Balloon.Default, 10)
		}
	}
	b, err := m.eng.Balloon(ctx, vm, target)
	switch {
	case errors.Is(err, engine.ErrBalloonUnsupported):
		m.setBalloon(sb, types.BalloonUnsupported, "", 0)
	case err != nil:
		m.balloonFailed(sb)
		logger.Warnf(ctx, "balloon %s to %s: %v", sb.ID, target, err)
	case want != "":
		m.counters.balloonInflations.Add(1)
		m.setBalloon(sb, types.BalloonInflated, want, b.Default)
	default:
		m.counters.balloonDeflations.Add(1)
		m.setBalloon(sb, "", "", 0)
	}
}

// forgetBalloonLocked marks a claim another run or node may have inflated, so the sweep reads its target.
func (m *Manager) forgetBalloonLocked(sb *types.Sandbox) {
	if m.view.Load().balloonIdle > 0 {
		sb.Balloon = types.BalloonUnknown
	}
}

func (m *Manager) checkBalloon() bool {
	ctx := context.Background()
	if err := m.eng.CheckBalloonSupport(ctx); err != nil {
		log.WithFunc("pool.checkBalloon").Error(ctx, err, "balloon reclaim is off")
		return false
	}
	return true
}

func (m *Manager) balloonFailed(sb *types.Sandbox) {
	m.counters.balloonFailures.Add(1)
	m.mu.Lock()
	sb.BalloonRetryAt = time.Now().Add(balloonRetryDelay)
	m.mu.Unlock()
}

func (m *Manager) setBalloon(sb *types.Sandbox, state types.BalloonState, target string, def int64) {
	m.mu.Lock()
	sb.Balloon, sb.BalloonTarget, sb.BalloonDefault = state, target, def
	m.mu.Unlock()
}

func (m *Manager) balloonSkipLocked(sb *types.Sandbox) bool {
	return m.claimed[sb.ID] != sb || !holdsVM(sb) || sb.Waking || sb.PendingSnap != "" || sb.SaveSnap != "" ||
		sb.Balloon == types.BalloonUnsupported || time.Now().Before(sb.BalloonRetryAt) || m.midMoveLocked(sb)
}

// balloonDue reports whether sb's balloon needs a call to reach want, an idle target, or the boot target when want is empty; callers hold m.mu.
func balloonDue(sb *types.Sandbox, want string) bool {
	if want != "" {
		return sb.Balloon != types.BalloonInflated || sb.BalloonTarget != want
	}
	return sb.Balloon == types.BalloonInflated || sb.Balloon == types.BalloonUnknown
}

func balloonWant(sb *types.Sandbox, v *configView, now time.Time) string {
	if v.balloonIdle > 0 && !sb.Busy() && now.Sub(sb.LastSeen()) >= v.balloonIdle {
		return v.balloonTarget
	}
	return ""
}
