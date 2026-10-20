package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

func (m *Manager) removeVM(ctx context.Context, name string) bool {
	if m.managedUncertainVM(name) || m.growthOwnsVM(name) {
		return false
	}
	target, allowed := m.growthTeardownTarget(ctx, name)
	if !allowed {
		return false
	}
	m.out.ClosePrebound(name)
	// cancellation-immune: on a canceled ctx `cocoon vm rm` no-ops and orphans.
	ctx = context.WithoutCancel(ctx)
	hold, managed := m.managedHoldByVM(name)
	if managed {
		if err := m.retireManaged(hold.SandboxID); err != nil {
			if hold.Sleep != nil || m.growthOwnsHold(hold.SandboxID) {
				return false
			}
			_ = m.eng.Remove(ctx, target)
			return false
		}
	}
	if managed && hold.Sleep != nil {
		return m.removeManagedSleep(ctx, hold)
	}
	err := m.eng.Remove(ctx, target)
	if managed {
		return m.confirmGone(ctx, target) && m.managed.processGone(hold.Process)
	}
	if err == nil {
		return true
	}
	log.WithFunc("pool.removeVM").Warnf(ctx, "remove vm %s: %v; verifying", name, err)
	return m.confirmGone(ctx, target)
}

func (m *Manager) confirmGone(ctx context.Context, name string) bool {
	ctx, cancel := context.WithTimeout(ctx, removeVerifyTimeout)
	defer cancel()
	logger := log.WithFunc("pool.confirmGone")
	_, present, err := m.eng.Inspect(ctx, name)
	if err != nil {
		// cannot tell: a false "gone" leaks a running VM, a retry only costs a sweep
		logger.Warnf(ctx, "verify remove of %s: %v", name, err)
		return false
	}
	if present {
		logger.Error(ctx, fmt.Errorf("vm %s survived removal", name), "remove did not take effect; leaving it accounted for retry")
		return false
	}
	return true
}

// removeOrRetry reports whether the VM is confirmed gone; a survivor is queued for the reap tick.
func (m *Manager) removeOrRetry(ctx context.Context, name, sandboxID string, td volumeTeardown) bool {
	if h, ok := m.managedHoldByVM(name); ok {
		td.managedID = h.SandboxID
	}
	if m.removeVM(ctx, name) {
		m.finishVolumeTeardown(ctx, td)
		if td.managedID != "" {
			if _, held := m.managedHold(td.managedID); held {
				m.queueRemoval(name, sandboxID, "", td, time.Time{})
				return false
			}
		}
		return true
	}
	m.queueRemoval(name, sandboxID, "", td, time.Time{})
	return false
}

func (m *Manager) queueRemoval(name, sandboxID, tap string, td volumeTeardown, notBefore time.Time) {
	m.mu.Lock()
	m.pendingRemovals[name] = pendingRemoval{sandboxID: sandboxID, tap: tap, volumes: td, notBefore: notBefore}
	m.mu.Unlock()
}

func (m *Manager) queueStaleCreate(name, tap string) {
	m.mu.Lock()
	m.pendingRemovals[name] = pendingRemoval{tap: tap, staleCreate: true}
	m.mu.Unlock()
}

// retryRemovals moves the due entries out of the queue to dispatch, so the next tick cannot double-dispatch.
func (m *Manager) retryRemovals(ctx context.Context) *sync.WaitGroup {
	now := time.Now()
	batch := map[string]pendingRemoval{}
	m.mu.Lock()
	for name, pending := range m.pendingRemovals {
		if !now.Before(pending.notBefore) {
			batch[name] = pending
			delete(m.pendingRemovals, name)
		}
	}
	m.mu.Unlock()
	names := slices.Collect(maps.Keys(batch))
	return m.runBounded(ctx, len(names), func(ctx context.Context, i int) {
		m.retryRemoval(ctx, names[i], batch[names[i]])
	})
}

func (m *Manager) retryRemoval(ctx context.Context, name string, pending pendingRemoval) {
	if m.growthOwnsVM(name) {
		m.queueRemoval(name, pending.sandboxID, pending.tap, pending.volumes, pending.notBefore)
		return
	}
	if m.retryManagedSleepRemoval(ctx, name, pending) {
		return
	}
	if pending.staleCreate {
		switch outcome, err := m.eng.ReconcileStaleCreate(ctx, name); {
		case err != nil:
			log.WithFunc("pool.retryRemoval").Warnf(ctx, "reconcile stale create %s: %v; retrying", name, err)
			m.queueStaleCreate(name, pending.tap)
			return
		case outcome == engine.StaleCreateBusy:
			m.queueStaleCreate(name, pending.tap)
			return
		case outcome == engine.StaleCreateCollected, outcome == engine.StaleCreateNotFound:
			m.finishRemoval(ctx, pending)
			return
		}
	}
	if !m.removeVM(ctx, name) {
		m.queueRemoval(name, pending.sandboxID, pending.tap, pending.volumes, time.Time{})
		return
	}
	m.finishRemoval(ctx, pending)
	if pending.volumes.managedID != "" {
		if _, held := m.managedHold(pending.volumes.managedID); held {
			m.queueRemoval(name, pending.sandboxID, pending.tap, pending.volumes, time.Time{})
		}
	}
}

func (m *Manager) finishRemoval(ctx context.Context, pending pendingRemoval) {
	m.finishVolumeTeardown(ctx, pending.volumes)
	if pending.sandboxID != "" {
		m.out.Disarm(pending.sandboxID, true)
	} else if pending.tap != "" {
		_ = m.out.UnlockTap(pending.tap)
	}
}

func (m *Manager) destroy(ctx context.Context, name string) {
	m.removeOrRetry(ctx, name, "", volumeTeardown{})
}
