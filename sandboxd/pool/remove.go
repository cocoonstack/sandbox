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
	m.out.ClosePrebound(name)
	// Cancellation-immune: on a canceled ctx `cocoon vm rm` no-ops and orphans.
	ctx = context.WithoutCancel(ctx)
	err := m.eng.Remove(ctx, name)
	if err == nil {
		return true
	}
	logger := log.WithFunc("pool.removeVM")
	logger.Warnf(ctx, "remove vm %s: %v; verifying", name, err)
	ctx, cancel := context.WithTimeout(ctx, removeVerifyTimeout)
	defer cancel()
	_, present, err := m.eng.Inspect(ctx, name)
	if err != nil {
		// Cannot tell: a false "gone" leaks a running VM, a retry only costs a sweep
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
	if m.removeVM(ctx, name) {
		m.finishVolumeTeardown(ctx, td)
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

func (m *Manager) destroyAll(ctx context.Context, names []string) *sync.WaitGroup {
	return m.runBounded(ctx, len(names), func(ctx context.Context, i int) { m.destroy(ctx, names[i]) })
}

func (m *Manager) dropSnap(ctx context.Context, snap string) {
	if snap == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.SnapshotRemove(ctx, snap); err != nil {
		log.WithFunc("pool.dropSnap").Warnf(ctx, "drop snapshot %s: %v", snap, err)
	}
}
