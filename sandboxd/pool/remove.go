package pool

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
)

func (m *Manager) removeVM(ctx context.Context, name string) bool {
	// Cancellation-immune: on a canceled ctx `cocoon vm rm` no-ops and orphans.
	ctx = context.WithoutCancel(ctx)
	err := m.eng.Remove(ctx, name)
	if err == nil {
		return true
	}
	log.WithFunc("pool.removeVM").Warnf(ctx, "remove vm %s: %v; verifying", name, err)
	return m.confirmGone(ctx, name)
}

func (m *Manager) confirmGone(ctx context.Context, name string) bool {
	ctx, cancel := context.WithTimeout(ctx, removeVerifyTimeout)
	defer cancel()
	_, present, err := m.findVM(ctx, name)
	if err != nil {
		// Cannot tell: a false "gone" leaks a running VM, a retry only costs a sweep.
		log.WithFunc("pool.confirmGone").Warnf(ctx, "verify remove of %s: %v", name, err)
		return false
	}
	if present {
		log.WithFunc("pool.confirmGone").Errorf(ctx,
			fmt.Errorf("vm %s survived removal", name),
			"remove did not take effect; leaving it accounted for retry")
		return false
	}
	return true
}

// removeOrRetry reports whether the VM is confirmed gone; a survivor is queued for the reap tick.
func (m *Manager) removeOrRetry(ctx context.Context, name, sandboxID, tap string, td volumeTeardown) bool {
	if m.removeVM(ctx, name) {
		m.finishVolumeTeardown(ctx, td)
		return true
	}
	m.queueRemoval(name, sandboxID, tap, td)
	return false
}

func (m *Manager) queueRemoval(name, sandboxID, tap string, td volumeTeardown) {
	m.mu.Lock()
	m.pendingRemovals[name] = pendingRemoval{sandboxID: sandboxID, tap: tap, volumes: td}
	m.mu.Unlock()
}

func (m *Manager) queueStaleCreate(name, tap string) {
	m.mu.Lock()
	m.pendingRemovals[name] = pendingRemoval{tap: tap, staleCreate: true}
	m.mu.Unlock()
}

// retryRemovals empties the queue to dispatch, so the next tick cannot double-dispatch.
func (m *Manager) retryRemovals(ctx context.Context) *sync.WaitGroup {
	m.mu.Lock()
	if len(m.pendingRemovals) == 0 {
		m.mu.Unlock()
		return new(sync.WaitGroup)
	}
	batch := m.pendingRemovals
	m.pendingRemovals = map[string]pendingRemoval{}
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
		m.queueRemoval(name, pending.sandboxID, pending.tap, pending.volumes)
		return
	}
	m.finishRemoval(ctx, pending)
}

func (m *Manager) finishRemoval(ctx context.Context, pending pendingRemoval) {
	m.finishVolumeTeardown(ctx, pending.volumes)
	if pending.sandboxID != "" {
		m.disarmEgress(pending.sandboxID, true)
	} else if pending.tap != "" {
		_ = netfilter.Unlock(pending.tap)
	}
}

func (m *Manager) destroy(ctx context.Context, name string) {
	m.removeOrRetry(ctx, name, "", "", volumeTeardown{})
}
