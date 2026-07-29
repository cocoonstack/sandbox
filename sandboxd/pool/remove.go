package pool

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
)

func (m *Manager) removeVM(ctx context.Context, name string) bool {
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

func (m *Manager) removeOrRetry(ctx context.Context, name, sandboxID string) bool {
	if m.removeVM(ctx, name) {
		return true
	}
	m.queueRemoval(name, sandboxID, "")
	return false
}

func (m *Manager) queueRemoval(name, sandboxID, tap string) {
	m.mu.Lock()
	pending := m.pendingRemovals[name]
	if pending.sandboxID == "" {
		pending.sandboxID = sandboxID
	}
	if pending.tap == "" {
		pending.tap = tap
	}
	m.pendingRemovals[name] = pending
	m.mu.Unlock()
}

func (m *Manager) retryRemovals(ctx context.Context) {
	m.mu.Lock()
	names := make([]string, 0, len(m.pendingRemovals))
	for name, pending := range m.pendingRemovals {
		if pending.retrying {
			continue
		}
		pending.retrying = true
		m.pendingRemovals[name] = pending
		names = append(names, name)
	}
	m.mu.Unlock()
	m.runBounded(ctx, len(names), func(ctx context.Context, i int) {
		m.retryRemoval(ctx, names[i])
	})
}

func (m *Manager) retryRemoval(ctx context.Context, name string) {
	removed := m.removeVM(ctx, name)
	m.mu.Lock()
	pending, ok := m.pendingRemovals[name]
	if !ok {
		m.mu.Unlock()
		return
	}
	if removed {
		delete(m.pendingRemovals, name)
	} else {
		pending.retrying = false
		m.pendingRemovals[name] = pending
	}
	m.mu.Unlock()
	if removed && pending.sandboxID != "" {
		m.disarmEgress(pending.sandboxID, true)
	} else if removed && pending.tap != "" {
		_ = netfilter.Unlock(pending.tap)
	}
}

func (m *Manager) destroy(ctx context.Context, name string) {
	m.removeOrRetry(ctx, name, "")
}
