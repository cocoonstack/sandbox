package pool

import "context"

// Drain cordons the node: claims answer 429 and warm VMs are destroyed. Not persisted.
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

// Uncordon lifts a drain and kicks an immediate refill.
func (m *Manager) Uncordon(ctx context.Context) {
	m.mu.Lock()
	m.draining = false
	m.mu.Unlock()
	m.refillOnce(context.WithoutCancel(ctx))
}
