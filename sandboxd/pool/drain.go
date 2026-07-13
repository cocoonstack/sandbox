package pool

import "context"

// Drain cordons the node for maintenance: claim/fork/branch answer 429 like
// a node at max_claims (a warm peer wins the redirect first on a cluster),
// unclaimed warm VMs are destroyed, and live claims run to their leases.
// Deliberately not persisted — a restarted node serves again.
func (m *Manager) Drain(ctx context.Context) {
	var trim []string
	m.mu.Lock()
	m.draining = true
	for _, p := range m.pools {
		for _, sb := range p.warm {
			trim = append(trim, sb.VMName)
		}
		p.warm = p.warm[:0]
	}
	m.mu.Unlock()
	m.runBounded(context.WithoutCancel(ctx), len(trim), func(ctx context.Context, i int) {
		m.destroy(ctx, trim[i])
	}).Wait()
}

// Uncordon lifts a drain and kicks an immediate refill.
func (m *Manager) Uncordon(ctx context.Context) {
	m.mu.Lock()
	m.draining = false
	m.mu.Unlock()
	m.refillOnce(context.WithoutCancel(ctx))
}
