package pool

import (
	"context"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Sandbox reports one live claim's summary, the single-sandbox read the
// whole-node listing otherwise forces a caller to scan for.
func (m *Manager) Sandbox(id string) (SandboxSummary, bool) {
	sb, ok := m.byID(id)
	if !ok {
		return SandboxSummary{}, false
	}
	return SandboxSummary{
		ID: sb.ID, Key: sb.Key, Deadline: sb.Deadline,
		Hibernated: sb.HibernateSnap != "", Archived: sb.ArchiveCk != "",
		FromCheckpoint: sb.FromCheckpoint, ClaimRef: sb.ClaimRef,
	}, true
}

// HibernateOperator hibernates a sandbox by id without a per-sandbox token.
func (m *Manager) HibernateOperator(ctx context.Context, id string) error {
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	return m.hibernateLocked(ctx, sb)
}

// Wake restores a hibernated sandbox and leaves it running, so a control plane
// can resume one it is not about to talk to — waking is otherwise only a side
// effect of opening an agent connection. Idempotent on a running sandbox.
func (m *Manager) Wake(ctx context.Context, id, token string) error {
	sb, ok := m.claim(id, token)
	if !ok {
		return ErrUnknownSandbox
	}
	return m.wake(ctx, sb)
}

// WakeOperator wakes a sandbox by id without a per-sandbox token.
func (m *Manager) WakeOperator(ctx context.Context, id string) error {
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	return m.wake(ctx, sb)
}

// byID resolves a live claim by id alone, with no ownership proof. The server
// authorizes the operator paths by root api_token before the call, so a tenant
// token never reaches one.
func (m *Manager) byID(id string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	return sb, sb != nil
}

// wake reuses the relay's resolve path, then discards the resolved socket: the
// caller wants the VM running, not a connection to it.
func (m *Manager) wake(ctx context.Context, sb *types.Sandbox) error {
	sb.Touch()
	_, err := m.wakeResolved(ctx, sb)
	return err
}
