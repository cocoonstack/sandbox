package pool

import (
	"context"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// The operator paths let a control plane holding the node's root api_token
// drive a sandbox's lifecycle without its per-sandbox token. They exist
// because a stateless aggregated control plane cannot keep one secret per
// sandbox without turning O(nodes) storage into O(sandboxes) — the property
// the whole design rests on. ReleaseOperator established the pattern; these
// follow it exactly: the server authorizes by root api_token before calling,
// so no token check happens here, and a tenant token never reaches them.

// byID resolves a live claim by id alone, with no ownership proof.
func (m *Manager) byID(id string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	return sb, sb != nil
}

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

// Wake restores a hibernated sandbox and leaves it running, the explicit
// counterpart to Hibernate. Waking is otherwise only a side effect of opening
// an agent connection, which gives a control plane no way to resume a sandbox
// it is not about to talk to. Idempotent on an already-running sandbox.
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

// wake is the body shared by both wake entry points. It reuses the relay's
// resolve path, which restores through cocoon's mmap fast path (~55 ms) and
// queues concurrent wakes on the transition lock, then discards the resolved
// socket: the caller wants the VM running, not a connection to it.
func (m *Manager) wake(ctx context.Context, sb *types.Sandbox) error {
	sb.Touch()
	_, err := m.wakeResolved(ctx, sb)
	return err
}
