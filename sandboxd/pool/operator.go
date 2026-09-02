package pool

import (
	"context"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Cred is a caller's resolved authority over one sandbox: its token, or Operator for root.
type Cred struct {
	Token    string
	Operator bool
}

// Sandbox reports one live claim's summary.
func (m *Manager) Sandbox(id string) (SandboxSummary, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	if sb == nil {
		return SandboxSummary{}, false
	}
	// summarize reads fields hibernate and archive mutate under m.mu
	return summarize(sb), true
}

// Wake restores a hibernated sandbox and leaves it running. Idempotent.
func (m *Manager) Wake(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	return m.wake(ctx, sb)
}

// resolve authorizes id under cred; an unclaimed slot must never match an empty token.
func (m *Manager) resolve(id string, cred Cred) (*types.Sandbox, bool) {
	if cred.Operator {
		return m.byID(id)
	}
	if cred.Token == "" {
		return nil, false
	}
	return m.claim(id, cred.Token)
}

// byID resolves a live claim by id alone; callers must have authorized the Operator credential.
func (m *Manager) byID(id string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	return sb, sb != nil
}

// wake reuses the relay's resolve path, then discards the socket.
func (m *Manager) wake(ctx context.Context, sb *types.Sandbox) error {
	sb.Touch()
	_, err := m.wakeResolved(ctx, sb)
	return err
}
