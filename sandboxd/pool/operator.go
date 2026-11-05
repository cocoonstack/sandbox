package pool

import (
	"context"
	"crypto/subtle"

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
	out := summarize(sb)
	out.Token = sb.Token
	return out, true
}

// Wake restores a hibernated sandbox, or cold-boots a failed one, and leaves it running. Idempotent.
func (m *Manager) Wake(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	if _, err := m.fenceWatch(sb); err != nil {
		return err
	}
	if err := m.restartFailed(ctx, sb); err != nil {
		return err
	}
	sb.Touch()
	_, err := m.wakeResolved(ctx, sb)
	return err
}

// SetInstanceMetadata replaces a running sandbox's instance-metadata document; a paused or archived one is refused, not woken.
func (m *Manager) SetInstanceMetadata(ctx context.Context, id string, doc []byte) error {
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	if !sb.Transition.TryLock() {
		return ErrPaused
	}
	defer sb.Transition.Unlock()
	switch {
	case sb.ArchiveCk != "":
		return ErrArchived
	case sb.Failed != "":
		return ErrFailed
	case !runningShaped(sb):
		return ErrPaused
	}
	return m.eng.WriteInstanceMetadata(ctx, sb.VsockSocket, doc)
}

// resolve authorizes id under cred.
func (m *Manager) resolve(id string, cred Cred) (*types.Sandbox, bool) {
	if cred.Operator {
		return m.byID(id)
	}
	return m.claim(id, cred.Token)
}

// byID resolves a live claim by id alone; the caller authorizes the request.
func (m *Manager) byID(id string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	return sb, sb != nil
}

func (m *Manager) claim(id, token string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authed(id, token)
}

// Callers hold m.mu.
func (m *Manager) authed(id, token string) (*types.Sandbox, bool) {
	sb := m.claimed[id]
	if sb == nil || subtle.ConstantTimeCompare([]byte(sb.Token), []byte(token)) != 1 {
		return nil, false
	}
	return sb, true
}
