package pool

import (
	"context"
	"crypto/subtle"

	"github.com/cocoonstack/sandbox/sandboxd/tenants"
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
	out.Token, out.TunAddr, out.TunOff, out.TunProfile, out.EgressLimit = sb.Token, sb.TunAddr, sb.TunOff, sb.TunProfile, sb.EgressLimit
	if sb.TokenSHA256 != "" {
		out.Token = ""
	}
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

func (m *Manager) resolve(id string, cred Cred) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolveLocked(id, cred)
}

func (m *Manager) resolveLocked(id string, cred Cred) (*types.Sandbox, bool) {
	if cred.Operator {
		sb := m.claimed[id]
		return sb, sb != nil
	}
	return m.authed(id, cred.Token)
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
	switch {
	case sb == nil || token == "":
		return nil, false
	case sb.TokenSHA256 != "":
		if !tokenMatches(token, sb.TokenSHA256) {
			return nil, false
		}
		sb.Token, sb.TokenSHA256 = token, ""
	case subtle.ConstantTimeCompare([]byte(sb.Token), []byte(token)) != 1:
		return nil, false
	}
	return sb, true
}

func tokenMatches(token, sum string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(tenants.TokenSHA256(token)), []byte(sum)) == 1
}
