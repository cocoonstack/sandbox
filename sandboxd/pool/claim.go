// Claim hand-out, authorization, release, and lease reaping.
package pool

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// ClaimWarm transfers ownership of a warm sandbox without provisioning;
// ErrNoWarm means the pool is empty (the caller may redirect or provision).
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if m.overQuota(1) {
		return nil, fmt.Errorf("%w: cap %d", ErrQuota, m.maxClaims)
	}
	m.mu.Lock()
	var sb *types.Sandbox
	if p := m.pools[key]; p != nil {
		p.noteArrival(start)
		if n := len(p.warm); n > 0 {
			sb = p.warm[n-1]
			p.warm = p.warm[:n-1]
		}
	}
	m.mu.Unlock()
	if sb == nil {
		return nil, ErrNoWarm
	}
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox (golden clone or cold boot).
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if m.overQuota(1) {
		return nil, fmt.Errorf("%w: cap %d", ErrQuota, m.maxClaims)
	}
	golden := m.goldenDirFor(key)
	sb, err := m.provision(ctx, key, golden)
	if err != nil {
		return nil, err
	}
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		if golden != "" {
			m.counters.claimsClone.Add(1)
		} else {
			m.counters.claimsCold.Add(1)
		}
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// Release destroys a claimed sandbox after validating its token.
func (m *Manager) Release(ctx context.Context, id, token string) error {
	m.mu.Lock()
	sb, ok := m.authed(id, token)
	if !ok {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	delete(m.claimed, id)
	snap := sb.HibernateSnap
	saveErr := m.store.save(m.claimed)
	m.mu.Unlock()
	if saveErr != nil {
		log.WithFunc("pool.Release").Errorf(ctx, saveErr, "persist release of %s", id)
	}
	// The claim is already dropped; removal must survive the caller hanging up.
	err := m.eng.Remove(context.WithoutCancel(ctx), sb.VMName)
	m.dropSnap(ctx, snap)
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: id, VMName: sb.VMName})
	return err
}

// ClaimDeadline authorizes a sandbox by token and returns its lease
// deadline — the preview mint clamps a URL's life to it.
func (m *Manager) ClaimDeadline(id, token string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return time.Time{}, ErrUnknownSandbox
	}
	return sb.Deadline, nil
}

// PreviewDial opens a byte stream to a guest port for the preview server. The
// caller has already verified the signed preview token, so no sandbox token
// is needed; the live-claim lookup is the revocation check — a released or
// reaped sandbox is absent and this fails. A hibernated sandbox wakes.
func (m *Manager) PreviewDial(ctx context.Context, id string, port uint16) (net.Conn, error) {
	m.mu.Lock()
	sb, ok := m.claimed[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrUnknownSandbox
	}
	m.touch(sb) // a live preview stream is data-plane activity
	// Preview bypasses the relay's audit tap (it dials the engine directly),
	// so record the access here — the only data-plane entry that would
	// otherwise leave no audit trace.
	m.recordAudit(ctx, id, auditFrame{Op: "preview_dial", Port: port})
	sock, err := m.wakeResolved(ctx, sb)
	if err != nil {
		return nil, err
	}
	return m.eng.DialGuestPort(ctx, sock, port)
}

// AgentSocket resolves a claimed sandbox's vsock UDS without waking it (the
// ownership probe must not restore a hibernated VM).
func (m *Manager) AgentSocket(id, token string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return "", ErrUnknownSandbox
	}
	// No activity stamp here: owner/Lookup probes use this path, and a
	// control-plane poll must not keep an idle sandbox awake. The relay's
	// stamp lives in WakeAgentSocket.
	return sb.VsockSocket, nil
}

// overQuota is the cheap advisory precheck: the authoritative check stays
// in finalizeBatch (admission races resolve there), this one just spares a
// doomed request the provision cost.
func (m *Manager) overQuota(extra int) bool {
	if m.maxClaims <= 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.claimed)+extra > m.maxClaims
}

// finalize stamps identity, persists the claim, and destroys the VM if the
// store write fails so a durable claim always matches a live VM.
func (m *Manager) finalize(ctx context.Context, sb *types.Sandbox, ttl time.Duration) (*types.Sandbox, error) {
	if err := m.finalizeBatch(ctx, []*types.Sandbox{sb}, ttl); err != nil {
		return nil, err
	}
	return sb, nil
}

// finalizeBatch stamps identities and persists the claims as one journal
// write; on a failed write every VM in the batch is destroyed — a durable
// claim always matches a live VM, and a batch lands all-or-nothing.
func (m *Manager) finalizeBatch(ctx context.Context, sbs []*types.Sandbox, ttl time.Duration) error {
	now := time.Now()
	for _, sb := range sbs {
		stampIdentity(sb, clampTTL(ttl))
		sb.LastActivity = now
	}
	m.mu.Lock()
	if live := len(m.claimed); m.maxClaims > 0 && live+len(sbs) > m.maxClaims {
		m.mu.Unlock()
		for _, sb := range sbs {
			m.destroy(ctx, sb.VMName)
		}
		return fmt.Errorf("%w: %d live claims, cap %d", ErrQuota, live, m.maxClaims)
	}
	for _, sb := range sbs {
		m.claimed[sb.ID] = sb
	}
	saveErr := m.store.save(m.claimed)
	if saveErr != nil {
		for _, sb := range sbs {
			delete(m.claimed, sb.ID)
		}
	}
	m.mu.Unlock()
	if saveErr != nil {
		for _, sb := range sbs {
			m.destroy(ctx, sb.VMName)
		}
		return fmt.Errorf("persist claim: %w", saveErr)
	}
	for _, sb := range sbs {
		m.recordUsage(ctx, usageEvent{Event: "claim", ID: sb.ID, VMName: sb.VMName, KeyHash: sb.Key.Hash()})
	}
	return nil
}

func (m *Manager) reapOnce(ctx context.Context) {
	now := time.Now()
	type victim struct {
		id, vmName, snap string
	}
	m.mu.Lock()
	var expired []victim
	for id, sb := range m.claimed {
		if now.After(sb.Deadline) {
			expired = append(expired, victim{id: id, vmName: sb.VMName, snap: sb.HibernateSnap})
			delete(m.claimed, id)
		}
	}
	var saveErr error
	if len(expired) > 0 {
		saveErr = m.store.save(m.claimed)
	}
	m.mu.Unlock()

	logger := log.WithFunc("pool.reapOnce")
	if saveErr != nil {
		logger.Errorf(ctx, saveErr, "persist reap")
	}
	for _, v := range expired {
		m.destroy(ctx, v.vmName)
		m.dropSnap(ctx, v.snap)
		m.counters.reaps.Add(1)
		m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id, VMName: v.vmName})
		logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
	}
}

// touch records data-plane activity for the idle policy.
func (m *Manager) touch(sb *types.Sandbox) {
	m.mu.Lock()
	sb.LastActivity = time.Now()
	m.mu.Unlock()
}

func (m *Manager) claim(id, token string) (*types.Sandbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.authed(id, token)
}

// authed looks up a claim by id and token; callers hold m.mu.
func (m *Manager) authed(id, token string) (*types.Sandbox, bool) {
	sb := m.claimed[id]
	if sb == nil || subtle.ConstantTimeCompare([]byte(sb.Token), []byte(token)) != 1 {
		return nil, false
	}
	return sb, true
}

func stampIdentity(sb *types.Sandbox, ttl time.Duration) {
	sb.ID = "sb_" + randHex(8)
	sb.Token = randHex(16)
	sb.Deadline = time.Now().Add(ttl)
}

func clampTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultTTL
	}
	return min(ttl, maxTTL)
}
