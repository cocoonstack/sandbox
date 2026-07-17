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

// reapAction is what reapOnce does with an expired claim.
type reapAction int

const (
	reapDestroy reapAction = iota // engine teardown + drop snapshot
	reapArchive                   // hibernated + archive-enabled: archive, keep the claim
	reapPurge                     // archived past retention: delete the store checkpoint
)

// ClaimWarm transfers ownership of a warm sandbox without provisioning;
// ErrNoWarm means the pool is empty (the caller may redirect or provision).
// tenant attributes the claim; empty means the operator (root).
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant string) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if err := m.overQuota(1, tenant); err != nil {
		return nil, err
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
	m.kickRefill()
	sb.Tenant = tenant
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox (golden clone or cold boot).
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant string) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	if err := m.overQuota(1, tenant); err != nil {
		return nil, err
	}
	golden, release, err := m.resolveGolden(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("resolve template: %w", err)
	}
	defer release()
	sb, err := m.provision(ctx, key, golden)
	if err != nil {
		return nil, err
	}
	sb.Tenant = tenant
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
	m.tenantDelta(sb.Tenant, -1)
	snap, ck, vmName := sb.HibernateSnap, sb.ArchiveCk, sb.VMName
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		m.claimed[id] = sb // roll back so memory matches the still-durable claim; ck stays pinned
		m.tenantDelta(sb.Tenant, 1)
		rb := m.store.snapshot(m.claimed)
		m.mu.Unlock()
		m.recommit(ctx, rb)
		log.WithFunc("pool.Release").Errorf(ctx, saveErr, "persist release of %s", id)
		return fmt.Errorf("release %s: %w", id, saveErr)
	}
	// Cleanup must survive the caller hanging up; the claim is already dropped.
	ctx = context.WithoutCancel(ctx)
	if ck != "" {
		m.purgeArchiveCk(ctx, id, ck, sb.Tenant) // archived: no local VM
	}
	var err error
	if vmName != "" {
		err = m.eng.Remove(ctx, vmName)
	}
	m.disarmEgress(id, err == nil)
	m.dropSnap(ctx, snap)
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: id, VMName: vmName})
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
	sb.Touch() // a live preview stream is data-plane activity
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

// overQuota is a cheap advisory precheck; finalizeBatch does the authoritative
// admission check, so this just spares a doomed request the provision cost.
func (m *Manager) overQuota(extra int, tenant string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.quotaErr(extra, tenant)
}

// quotaErr answers ErrQuota over the node cap, the tenant cap, or a
// draining node; callers hold m.mu.
func (m *Manager) quotaErr(extra int, tenant string) error {
	if m.draining {
		return fmt.Errorf("%w: node draining", ErrQuota)
	}
	if m.maxClaims > 0 && len(m.claimed)+extra > m.maxClaims {
		return fmt.Errorf("%w: %d live claims, cap %d", ErrQuota, len(m.claimed), m.maxClaims)
	}
	limit := m.tenantMax[tenant]
	if tenant == "" || limit <= 0 {
		return nil
	}
	if live := m.tenantLive[tenant]; live+extra > limit {
		return fmt.Errorf("%w: tenant %s at %d live claims, cap %d", ErrQuota, tenant, live, limit)
	}
	return nil
}

// tenantDelta tracks tenantLive as m.claimed changes; callers hold m.mu.
func (m *Manager) tenantDelta(tenant string, delta int) {
	if tenant == "" {
		return
	}
	if n := m.tenantLive[tenant] + delta; n > 0 {
		m.tenantLive[tenant] = n
	} else {
		delete(m.tenantLive, tenant)
	}
}

// finalize stamps identity, persists the claim, and destroys the VM if the
// store write fails so a durable claim always matches a live VM.
func (m *Manager) finalize(ctx context.Context, sb *types.Sandbox, ttl time.Duration) (*types.Sandbox, error) {
	if err := m.finalizeBatch(ctx, []*types.Sandbox{sb}, ttl); err != nil {
		return nil, err
	}
	return sb, nil
}

// finalizeBatch stamps identities and persists the batch as one journal write;
// a failed write destroys every VM in it — a durable claim always matches a
// live VM, all-or-nothing. One tenant per batch (fork children inherit it).
func (m *Manager) finalizeBatch(ctx context.Context, sbs []*types.Sandbox, ttl time.Duration) error {
	now := time.Now()
	for _, sb := range sbs {
		stampIdentity(sb, clampTTL(ttl))
		sb.TouchAt(now)
	}
	m.mu.Lock()
	if quotaErr := m.quotaErr(len(sbs), sbs[0].Tenant); quotaErr != nil {
		m.mu.Unlock()
		for _, sb := range sbs {
			m.destroy(ctx, sb.VMName)
		}
		return quotaErr
	}
	for _, sb := range sbs {
		m.claimed[sb.ID] = sb
		m.tenantDelta(sb.Tenant, 1)
	}
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.rollbackClaim(ctx, sbs)
		return fmt.Errorf("persist claim: %w", saveErr)
	}
	for _, sb := range sbs {
		if armErr := m.armEgress(ctx, sb); armErr != nil {
			m.rollbackClaim(ctx, sbs)
			return fmt.Errorf("arm egress %s: %w", sb.ID, armErr)
		}
	}
	// Usage lands only after the whole batch armed: a rollback must not leave
	// claim events with no terminal release/reap in the billing stream.
	for _, sb := range sbs {
		m.recordUsage(ctx, usageEvent{Event: "claim", ID: sb.ID, VMName: sb.VMName, KeyHash: sb.Key.Hash(), Tenant: sb.Tenant})
	}
	return nil
}

// rollbackClaim unwinds a claim batch after a persist or egress-arm failure:
// drop the claims, reconverge the journal, destroy the VMs — a NIC that
// cannot be locked must never be handed out.
func (m *Manager) rollbackClaim(ctx context.Context, sbs []*types.Sandbox) {
	m.mu.Lock()
	for _, sb := range sbs {
		delete(m.claimed, sb.ID)
		m.tenantDelta(sb.Tenant, -1)
	}
	rb := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	m.recommit(ctx, rb)
	for _, sb := range sbs {
		m.disarmEgress(sb.ID, m.removeVM(ctx, sb.VMName))
	}
}

func (m *Manager) reapOnce(ctx context.Context) {
	now := time.Now()
	type victim struct {
		action                       reapAction
		id, vmName, snap, ck, tenant string
		sb                           *types.Sandbox
	}
	m.mu.Lock()
	var expired []victim
	for id, sb := range m.claimed {
		// A zero deadline means no expiry (an archived claim kept forever).
		if sb.Deadline.IsZero() || !now.After(sb.Deadline) {
			continue
		}
		switch {
		case sb.ArchiveCk != "":
			expired = append(expired, victim{action: reapPurge, id: id, ck: sb.ArchiveCk, tenant: sb.Tenant, sb: sb})
			delete(m.claimed, id)
			m.tenantDelta(sb.Tenant, -1)
		case sb.HibernateSnap != "" && m.archiveEnabledFor(sb.Key):
			// End-of-lease hibernated + archive-enabled: archive instead of
			// destroy, kept in m.claimed for archive() to re-validate and move.
			if _, busy := m.archiving[id]; busy {
				continue // an archive is already exporting this sandbox
			}
			m.archiving[id] = struct{}{}
			expired = append(expired, victim{action: reapArchive, id: id, sb: sb})
		default:
			expired = append(expired, victim{action: reapDestroy, id: id, vmName: sb.VMName, snap: sb.HibernateSnap, tenant: sb.Tenant, sb: sb})
			delete(m.claimed, id)
			m.tenantDelta(sb.Tenant, -1)
		}
	}
	var js claimSnapshot
	if len(expired) > 0 {
		js = m.store.snapshot(m.claimed)
	}
	m.mu.Unlock()

	logger := log.WithFunc("pool.reapOnce")
	if len(expired) > 0 {
		if saveErr := m.store.commit(js); saveErr != nil {
			m.mu.Lock()
			for _, v := range expired {
				if v.action == reapArchive {
					delete(m.archiving, v.id) // never removed from m.claimed
				} else {
					m.claimed[v.id] = v.sb // roll back so memory matches the still-durable claim
					m.tenantDelta(v.tenant, 1)
				}
			}
			rb := m.store.snapshot(m.claimed)
			m.mu.Unlock()
			m.recommit(ctx, rb)
			logger.Error(ctx, saveErr, "persist reap; rolled back")
			return
		}
	}
	// Engine subprocesses (worst case minutes on a hung stop): fan out bounded
	// so a big batch never stalls the ticker loop.
	m.runBounded(ctx, len(expired), func(ctx context.Context, i int) {
		v := expired[i]
		switch v.action {
		case reapPurge:
			m.disarmEgress(v.id, true)
			m.purgeArchiveCk(ctx, v.id, v.ck, v.tenant)
			logger.Infof(ctx, "purged archived sandbox %s", v.id)
		case reapArchive:
			switch err := m.archive(ctx, v.sb); {
			case err == nil:
				logger.Infof(ctx, "archived expired sandbox %s", v.id)
			case !benignSweepErr(err):
				logger.Errorf(ctx, err, "archive expired %s", v.id)
			}
		default:
			m.disarmEgress(v.id, m.removeVM(ctx, v.vmName))
			m.dropSnap(ctx, v.snap)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id, VMName: v.vmName})
			logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
		}
	})
}

// purgeArchiveCk deletes an archived claim's store checkpoint and records the
// retention billing event; shared by Release and reapOnce's purge.
func (m *Manager) purgeArchiveCk(ctx context.Context, id, ck, tenant string) {
	if err := m.deleteCkLocked(ctx, ck); err != nil {
		log.WithFunc("pool.purgeArchiveCk").Warnf(ctx, "delete archive ck %s: %v", ck, err)
	}
	m.counters.archiveDeletes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "archive_delete", ID: id, Reference: ck, Tenant: tenant})
}

// recommit re-persists a snapshot in the background until its own success or
// any newer durable write ends the loop (commit coalesces by sequence);
// detached, because callers may hold the transition lock.
func (m *Manager) recommit(ctx context.Context, snap claimSnapshot) {
	go func() {
		backoff := recommitBackoff
		for {
			err := m.store.commit(snap)
			if err == nil {
				return
			}
			log.WithFunc("pool.recommit").Error(ctx, err, "persist claims")
			time.Sleep(backoff)
			backoff = min(backoff*2, recommitMaxBackoff)
		}
	}()
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
