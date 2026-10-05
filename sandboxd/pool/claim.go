package pool

import (
	"cmp"
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	reapDestroy reapAction = iota // engine teardown + drop snapshot
	reapArchive                   // hibernated with archive enabled or on_expire archive: archive, keep the claim
	reapPurge                     // archived past retention: delete the store checkpoint
	reapPause
)

type reapAction int

// ClaimOptions is what a claim asks for beyond its key; the zero value takes every default.
type ClaimOptions struct {
	TTL             time.Duration
	OnExpire        types.ExpireAction
	Tenant          string
	ClaimRef        string
	Metadata        types.Metadata
	Volumes         []types.Volume
	NoEgress        bool
	RequirePromoted bool
	Env             types.Env
	// EgressClass is the tenant's egress class as its token resolved for this request.
	EgressClass string
}

// apply stamps the options a claim carries for its life.
func (o ClaimOptions) apply(sb *types.Sandbox) {
	sb.Tenant, sb.ClaimRef, sb.Metadata, sb.OnExpire, sb.NoEgress = o.Tenant, o.ClaimRef, o.Metadata, o.OnExpire.Or(""), o.NoEgress
	sb.EgressClass = o.EgressClass
	sb.SetEnv(o.Env)
}

// ClaimWarm transfers ownership of a warm sandbox without provisioning; ErrNoWarm means empty.
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, o ClaimOptions) (*types.Sandbox, error) {
	start := time.Now()
	volumeSpecs, applied, err := m.admitOptions(ctx, key, o)
	if err != nil {
		return nil, err
	}
	// holds belong to this path until finalize; past it the sandbox carries them.
	reserved := applied
	defer func() { m.unreserveVolumes(reserved) }()
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
	m.kickRefill()
	if sb == nil {
		return nil, ErrNoWarm
	}
	if volumeErr := m.applyVolumes(ctx, sb, volumeSpecs, applied); volumeErr != nil {
		m.abortVolumeClaim(ctx, sb.VMName, &reserved)
		return nil, volumeErr
	}
	o.apply(sb)
	reserved = nil
	out, err := m.finalize(ctx, sb, o.TTL, false)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox, a golden clone or a cold boot; with RequirePromoted only a promoted template serves it.
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, o ClaimOptions) (*types.Sandbox, error) {
	start := time.Now()
	volumeSpecs, applied, err := m.admitOptions(ctx, key, o)
	if err != nil {
		return nil, err
	}
	reserved := applied
	defer func() { m.unreserveVolumes(reserved) }()
	golden, err := m.resolveGolden(ctx, key, o.Tenant)
	if err != nil {
		return nil, fmt.Errorf("resolve template: %w", err)
	}
	if o.RequirePromoted && !golden.promoted {
		golden.release()
		return nil, ErrUnknownTemplate
	}
	sb, err := m.provision(ctx, key, golden.dir)
	golden.release()
	if err != nil {
		return nil, err
	}
	if volumeErr := m.applyVolumes(ctx, sb, volumeSpecs, applied); volumeErr != nil {
		m.abortVolumeClaim(ctx, sb.VMName, &reserved)
		return nil, volumeErr
	}
	sb.TemplateDigest = golden.templateDigest
	sb.PolicySource = golden.source
	o.apply(sb)
	reserved = nil
	out, err := m.finalize(ctx, sb, o.TTL, golden.guestEnv)
	if err == nil {
		if golden.dir != "" {
			m.counters.claimsClone.Add(1)
		} else {
			m.counters.claimsCold.Add(1)
		}
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// Release destroys a claimed sandbox after authorizing cred.
func (m *Manager) Release(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	return m.releaseResolved(ctx, id, sb)
}

// ClaimDeadline authorizes a sandbox by token and returns its lease deadline.
func (m *Manager) ClaimDeadline(id, token string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return time.Time{}, ErrUnknownSandbox
	}
	return sb.Deadline, nil
}

// DialPort opens a byte stream to a guest port after authorizing cred, waking a hibernated VM first; the operator credential never wakes and answers ErrPaused.
func (m *Manager) DialPort(ctx context.Context, id string, cred Cred, port uint16) (net.Conn, error) {
	if cred.Operator {
		return m.dialPassive(ctx, id, port)
	}
	return m.dialPort(ctx, id, cred, port, "port")
}

// Renew resets a claim's lease to ttl from now after authorizing cred, and reports the granted deadline.
func (m *Manager) Renew(ctx context.Context, id string, cred Cred, ttl time.Duration, onExpire types.ExpireAction) (time.Time, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return time.Time{}, ErrUnknownSandbox
	}
	if !cred.Operator {
		if err := m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return time.Time{}, err
		}
	}
	if err := archivable(sb.Key, hasAppliedVolumes(sb), onExpire); err != nil {
		return time.Time{}, err
	}
	lease := clampTTL(ttl)
	m.mu.Lock()
	if m.claimed[id] != sb {
		m.mu.Unlock()
		return time.Time{}, ErrUnknownSandbox
	}
	// an archived claim's Deadline is a retention window, not a lease: a TTL over it schedules a purge
	if _, archiving := m.archiving[id]; sb.ArchiveCk != "" || archiving {
		m.mu.Unlock()
		return time.Time{}, ErrArchived
	}
	prev, prevLease, prevExpire := sb.Deadline, sb.LeaseSeconds, sb.OnExpire
	sb.Deadline, sb.LeaseSeconds, sb.OnExpire = time.Now().Add(lease), int(lease/time.Second), onExpire.Or(sb.OnExpire)
	deadline, js := sb.Deadline, m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		// a concurrent renew that landed keeps its grant: roll back only what this call wrote
		if m.claimed[id] == sb && sb.Deadline.Equal(deadline) {
			sb.Deadline, sb.LeaseSeconds, sb.OnExpire = prev, prevLease, prevExpire
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		m.recommit(ctx, rb)
		return time.Time{}, fmt.Errorf("renew %s: persist claims: %w", id, err)
	}
	sb.Touch()
	return deadline, nil
}

// PreviewDial opens a preview request's guest connection; the caller verified the HMAC token.
func (m *Manager) PreviewDial(ctx context.Context, id string, port uint16) (net.Conn, error) {
	return m.dialPort(ctx, id, Cred{Operator: true}, port, "preview")
}

// AgentSocket resolves a claimed sandbox's vsock UDS without waking it.
func (m *Manager) AgentSocket(id, token string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return "", ErrUnknownSandbox
	}
	// no activity stamp: a control-plane poll must not keep an idle sandbox awake
	return sb.VsockSocket, nil
}

// dialPort holds the sandbox for the connection's lifetime, so an idle sweep cannot reap a live stream.
func (m *Manager) dialPort(ctx context.Context, id string, cred Cred, port uint16, op string) (net.Conn, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	sb.Touch()
	m.recordAudit(ctx, id, auditFrame{Op: op, Port: port})
	sb.Hold()
	sock, err := m.wakeResolved(ctx, sb)
	if err != nil {
		sb.Unhold()
		return nil, err
	}
	conn, err := m.eng.DialGuestPort(ctx, sock, port)
	if err != nil {
		sb.Unhold()
		return nil, err
	}
	return &heldConn{Conn: conn, release: sync.OnceFunc(sb.Unhold)}, nil
}

func (m *Manager) dialPassive(ctx context.Context, id string, port uint16) (net.Conn, error) {
	sb, ok := m.byID(id)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	if !sb.Transition.TryLock() {
		return nil, ErrPaused
	}
	failed := sb.Failed != ""
	paused, sock := sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.ArchiveCk != "", sb.VsockSocket
	if !paused && !failed {
		sb.Hold()
	}
	sb.Transition.Unlock()
	switch {
	case failed:
		return nil, ErrFailed
	case paused:
		return nil, ErrPaused
	}
	m.recordAudit(ctx, id, auditFrame{Op: "port", Port: port})
	conn, err := m.eng.DialGuestPort(ctx, sock, port)
	if err != nil {
		sb.UnholdIdle()
		return nil, err
	}
	return &heldConn{Conn: conn, release: sync.OnceFunc(sb.UnholdIdle)}, nil
}

// releaseResolved re-checks under m.mu that sb is still the live claim: no double teardown.
func (m *Manager) releaseResolved(ctx context.Context, id string, sb *types.Sandbox) error {
	var marked string
	m.mu.Lock()
	for {
		if m.claimed[id] != sb {
			m.mu.Unlock()
			return ErrUnknownSandbox
		}
		ck := sb.ArchiveCk
		if ck == "" || ck == marked {
			break
		}
		m.mu.Unlock()
		if markErr := m.markArchiveCk(ck); markErr != nil {
			return fmt.Errorf("release %s: track archive checkpoint: %w", id, markErr)
		}
		marked = ck
		m.mu.Lock()
	}
	snap, ck, vmName := sb.HibernateSnap, sb.ArchiveCk, sb.VMName
	if ck != "" {
		m.pendingCks[ck] = struct{}{}
	}
	delete(m.claimed, id)
	m.tenantDelta(sb.Tenant, -1)
	js := m.store.del(id)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		m.claimed[id] = sb // roll back so memory matches the still-durable claim
		m.tenantDelta(sb.Tenant, 1)
		delete(m.pendingCks, ck)
		rb := m.store.set(sb)
		m.mu.Unlock()
		logger := log.WithFunc("pool.releaseResolved")
		if ck != "" {
			if clearErr := m.clearArchiveCk(ck); clearErr != nil {
				logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
			}
		}
		m.recommit(ctx, rb)
		logger.Errorf(ctx, saveErr, "persist release of %s", id)
		return fmt.Errorf("release %s: %w", id, saveErr)
	}
	// cleanup must survive the caller hanging up; the claim is already dropped.
	ctx = context.WithoutCancel(ctx)
	if ck != "" {
		m.purgeArchiveCk(ctx, id, ck, sb.Tenant) // archived: no local VM
		m.untrack(m.pendingCks, ck)
	}
	td := m.quiesceVolumes(ctx, sb)
	var err error
	removed := vmName == ""
	switch {
	case removed:
		m.finishVolumeTeardown(ctx, td)
	case m.releaseDelay > 0:
		m.queueRemoval(vmName, id, "", td, time.Now().Add(m.releaseDelay))
	default:
		if removed = m.removeOrRetry(ctx, vmName, id, "", td); !removed {
			err = fmt.Errorf("vm %s survived removal", vmName)
		}
	}
	m.out.Disarm(id, removed)
	m.dropSnap(ctx, snap)
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: id, VMName: vmName})
	return err
}

// overQuota is an advisory precheck; finalizeBatch does the authoritative admission check.
func (m *Manager) overQuota(extra int, tenant string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.quotaErr(extra, tenant)
}

// admitVolumes takes one claim's holds; a refusal on either check leaves nothing held.
func (m *Manager) admitVolumes(tenant string, volumes []resolvedVolume) ([]types.Volume, error) {
	applied := appliedVolumes(volumes)
	if err := m.admitClaim(tenant, applied); err != nil {
		return nil, err
	}
	if err := m.confirmVolumesClean(volumes); err != nil {
		m.unreserveVolumes(applied)
		return nil, err
	}
	return applied, nil
}

// admitClaim refuses a busy volume or an over-quota tenant before any VM is built.
func (m *Manager) admitClaim(tenant string, volumes []types.Volume) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.quotaErr(1, tenant); err != nil {
		return err
	}
	return m.reserveVolumes(volumes)
}

// quotaErr answers ErrQuota over a cap or a draining node; callers hold m.mu.
func (m *Manager) quotaErr(extra int, tenant string) error {
	if m.draining {
		return fmt.Errorf("%w: node draining", ErrQuota)
	}
	if m.maxClaims > 0 && len(m.claimed)+extra > m.maxClaims {
		return fmt.Errorf("%w: %d live claims, cap %d", ErrQuota, len(m.claimed), m.maxClaims)
	}
	if tenant == "" {
		return nil
	}
	r, _ := m.tenants.Peek(tenant)
	limit := r.MaxClaims
	if limit <= 0 {
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

// finalize stamps identity and persists the claim; a failed write destroys the VM.
func (m *Manager) finalize(ctx context.Context, sb *types.Sandbox, ttl time.Duration, inherited bool) (*types.Sandbox, error) {
	if err := m.finalizeBatch(ctx, []*types.Sandbox{sb}, ttl, "", inherited); err != nil {
		return nil, err
	}
	return sb, nil
}

// finalizeBatch persists the batch as one write, all-or-nothing; one tenant per batch.
func (m *Manager) finalizeBatch(ctx context.Context, sbs []*types.Sandbox, ttl time.Duration, claimRefPrefix string, inherited bool) error {
	now := time.Now()
	for _, sb := range sbs {
		stampIdentity(sb, clampTTL(ttl))
		if claimRefPrefix != "" {
			sb.ClaimRef = claimRefPrefix + sb.ID
		}
		sb.TouchAt(now)
	}
	err := m.checkInjects(m.view.Load(), sbs)
	if err == nil {
		err = m.deliverEnv(ctx, sbs, inherited)
	}
	m.mu.Lock()
	if err == nil {
		err = m.quotaErr(len(sbs), sbs[0].Tenant)
	}
	if err != nil {
		m.mu.Unlock()
		for _, sb := range sbs {
			// the rw mount dirtied the journal, so the clean umount is still owed
			td := m.quiesceVolumes(ctx, sb)
			m.removeOrRetry(ctx, sb.VMName, "", "", td)
		}
		return err
	}
	for _, sb := range sbs {
		sb.Layer = types.LayerUnpooled
		if _, pooled := m.activePool(sb.PolicyKey()); pooled {
			sb.Layer = types.LayerPooled
		}
		m.claimed[sb.ID] = sb
		m.tenantDelta(sb.Tenant, 1)
	}
	js := m.store.set(sbs...)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.rollbackClaim(ctx, sbs)
		return fmt.Errorf("persist claim: %w", saveErr)
	}
	for _, sb := range sbs {
		if armErr := m.out.Arm(ctx, sb); armErr != nil {
			m.rollbackClaim(ctx, sbs)
			return fmt.Errorf("arm egress %s: %w", sb.ID, armErr)
		}
	}
	// usage lands only after the batch armed, so a rollback leaves no unterminated claim event
	for _, sb := range sbs {
		m.recordUsage(ctx, usageEvent{
			Event: "claim",
			ID:    sb.ID, VMName: sb.VMName,
			KeyHash: sb.Key.Hash(), Tenant: sb.Tenant,
			Volumes: types.VolumeNames(sb.Volumes), VolumesRW: types.VolumeRWNames(sb.Volumes),
		})
	}
	return nil
}

// rollbackClaim unwinds a claim batch: a NIC that cannot be locked is never handed out.
func (m *Manager) rollbackClaim(ctx context.Context, sbs []*types.Sandbox) {
	m.mu.Lock()
	ids := make([]string, len(sbs))
	for i, sb := range sbs {
		delete(m.claimed, sb.ID)
		m.tenantDelta(sb.Tenant, -1)
		ids[i] = sb.ID
	}
	rb := m.store.del(ids...)
	m.mu.Unlock()
	m.recommit(ctx, rb)
	for _, sb := range sbs {
		td := m.quiesceVolumes(ctx, sb)
		m.out.Disarm(sb.ID, m.removeOrRetry(ctx, sb.VMName, sb.ID, "", td))
	}
}

// abortVolumeClaim removes a failed claim's VM and clears reserved against a double release.
func (m *Manager) abortVolumeClaim(ctx context.Context, vmName string, reserved *[]types.Volume) {
	m.removeOrRetry(ctx, vmName, "", "", volumeTeardown{holds: *reserved})
	*reserved = nil
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
	var dropped []string
	for id, sb := range m.claimed {
		// a zero deadline means no expiry (an archived claim kept forever).
		if sb.Deadline.IsZero() || !now.After(sb.Deadline) {
			continue
		}
		switch {
		case sb.ArchiveCk != "":
			expired = append(expired, victim{action: reapPurge, id: id, ck: sb.ArchiveCk, tenant: sb.Tenant, sb: sb})
		case m.archivesAtDeadline(sb):
			// archive instead of destroy, kept in m.claimed for archive() to move
			if _, busy := m.archiving[id]; busy {
				continue
			}
			m.archiving[id] = struct{}{}
			action := reapArchive
			if sb.HibernateSnap == "" {
				action = reapPause
			}
			expired = append(expired, victim{action: action, id: id, sb: sb})
		default:
			expired = append(expired, victim{action: reapDestroy, id: id, vmName: sb.VMName, snap: sb.HibernateSnap, tenant: sb.Tenant, sb: sb})
			delete(m.claimed, id)
			m.tenantDelta(sb.Tenant, -1)
			dropped = append(dropped, id)
		}
	}
	if len(dropped) > 0 {
		m.store.del(dropped...)
	}
	m.mu.Unlock()
	if len(expired) == 0 {
		return
	}

	logger := log.WithFunc("pool.reapOnce")
	var purges []string
	for _, v := range expired {
		if v.action == reapPurge {
			purges = append(purges, v.ck)
		}
	}
	failed := m.markArchiveCks(purges)
	expired = slices.DeleteFunc(expired, func(v victim) bool {
		markErr, bad := failed[v.ck]
		if bad && v.action == reapPurge {
			logger.Errorf(ctx, markErr, "mark archive ck %s; keeping %s", v.ck, v.id)
		}
		return bad && v.action == reapPurge
	})
	if len(expired) == 0 {
		return
	}
	m.mu.Lock()
	keep := expired[:0]
	var purgeCks, purged []string
	for _, v := range expired {
		if v.action == reapPurge {
			if m.claimed[v.id] != v.sb || v.sb.ArchiveCk != v.ck {
				continue
			}
			m.pendingCks[v.ck] = struct{}{}
			delete(m.claimed, v.id)
			m.tenantDelta(v.tenant, -1)
			purgeCks = append(purgeCks, v.ck)
			purged = append(purged, v.id)
		}
		keep = append(keep, v)
	}
	expired = keep
	if len(expired) == 0 {
		m.mu.Unlock()
		return
	}
	js := m.store.del(purged...)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		var restored []*types.Sandbox
		for _, v := range expired {
			if v.action == reapArchive || v.action == reapPause {
				delete(m.archiving, v.id) // never removed from m.claimed
			} else {
				m.claimed[v.id] = v.sb // roll back so memory matches the still-durable claim
				m.tenantDelta(v.tenant, 1)
				delete(m.pendingCks, v.ck)
				restored = append(restored, v.sb)
			}
		}
		rb := m.store.set(restored...)
		m.mu.Unlock()
		for _, ck := range purgeCks {
			if clearErr := m.clearArchiveCk(ck); clearErr != nil {
				logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
			}
		}
		m.recommit(ctx, rb)
		logger.Error(ctx, saveErr, "persist reap; rolled back")
		return
	}
	// engine teardown can take minutes, so fan out bounded and never stall the ticker loop
	m.runBounded(ctx, len(expired), func(ctx context.Context, i int) {
		v := expired[i]
		switch v.action {
		case reapPurge:
			m.out.Disarm(v.id, true)
			m.purgeArchiveCk(ctx, v.id, v.ck, v.tenant)
			m.untrack(m.pendingCks, v.ck)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id})
			logger.Infof(ctx, "purged archived sandbox %s", v.id)
		case reapArchive:
			logSweepResult(ctx, logger, m.archive(ctx, v.sb), "archived expired sandbox "+v.id, "archive expired sandbox "+v.id)
		case reapPause:
			logSweepResult(ctx, logger, m.pauseExpired(ctx, v.sb), "archived expired sandbox "+v.id, "archive expired sandbox "+v.id)
		default:
			td := m.quiesceVolumes(ctx, v.sb)
			m.out.Disarm(v.id, m.removeOrRetry(ctx, v.vmName, v.id, "", td))
			m.dropSnap(ctx, v.snap)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id, VMName: v.vmName})
			logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
		}
	})
}

func (m *Manager) archivesAtDeadline(sb *types.Sandbox) bool {
	return sb.Failed == "" && !m.tenantGone(sb.Tenant) && (sb.OnExpire == types.ExpireArchive || sb.HibernateSnap != "" && m.archiveEnabledFor(sb.Key))
}

func (m *Manager) pauseExpired(ctx context.Context, sb *types.Sandbox) error {
	sb.Transition.Lock()
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb && sb.ArchiveCk == ""
	m.mu.Unlock()
	var err error
	if live {
		err = m.hibernateLocked(ctx, sb)
	}
	sb.Transition.Unlock()
	if !live || err != nil {
		m.untrack(m.archiving, sb.ID)
		return cmp.Or(err, errWokeMeanwhile)
	}
	return m.archive(ctx, sb)
}

func (m *Manager) purgeArchiveCk(ctx context.Context, id, ck, tenant string) {
	logger := log.WithFunc("pool.purgeArchiveCk")
	if err := m.deleteCkLocked(ctx, ck); err != nil {
		logger.Warnf(ctx, "delete archive ck %s: %v", ck, err)
	} else if err := m.clearArchiveCk(ck); err != nil {
		logger.Warnf(ctx, "clear archive ck %s: %v", ck, err)
	}
	m.counters.archiveDeletes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "archive_delete", ID: id, Reference: ck, Tenant: tenant})
}

// recommit keeps claims.json catching up with the store: one retrier at a time writes the current state until it is durable and nothing newer is pending.
func (m *Manager) recommit(ctx context.Context, snap claimSnapshot) {
	if !m.recommitting.CompareAndSwap(false, true) {
		return
	}
	go func() {
		backoff := recommitBackoff
		for {
			err := m.store.commit(snap)
			if err == nil {
				m.recommitting.Store(false)
				if snap = m.store.mark(); !m.store.pending(snap) || !m.recommitting.CompareAndSwap(false, true) {
					return
				}
				continue
			}
			log.WithFunc("pool.recommit").Error(ctx, err, "persist claims")
			time.Sleep(backoff)
			backoff = min(backoff*2, recommitMaxBackoff)
		}
	}()
}

func (m *Manager) commitIfLive(ctx context.Context, sb *types.Sandbox, mutate func()) (bool, error) {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		mutate()
		js = m.store.set(sb)
	}
	m.mu.Unlock()
	if !live {
		return false, nil
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
		return true, err
	}
	return true, nil
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

// admitOptions validates the key and options and admits the claim's volumes, which the caller unreserves until finalize.
func (m *Manager) admitOptions(ctx context.Context, key types.PoolKey, o ClaimOptions) ([]resolvedVolume, []types.Volume, error) {
	if err := m.validate(key); err != nil {
		return nil, nil, err
	}
	if err := archivable(key, len(o.Volumes) > 0, o.OnExpire); err != nil {
		return nil, nil, err
	}
	if err := m.checkEnv(o.Env); err != nil {
		return nil, nil, err
	}
	volumeSpecs, err := m.resolveVolumes(ctx, o.Tenant, o.Volumes)
	if err != nil {
		return nil, nil, err
	}
	applied, err := m.admitVolumes(o.Tenant, volumeSpecs)
	return volumeSpecs, applied, err
}

// heldConn keeps its sandbox held until the guest connection closes.
type heldConn struct {
	net.Conn
	release func()
}

func (c *heldConn) Close() error {
	c.release()
	return c.Conn.Close()
}

// CloseWrite forwards the half-close; net.Conn does not carry it, so embedding cannot promote one.
func (c *heldConn) CloseWrite() error {
	cw, ok := c.Conn.(interface{ CloseWrite() error })
	if !ok {
		return nil
	}
	return cw.CloseWrite()
}

func stampIdentity(sb *types.Sandbox, ttl time.Duration) {
	now := time.Now()
	sb.ID = "sb_" + randHex(8)
	sb.Token = randHex(16)
	sb.ClaimedAt = now
	sb.Deadline = now.Add(ttl)
	sb.LeaseSeconds = int(ttl / time.Second)
}

func clampTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultTTL
	}
	return min(ttl, maxTTL)
}

func archivable(key types.PoolKey, volumes bool, onExpire types.ExpireAction) error {
	switch {
	case onExpire != types.ExpireArchive:
		return nil
	case key.Net == types.NetEgress:
		return ErrNoEgressHibernate
	case volumes:
		return ErrVolumeCapture
	default:
		return nil
	}
}
