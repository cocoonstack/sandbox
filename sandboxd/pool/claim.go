package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

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

func (o ClaimOptions) apply(sb *types.Sandbox) {
	sb.Tenant, sb.ClaimRef, sb.Metadata, sb.OnExpire, sb.NoEgress = o.Tenant, o.ClaimRef, o.Metadata, o.OnExpire.Or(""), o.NoEgress
	sb.EgressClass = o.EgressClass
	sb.SetEnv(o.Env)
}

type claimHold struct {
	n      int
	tenant string
	done   bool
}

// ClaimWarm transfers ownership of a warm sandbox without provisioning; ErrNoWarm means empty.
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, o ClaimOptions) (*types.Sandbox, error) {
	start := time.Now()
	volumeSpecs, applied, hold, err := m.admitOptions(ctx, key, o)
	if err != nil {
		return nil, err
	}
	defer m.dropHold(hold)
	reserved := applied
	defer func() { m.unreserveVolumes(reserved) }()
	m.mu.Lock()
	var sb *types.Sandbox
	if p := m.pools[key]; p != nil {
		p.noteArrival(start)
		sb = p.popWarm()
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
	out, err := m.finalize(ctx, sb, o.TTL, false, hold)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimWarmLatency.observe(time.Since(start))
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox, a golden clone or a cold boot; with RequirePromoted only a promoted template serves it.
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, o ClaimOptions) (*types.Sandbox, error) {
	start := time.Now()
	volumeSpecs, applied, hold, err := m.admitOptions(ctx, key, o)
	if err != nil {
		return nil, err
	}
	defer m.dropHold(hold)
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
	sb, err := m.provisionResolved(ctx, key, golden)
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
	out, err := m.finalize(ctx, sb, o.TTL, golden.guestEnv, hold)
	if err == nil {
		if golden.dir != "" {
			m.counters.claimsClone.Add(1)
			m.counters.claimCloneLatency.observe(time.Since(start))
		} else {
			m.counters.claimsCold.Add(1)
			m.counters.claimColdLatency.observe(time.Since(start))
		}
	}
	return out, err
}

// Release destroys a claimed sandbox after authorizing cred.
func (m *Manager) Release(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	if m.archives != nil {
		return m.releaseRowed(ctx, id, sb)
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
	lease := clampTTL(ttl)
	m.mu.Lock()
	if err := archivable(sb.Key, m.hasVolumeHolds(sb), onExpire); err != nil {
		m.mu.Unlock()
		return time.Time{}, err
	}
	if m.claimed[id] != sb {
		m.mu.Unlock()
		return time.Time{}, ErrUnknownSandbox
	}
	if m.pausedForMoveLocked(sb) {
		m.mu.Unlock()
		return time.Time{}, ErrEvacuating
	}
	// An archived claim's Deadline is a retention window, not a lease: a TTL over it schedules a purge
	if _, archiving := m.archiving[id]; sb.ArchiveCk != "" || archiving {
		m.mu.Unlock()
		return time.Time{}, ErrArchived
	}
	prev, prevLease, prevExpire := sb.Deadline, sb.LeaseSeconds, sb.OnExpire
	renewedAt := time.Now()
	sb.Deadline, sb.LeaseSeconds, sb.OnExpire = renewedAt.Add(lease), int(lease/time.Second), onExpire.Or(sb.OnExpire)
	deadline, js, rowed := sb.Deadline, m.store.set(sb), m.rowFollows(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		// A concurrent renew that landed keeps its grant: roll back only what this call wrote
		if m.claimed[id] == sb && sb.Deadline.Equal(deadline) {
			sb.Deadline, sb.LeaseSeconds, sb.OnExpire = prev, prevLease, prevExpire
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		m.store.recommit(ctx, rb)
		return time.Time{}, fmt.Errorf("renew %s: persist claims: %w", id, err)
	}
	m.recordUsage(ctx, usage.Event{Time: renewedAt, Event: "renew", ID: id, Tenant: sb.Tenant, Deadline: deadline})
	if rowed {
		if err := m.rewriteArchiveRow(ctx, sb, false); err != nil {
			return time.Time{}, fmt.Errorf("renew %s: %w", id, err)
		}
	}
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
	if sb.Replicated && m.isFenced() {
		return "", ErrLeaseLost
	}
	// No activity stamp: a control-plane poll must not keep an idle sandbox awake
	return sb.VsockSocket, nil
}

// dialPort holds the sandbox for the connection's lifetime, so an idle sweep cannot reap a live stream.
func (m *Manager) dialPort(ctx context.Context, id string, cred Cred, port uint16, op string) (net.Conn, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	fenced, err := m.fenceWatch(sb)
	if err != nil {
		return nil, err
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
	return newHeldConn(conn, fenced, sb.Unhold), nil
}

func (m *Manager) dialPassive(ctx context.Context, id string, port uint16) (net.Conn, error) {
	sb, ok := m.byID(id)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	fenced, err := m.fenceWatch(sb)
	if err != nil {
		return nil, err
	}
	if !sb.Transition.TryLock() {
		return nil, ErrPaused
	}
	failed, paused, sock := sb.Failed != "", !runningShaped(sb), sb.VsockSocket
	if !paused {
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
	return newHeldConn(conn, fenced, sb.UnholdIdle), nil
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
		if m.midMoveLocked(sb) {
			m.mu.Unlock()
			return ErrEvacuating
		}
		ck := sb.ArchiveCk
		if ck == "" || ck == marked {
			break
		}
		m.mu.Unlock()
		if markErr := m.ckMarks.mark(ck); markErr != nil {
			return fmt.Errorf("release %s: track archive checkpoint: %w", id, markErr)
		}
		marked = ck
		m.mu.Lock()
	}
	volumeHolds := m.cancelVolumeRun(sb)
	snap, ck, vmName := sb.HibernateSnap, sb.ArchiveCk, sb.VMName
	if ck != "" {
		m.pendingCks[ck] = struct{}{}
	}
	m.dropClaimLocked(sb)
	js := m.store.del(id)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		m.addClaimLocked(sb)
		m.restoreVolumeHolds(sb, volumeHolds)
		delete(m.pendingCks, ck)
		rb := m.store.set(sb)
		m.mu.Unlock()
		logger := log.WithFunc("pool.releaseResolved")
		if ck != "" {
			if clearErr := m.ckMarks.clear(ck); clearErr != nil {
				logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
			}
		}
		m.store.recommit(ctx, rb)
		logger.Errorf(ctx, saveErr, "persist release of %s", id)
		return fmt.Errorf("release %s: %w", id, saveErr)
	}
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usage.Event{Event: "release", ID: id, VMName: vmName, Tenant: sb.Tenant})
	// Cleanup must survive the caller hanging up; the claim is already dropped.
	ctx = context.WithoutCancel(ctx)
	if ck != "" {
		m.purgeArchiveCk(ctx, id, ck, sb.Tenant) // archived: no local VM
		m.untrack(m.pendingCks, ck)
	}
	td := m.quiesceVolumes(ctx, sb)
	td.holds = volumeHolds
	var err error
	removed := vmName == ""
	delay := m.view.Load().releaseDelay
	switch {
	case removed:
		m.finishVolumeTeardown(ctx, td)
	case delay > 0:
		m.queueRemoval(vmName, id, "", td, time.Now().Add(delay))
	default:
		if removed = m.removeOrRetry(ctx, vmName, id, td); !removed {
			err = fmt.Errorf("vm %s survived removal", vmName)
		}
	}
	m.out.Disarm(id, removed)
	m.dropSnap(ctx, snap)
	return err
}

// holdClaims takes n claim slots under every cap, so claims racing for the last slot fail before they spend a VM.
func (m *Manager) holdClaims(n int, tenant string) (*claimHold, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.holdClaimsLocked(n, tenant)
}

func (m *Manager) holdClaimsLocked(n int, tenant string) (*claimHold, error) {
	if err := m.quotaErr(n, tenant); err != nil {
		return nil, err
	}
	m.held += n
	if tenant != "" {
		m.heldTenants[tenant] += n
	}
	return &claimHold{n: n, tenant: tenant}, nil
}

func (m *Manager) dropHold(h *claimHold) {
	if h == nil || h.done {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropHoldLocked(h)
}

func (m *Manager) dropHoldLocked(h *claimHold) {
	if h == nil || h.done {
		return
	}
	h.done = true
	m.held -= h.n
	if h.tenant == "" {
		return
	}
	if left := m.heldTenants[h.tenant] - h.n; left > 0 {
		m.heldTenants[h.tenant] = left
	} else {
		delete(m.heldTenants, h.tenant)
	}
}

// admitVolumes takes one claim's holds; a refusal on either check leaves nothing held.
func (m *Manager) admitVolumes(tenant string, volumes []resolvedVolume) ([]types.Volume, *claimHold, error) {
	applied := appliedVolumes(volumes)
	hold, err := m.admitClaim(tenant, volumes, applied)
	if err != nil {
		return nil, nil, err
	}
	if err := m.confirmVolumesClean(volumes); err != nil {
		m.unreserveVolumes(applied)
		m.dropHold(hold)
		return nil, nil, err
	}
	return applied, hold, nil
}

func (m *Manager) admitClaim(tenant string, volumes []resolvedVolume, applied []types.Volume) (*claimHold, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hold, err := m.holdClaimsLocked(1, tenant)
	if err != nil {
		return nil, err
	}
	if err := m.catalog.checkResolvedLocked(volumes); err != nil {
		m.dropHoldLocked(hold)
		return nil, err
	}
	m.catalog.adoptVolumes(applied)
	return hold, nil
}

// Callers hold m.mu.
func (m *Manager) quotaErr(extra int, tenant string) error {
	if m.draining {
		return fmt.Errorf("%w: node draining", ErrQuota)
	}
	if err := m.diskErr(); err != nil {
		return err
	}
	if m.memoryLow.Load() {
		return fmt.Errorf("%w: %s", ErrQuota, memoryLowReason)
	}
	if limit, live := m.view.Load().maxClaims, len(m.claimed)+len(m.incoming)+m.held; limit > 0 && live+extra > limit {
		return fmt.Errorf("%w: %d live claims, cap %d", ErrQuota, live, limit)
	}
	if err := m.runningErr(extra); err != nil {
		return err
	}
	if tenant == "" {
		return nil
	}
	r, _ := m.tenants.Peek(tenant)
	limit := r.MaxClaims
	if limit <= 0 {
		return nil
	}
	live := m.tenantLive[tenant] + m.incomingOf(tenant) + m.heldTenants[tenant]
	if live+extra > limit {
		return fmt.Errorf("%w: tenant %s at %d live claims, cap %d", ErrQuota, tenant, live, limit)
	}
	return nil
}

// Callers hold m.mu.
func (m *Manager) runningErr(extra int) error {
	limit := m.view.Load().maxRunning
	if limit == 0 {
		return nil
	}
	if running := m.store.runningCount() + len(m.incoming) + m.held; running+extra > limit {
		return fmt.Errorf("%w: %d running claims, cap %d", ErrQuota, running, limit)
	}
	return nil
}

// beginWake admits a wake and counts it as running until endWake; the caller holds sb.Transition.
func (m *Manager) beginWake(sb *types.Sandbox) error {
	if err := m.diskErr(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb.Failed == "" && m.evacuatingLocked(sb.ID) {
		return ErrEvacuating
	}
	if err := m.runningErr(1); err != nil {
		return err
	}
	sb.Waking = true
	m.store.recount(sb)
	return nil
}

func (m *Manager) endWake(sb *types.Sandbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb.Waking = false
	m.store.recount(sb)
}

// Callers hold m.mu.
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

func (m *Manager) addClaimLocked(sb *types.Sandbox) {
	if m.archives != nil {
		if replicated := m.view.Load().poolReplicate[sb.PolicyKey()] > 0 || sb.HasReplica(); sb.Replicated != replicated {
			sb.Replicated = replicated
		}
	}
	m.claimed[sb.ID] = sb
	if sb.VMName != "" {
		m.claimedVMs[sb.VMName] = sb
	}
	m.tenantDelta(sb.Tenant, 1)
}

func (m *Manager) dropClaimLocked(sb *types.Sandbox) {
	delete(m.claimed, sb.ID)
	if m.claimedVMs[sb.VMName] == sb {
		delete(m.claimedVMs, sb.VMName)
	}
	m.tenantDelta(sb.Tenant, -1)
}

func (m *Manager) setVMNameLocked(sb *types.Sandbox, vmName string) {
	if m.claimedVMs[sb.VMName] == sb {
		delete(m.claimedVMs, sb.VMName)
	}
	sb.VMName = vmName
	if vmName != "" {
		m.claimedVMs[vmName] = sb
	}
}

// finalize stamps identity and persists the claim; a failed write destroys the VM.
func (m *Manager) finalize(ctx context.Context, sb *types.Sandbox, ttl time.Duration, inherited bool, hold *claimHold) (*types.Sandbox, error) {
	if err := m.finalizeBatch(ctx, []*types.Sandbox{sb}, ttl, "", inherited, hold); err != nil {
		return nil, err
	}
	return sb, nil
}

// finalizeBatch persists the batch as one write, all-or-nothing; one tenant per batch.
func (m *Manager) finalizeBatch(ctx context.Context, sbs []*types.Sandbox, ttl time.Duration, claimRefPrefix string, inherited bool, hold *claimHold) error {
	now, lease := time.Now(), clampTTL(ttl)
	for _, sb := range sbs {
		sb.ID, sb.Token = "sb_"+randHex(8), randHex(16)
		sb.ClaimedAt, sb.Deadline, sb.LeaseSeconds = now, now.Add(lease), int(lease/time.Second)
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
	m.dropHoldLocked(hold)
	if err == nil {
		err = m.quotaErr(len(sbs), sbs[0].Tenant)
	}
	if err != nil {
		m.mu.Unlock()
		for _, sb := range sbs {
			// The rw mount dirtied the journal, so the clean umount is still owed
			td := m.quiesceVolumes(ctx, sb)
			m.removeOrRetry(ctx, sb.VMName, "", td)
		}
		return err
	}
	for _, sb := range sbs {
		sb.Layer = types.LayerUnpooled
		if _, pooled := m.activePool(sb.PolicyKey()); pooled {
			sb.Layer = types.LayerPooled
		}
		m.addClaimLocked(sb)
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
	// Usage lands only after the batch armed, so a rollback leaves no unterminated claim event
	for _, sb := range sbs {
		m.recordUsage(ctx, usage.Event{
			Event: "claim",
			ID:    sb.ID, VMName: sb.VMName,
			KeyHash: sb.Key.Hash(), Template: sb.Key.Template, Net: string(sb.Key.Net), Size: string(sb.Key.Size),
			Tenant: sb.Tenant, Deadline: sb.ClaimedAt.Add(lease),
			Volumes: types.VolumeNames(sb.Volumes), VolumesRW: types.VolumeRWNames(sb.Volumes),
		})
	}
	return nil
}

// rollbackClaim unwinds a claim batch: a NIC that cannot be locked is never handed out.
func (m *Manager) rollbackClaim(ctx context.Context, sbs []*types.Sandbox) {
	m.mu.Lock()
	ids := make([]string, len(sbs))
	holds := make([][]types.Volume, len(sbs))
	for i, sb := range sbs {
		holds[i] = m.cancelVolumeRun(sb)
		m.dropClaimLocked(sb)
		ids[i] = sb.ID
	}
	rb := m.store.del(ids...)
	m.mu.Unlock()
	m.store.recommit(ctx, rb)
	for i, sb := range sbs {
		td := m.quiesceVolumes(ctx, sb)
		td.holds = holds[i]
		m.out.Disarm(sb.ID, m.removeOrRetry(ctx, sb.VMName, sb.ID, td))
	}
}

func (m *Manager) abortVolumeClaim(ctx context.Context, vmName string, reserved *[]types.Volume) {
	m.removeOrRetry(ctx, vmName, "", volumeTeardown{holds: *reserved})
	*reserved = nil
}

func (m *Manager) setIfLive(sb *types.Sandbox, mutate func()) (claimSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb {
		return claimSnapshot{}, false
	}
	mutate()
	return m.store.set(sb), true
}

func (m *Manager) commitIfLive(ctx context.Context, sb *types.Sandbox, mutate func()) (bool, error) {
	js, live := m.setIfLive(sb, mutate)
	if !live {
		return false, nil
	}
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
		return true, err
	}
	return true, nil
}

func (m *Manager) deadlineOf(sb *types.Sandbox) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.Deadline
}

// admitOptions validates the key and options and admits the claim's volumes, which the caller unreserves until finalize.
func (m *Manager) admitOptions(ctx context.Context, key types.PoolKey, o ClaimOptions) ([]resolvedVolume, []types.Volume, *claimHold, error) {
	if err := m.validate(key); err != nil {
		return nil, nil, nil, err
	}
	if err := archivable(key, len(o.Volumes) > 0, o.OnExpire); err != nil {
		return nil, nil, nil, err
	}
	if err := m.checkEnv(o.Env); err != nil {
		return nil, nil, nil, err
	}
	volumeSpecs, err := m.resolveVolumes(ctx, o.Tenant, o.Volumes)
	if err != nil {
		return nil, nil, nil, err
	}
	applied, hold, err := m.admitVolumes(o.Tenant, volumeSpecs)
	return volumeSpecs, applied, hold, err
}

// heldConn keeps its sandbox held until the guest connection closes.
type heldConn struct {
	net.Conn
	release func()
}

func newHeldConn(conn net.Conn, fenced <-chan struct{}, release func()) *heldConn {
	stop := CloseOnFence(fenced, conn)
	return &heldConn{Conn: conn, release: sync.OnceFunc(func() { stop(); release() })}
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
