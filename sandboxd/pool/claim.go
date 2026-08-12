package pool

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/filecache"
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
// tenant attributes the claim; empty means the operator (root). claimRef is an
// opaque caller reference recorded on the claim; empty means none.
func (m *Manager) ClaimWarm(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef, workspace string, volumes []types.Volume) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	volumeSpecs, err := m.resolveVolumes(key, tenant, volumes)
	if err != nil {
		return nil, err
	}
	applied, err := m.admitVolumes(tenant, volumeSpecs)
	if err != nil {
		return nil, err
	}
	// Holds belong to this path until finalize; past it the sandbox carries them.
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
	if sb == nil {
		return nil, ErrNoWarm
	}
	m.kickRefill()
	if volumeErr := m.applyVolumes(ctx, sb, volumeSpecs, applied); volumeErr != nil {
		m.destroy(ctx, sb.VMName)
		return nil, volumeErr
	}
	sb.Tenant = tenant
	sb.ClaimRef = claimRef
	sb.Workspace = workspace
	reserved = nil
	out, err := m.finalize(ctx, sb, ttl)
	if err == nil {
		m.counters.claimsWarm.Add(1)
		m.counters.claimNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
	}
	return out, err
}

// ClaimProvision creates a claim-ready sandbox (golden clone or cold boot).
// claimRef is an opaque caller reference recorded on the claim; empty means none.
func (m *Manager) ClaimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef, workspace string, volumes []types.Volume) (*types.Sandbox, error) {
	return m.claimProvision(ctx, key, ttl, tenant, claimRef, workspace, volumes, false)
}

// ClaimProvisionPromoted requires key to resolve from a promoted template and
// never falls through to a cold image boot.
func (m *Manager) ClaimProvisionPromoted(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef, workspace string, volumes []types.Volume) (*types.Sandbox, error) {
	return m.claimProvision(ctx, key, ttl, tenant, claimRef, workspace, volumes, true)
}

// Release destroys a claimed sandbox after authorizing cred.
func (m *Manager) Release(ctx context.Context, id string, cred Cred) error {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return ErrUnknownSandbox
	}
	return m.releaseResolved(ctx, id, sb)
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

// releaseResolved drops the resolved claim and tears down its VM. resolve
// unlocks before this runs, so it re-checks under m.mu that sb is still the
// live claim: a second release racing in must not tear down twice.
func (m *Manager) releaseResolved(ctx context.Context, id string, sb *types.Sandbox) error {
	m.mu.Lock()
	if m.claimed[id] != sb {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	snap, ck, vmName := sb.HibernateSnap, sb.ArchiveCk, sb.VMName
	if ck != "" {
		if markErr := m.markArchiveCk(ck); markErr != nil {
			m.mu.Unlock()
			return fmt.Errorf("release %s: track archive checkpoint: %w", id, markErr)
		}
		m.pendingCks[ck] = struct{}{}
	}
	delete(m.claimed, id)
	m.tenantDelta(sb.Tenant, -1)
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		m.claimed[id] = sb // roll back so memory matches the still-durable claim; the restored claim re-pins ck
		m.tenantDelta(sb.Tenant, 1)
		delete(m.pendingCks, ck)
		rb := m.store.snapshot(m.claimed)
		m.mu.Unlock()
		if ck != "" {
			if clearErr := m.clearArchiveCk(ck); clearErr != nil {
				log.WithFunc("pool.releaseResolved").Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
			}
		}
		m.recommit(ctx, rb)
		log.WithFunc("pool.releaseResolved").Errorf(ctx, saveErr, "persist release of %s", id)
		return fmt.Errorf("release %s: %w", id, saveErr)
	}
	// Cleanup must survive the caller hanging up; the claim is already dropped.
	ctx = context.WithoutCancel(ctx)
	// Barrier before the VM is torn down: publish the workspace one last time
	// so every local change is on the NAS and visible to other clients.
	m.barrierWorkspace(ctx, id)
	if ck != "" {
		m.purgeArchiveCk(ctx, id, ck, sb.Tenant) // archived: no local VM
		m.untrack(m.pendingCks, ck)
	}
	td := m.quiesceVolumes(ctx, sb)
	var err error
	if vmName == "" {
		m.finishVolumeTeardown(ctx, td) // archived: no VM to confirm gone
	} else if !m.removeOrRetry(ctx, vmName, id, "", td) {
		err = fmt.Errorf("vm %s survived removal", vmName)
	}
	m.disarmEgress(id, err == nil)
	m.dropSnap(ctx, snap)
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: id, VMName: vmName})
	return err
}

// overQuota is a cheap advisory precheck; finalizeBatch does the authoritative
// admission check, so this just spares a doomed request the provision cost.
func (m *Manager) overQuota(extra int, tenant string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.quotaErr(extra, tenant)
}

// admitVolumes takes one claim's holds, then the marker check; a refusal on either leaves nothing held.
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

// admitClaim is the provision path's one admission section: the advisory quota
// precheck plus the authoritative volume reservation, so a busy volume is
// refused before any VM is built.
func (m *Manager) admitClaim(tenant string, volumes []types.Volume) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.quotaErr(1, tenant); err != nil {
		return err
	}
	return m.reserveVolumes(volumes)
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
			// No quiesce: the claim was never handed out, so nothing wrote to
			// the guest and the marker converges on the next writable claim.
			m.unreserveVolumes(sb.Volumes)
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
		m.recordUsage(ctx, usageEvent{
			Event: "claim", //nolint:goconst // event name; other occurrences are test assertions
			ID:    sb.ID, VMName: sb.VMName,
			KeyHash: sb.Key.Hash(), Tenant: sb.Tenant,
			Volumes: types.VolumeNames(sb.Volumes), VolumesRW: types.VolumeRWNames(sb.Volumes),
		})
	}
	// Workspace sync is a best-effort enhancement: a failure to bind the NAS
	// workspace must not fail an otherwise-good claim, so it arms after the
	// batch is durable and armed, and only logs on error.
	for _, sb := range sbs {
		m.armWorkspace(ctx, sb)
	}
	return nil
}

// armWorkspace binds a claimed sandbox to its shared workspace and starts the
// sync loops. No-op when workspace sync is disabled or the sandbox carries no
// workspace. Best-effort: an error is logged, not returned.
func (m *Manager) armWorkspace(ctx context.Context, sb *types.Sandbox) {
	if m.wsMgr == nil || sb.Workspace == "" || sb.VsockSocket == "" {
		return
	}
	ws := filecache.WorkspaceDir(m.wsRoot, sb.Workspace)
	// One writer per sandbox: the sandbox id is globally unique, so it is a
	// safe, collision-free writer id across nodes.
	cfg := filecache.Config{VMName: sb.VMName, DedicatedDisk: m.wsDedicatedDisk}
	if err := m.wsMgr.Arm(ctx, sb.ID, sb.VsockSocket, ws, sb.ID, cfg); err != nil {
		log.WithFunc("pool.armWorkspace").Errorf(ctx, err, "arm workspace %s for %s", sb.Workspace, sb.ID)
	}
}

// barrierWorkspace runs the final workspace sync for id (publish local changes
// to the NAS) and stops its loops. No-op when sync is disabled or id has none.
func (m *Manager) barrierWorkspace(ctx context.Context, id string) {
	if m.wsMgr == nil {
		return
	}
	m.wsMgr.Barrier(ctx, id)
}

// EnableWorkspaceSync turns on the workspace filecache, driving guests through
// g and resolving workspace tokens under root. Call once before serving.
func (m *Manager) EnableWorkspaceSync(g filecache.Guest, root string) {
	m.wsMgr = filecache.NewManager(g)
	m.wsRoot = root
}

// EnableWorkspaceDisk turns on the dedicated-workspace-disk mode for every
// workspace claim: a fresh ext4 virtio-blk disk (image under imageRoot, sized
// sizeMB) is attached read-write and mounted before hydration, isolating the
// workspace from the guest rootfs layer. Requires EnableWorkspaceSync first.
func (m *Manager) EnableWorkspaceDisk(d filecache.Disk, imageRoot string, sizeMB int) {
	if m.wsMgr == nil {
		return
	}
	m.wsMgr.EnableDedicatedDisk(d, imageRoot, sizeMB)
	m.wsDedicatedDisk = true
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
		td := m.quiesceVolumes(ctx, sb)
		m.disarmEgress(sb.ID, m.removeOrRetry(ctx, sb.VMName, sb.ID, "", td))
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
	m.mu.Unlock()
	if len(expired) == 0 {
		return
	}

	logger := log.WithFunc("pool.reapOnce")
	keep := expired[:0]
	for _, v := range expired {
		if v.action == reapPurge {
			if markErr := m.markArchiveCk(v.ck); markErr != nil {
				logger.Errorf(ctx, markErr, "mark archive ck %s; keeping %s", v.ck, v.id)
				continue
			}
		}
		keep = append(keep, v)
	}
	expired = keep
	if len(expired) == 0 {
		return
	}
	m.mu.Lock()
	keep = expired[:0]
	var purgeCks []string
	for _, v := range expired {
		if v.action == reapPurge {
			if m.claimed[v.id] != v.sb || v.sb.ArchiveCk != v.ck {
				continue
			}
			m.pendingCks[v.ck] = struct{}{}
			delete(m.claimed, v.id)
			m.tenantDelta(v.tenant, -1)
			purgeCks = append(purgeCks, v.ck)
		}
		keep = append(keep, v)
	}
	expired = keep
	if len(expired) == 0 {
		m.mu.Unlock()
		return
	}
	js := m.store.snapshot(m.claimed)
	m.mu.Unlock()
	if saveErr := m.store.commit(js); saveErr != nil {
		m.mu.Lock()
		for _, v := range expired {
			if v.action == reapArchive {
				delete(m.archiving, v.id) // never removed from m.claimed
			} else {
				m.claimed[v.id] = v.sb // roll back so memory matches the still-durable claim
				m.tenantDelta(v.tenant, 1)
				delete(m.pendingCks, v.ck)
			}
		}
		rb := m.store.snapshot(m.claimed)
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
	// Engine subprocesses (worst case minutes on a hung stop): fan out bounded
	// so a big batch never stalls the ticker loop.
	m.runBounded(ctx, len(expired), func(ctx context.Context, i int) {
		v := expired[i]
		switch v.action {
		case reapPurge:
			m.disarmEgress(v.id, true)
			m.purgeArchiveCk(ctx, v.id, v.ck, v.tenant)
			m.untrack(m.pendingCks, v.ck)
			logger.Infof(ctx, "purged archived sandbox %s", v.id)
		case reapArchive:
			logSweepResult(ctx, logger, m.archive(ctx, v.sb), "archived expired sandbox "+v.id, "archive expired sandbox "+v.id)
		default:
			m.barrierWorkspace(ctx, v.id)
			td := m.quiesceVolumes(ctx, v.sb)
			m.disarmEgress(v.id, m.removeOrRetry(ctx, v.vmName, v.id, "", td))
			m.dropSnap(ctx, v.snap)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usageEvent{Event: "reap", ID: v.id, VMName: v.vmName})
			logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
		}
	})
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

func (m *Manager) claimProvision(ctx context.Context, key types.PoolKey, ttl time.Duration, tenant, claimRef, workspace string, volumes []types.Volume, requirePromoted bool) (*types.Sandbox, error) {
	start := time.Now()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	volumeSpecs, err := m.resolveVolumes(key, tenant, volumes)
	if err != nil {
		return nil, err
	}
	applied, err := m.admitVolumes(tenant, volumeSpecs)
	if err != nil {
		return nil, err
	}
	reserved := applied
	defer func() { m.unreserveVolumes(reserved) }()
	golden, err := m.resolveGolden(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("resolve template: %w", err)
	}
	if requirePromoted && !golden.promoted {
		golden.release()
		return nil, ErrVolumeUnavailable
	}
	sb, err := m.provision(ctx, key, golden.dir)
	golden.release()
	if err != nil {
		return nil, err
	}
	if volumeErr := m.applyVolumes(ctx, sb, volumeSpecs, applied); volumeErr != nil {
		m.destroy(ctx, sb.VMName)
		return nil, volumeErr
	}
	sb.TemplateDigest = golden.templateDigest
	sb.Tenant = tenant
	sb.ClaimRef = claimRef
	sb.Workspace = workspace
	reserved = nil
	out, err := m.finalize(ctx, sb, ttl)
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
