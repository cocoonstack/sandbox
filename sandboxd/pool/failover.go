package pool

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

// AdoptArchived takes over a claim whose owner's lease expired and reports whether this node owns it now; the root needs no token match.
func (m *Manager) AdoptArchived(ctx context.Context, id string, cred Cred) (bool, error) {
	return m.adopt(ctx, id, cred, false)
}

// AdoptForWake adopts as AdoptArchived does, but only while this node could admit the claim's wake.
func (m *Manager) AdoptForWake(ctx context.Context, id string, cred Cred) (bool, error) {
	return m.adopt(ctx, id, cred, true)
}

func (m *Manager) adopt(ctx context.Context, id string, cred Cred, room bool) (bool, error) {
	if m.archives == nil {
		return false, nil
	}
	for range 2 {
		led := false
		adopted, err, _ := m.adoptFlights.Do(id, func() (any, error) {
			led = true
			return m.adoptArchived(ctx, id, cred, room)
		})
		if led || err != nil {
			return adopted.(bool), err
		}
		m.mu.Lock()
		_, held := m.resolveLocked(id, cred)
		m.mu.Unlock()
		if held {
			return true, nil
		}
	}
	return false, nil
}

func (m *Manager) adoptArchived(ctx context.Context, id string, cred Cred, room bool) (bool, error) {
	m.mu.Lock()
	draining, cordoned := m.draining, m.cordoned
	_, held := m.resolveLocked(id, cred)
	fenced := m.adoptFenced()
	full := false
	if room {
		_, roomErr := m.wakeRoomErrLocked()
		full = roomErr != nil
	}
	m.mu.Unlock()
	switch {
	case draining:
		return false, nil
	case held:
		return true, nil
	case fenced, cordoned, full:
		return false, nil
	}
	ctx = context.WithoutCancel(ctx)
	rec, ok, err := m.archives.Get(ctx, id)
	if err != nil || !ok {
		return false, err
	}
	if rec.Consumed || (!cred.Operator && !tokenMatches(cred.Token, rec.TokenSHA256)) || (!rec.Deadline.IsZero() && time.Now().After(rec.Deadline)) {
		return false, nil
	}
	sb := &types.Sandbox{}
	if err = json.Unmarshal(rec.Claim, sb); err != nil {
		return false, fmt.Errorf("adopt %s: decode claim: %w", id, err)
	}
	if err = m.confirmAdoptedTun(ctx, sb); err != nil {
		return false, fmt.Errorf("adopt %s: confirm overlay address: %w", id, err)
	}
	epoch, err := m.archives.Take(ctx, id, rec.Epoch, m.nodeID)
	if errors.Is(err, archives.ErrLost) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("adopt %s: %w", id, err)
	}
	sb.Token, sb.TokenSHA256, sb.ArchiveCk, sb.ArchiveEpoch, sb.Deadline = cred.Token, "", rec.Checkpoint, epoch, rec.Deadline
	if cred.Operator {
		sb.Token, sb.TokenSHA256 = randHex(16), rec.TokenSHA256
	}
	sb.SetEnv(sb.Env)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	if sb.Deadline.IsZero() {
		sb.Deadline = m.retentionDeadline(sb.Key)
	}
	m.addClaimLocked(sb)
	js, adoptedAt, retention := m.store.set(sb), time.Now(), sb.Deadline
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.forgetClaim(ctx, sb)
		return false, fmt.Errorf("adopt %s: persist claims: %w", id, err)
	}
	m.recordUsage(ctx, usage.Event{Time: adoptedAt, Event: "adopt", ID: id, Reference: rec.Checkpoint, Tenant: sb.Tenant, Deadline: retention})
	log.WithFunc("pool.adoptArchived").Infof(ctx, "adopted archived sandbox %s from %s", id, rec.Owner)
	return true, nil
}

func (m *Manager) putArchive(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	return m.putRow(ctx, sb, ck, false, false)
}

// writeArchiveRow upserts the archive row, or replaces the replica row the claim holds; the caller holds sb.Transition.
func (m *Manager) writeArchiveRow(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	if m.replicaOf(sb) == "" {
		return m.putArchive(ctx, sb, ck)
	}
	epoch, err := m.replaceArchive(ctx, sb, ck)
	if errors.Is(err, archives.ErrLost) {
		_ = m.refusedReplace(ctx, sb)
	}
	return epoch, err
}

// replaceArchive writes an archive row over the replica row this node holds, so a row that left meanwhile is not recreated.
func (m *Manager) replaceArchive(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	return m.putRow(ctx, sb, ck, false, true)
}

// replaceReplica moves the row from the epoch this node holds; a row another node took or deleted refuses the write.
func (m *Manager) replaceReplica(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	return m.putRow(ctx, sb, ck, true, true)
}

func (m *Manager) putRow(ctx context.Context, sb *types.Sandbox, ck string, replica, replace bool) (int64, error) {
	m.mu.Lock()
	if replica && m.hasVolumeHolds(sb) {
		m.mu.Unlock()
		return 0, ErrVolumeCapture
	}
	dto, deadline, held, sum := dtoOf(sb), sb.Deadline, sb.ArchiveEpoch, cmp.Or(sb.TokenSHA256, tenants.TokenSHA256(sb.Token))
	switch {
	case replica:
		deadline = time.Time{}
	case sb.ArchiveCk == "":
		deadline = m.archiveDeadline(sb)
	}
	m.mu.Unlock()
	dto.Token, dto.TokenSHA256, dto.VMName, dto.VsockSocket, dto.HibernateSnap, dto.PendingSnap, dto.ArchivePending, dto.ReplicaCk, dto.ReplicaPending = "", "", "", "", "", "", "", "", ""
	dto.ArchiveCk, dto.Deadline = ck, deadline
	if m.view.Load().hostEnvMemory {
		dto.Env = dto.Env.DropHostValues()
	}
	dto.ReaderSafe = dto.ReaderSafe && !replica
	claim, err := json.Marshal(&dto)
	if err != nil {
		return 0, fmt.Errorf("encode claim: %w", err)
	}
	rec := archives.Record{
		ID: sb.ID, Tenant: sb.Tenant, TokenSHA256: sum, Owner: m.nodeID,
		Checkpoint: ck, Deadline: deadline, Claim: claim,
	}
	if replace {
		return m.archives.Replace(ctx, rec, held)
	}
	return m.archives.Put(ctx, rec)
}

func (m *Manager) rewriteArchiveRow(ctx context.Context, sb *types.Sandbox, locked bool) error {
	if !locked {
		sb.Transition.Lock()
		defer sb.Transition.Unlock()
	}
	for range 2 {
		m.mu.Lock()
		ck, replica, live, rowed := sb.ArchiveCk, sb.ReplicaCk, m.claimed[sb.ID] == sb, m.rowFollows(sb)
		m.mu.Unlock()
		switch {
		case !live:
			return ErrUnknownSandbox
		case !rowed:
			return nil
		}
		epoch, err := m.putRow(ctx, sb, cmp.Or(ck, replica), ck == "", true)
		if err == nil {
			_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
			return nil
		}
		if !errors.Is(err, archives.ErrLost) {
			return err
		}
		if err = m.refusedReplace(ctx, sb); err != nil {
			return err
		}
	}
	return nil
}

// refusedReplace settles a refused replace: a row this node still owns only had a stale epoch, anything else means the claim moved.
func (m *Manager) refusedReplace(ctx context.Context, sb *types.Sandbox) error {
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	switch {
	case err != nil:
		return err
	case ok && rec.Owner == m.nodeID && rec.Consumed:
		m.reputArchive(ctx, sb)
		return nil
	case ok && rec.Owner == m.nodeID:
		_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = max(sb.ArchiveEpoch, rec.Epoch) })
		return nil
	}
	m.dropAdopted(ctx, sb)
	return ErrUnknownSandbox
}

func (m *Manager) checkArchiveRow(ctx context.Context, id string) error {
	if m.archives == nil {
		return nil
	}
	rec, ok, err := m.archives.Get(ctx, id)
	if err == nil && ok && rec.Owner != m.nodeID && rec.Owner != "" {
		return fmt.Errorf("%w: held by %s", archives.ErrLost, rec.Owner)
	}
	return err
}

func (m *Manager) abandonRefusedPut(ctx context.Context, sb *types.Sandbox, ck string, putErr error) {
	if errors.Is(putErr, archives.ErrLost) {
		m.abandonArchive(ctx, sb, 0, ck)
	}
}

func (m *Manager) abandonArchive(ctx context.Context, sb *types.Sandbox, epoch int64, ck string) {
	if epoch != 0 {
		if err := m.archives.Delete(ctx, sb.ID, epoch); err != nil {
			log.WithFunc("pool.abandonArchive").Warnf(ctx, "drop archive row %s, keeping ck %s: %v", sb.ID, ck, err)
			return
		}
	}
	if m.clearArchiveIntent(ctx, sb, ck) {
		m.deleteOrphanArchiveCk(ctx, ck)
	}
}

func (m *Manager) clearArchiveIntent(ctx context.Context, sb *types.Sandbox, ck string) bool {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || sb.ArchivePending != ck {
		m.mu.Unlock()
		return true
	}
	sb.ArchivePending = ""
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
		return false
	}
	return true
}

// takeArchive fences an owner's wake against an adoption elsewhere; the caller holds Transition.
func (m *Manager) takeArchive(ctx context.Context, sb *types.Sandbox) (int64, error) {
	if m.archives == nil || sb.ArchiveEpoch == 0 {
		return 0, nil
	}
	epoch, err := m.archives.Take(ctx, sb.ID, sb.ArchiveEpoch, m.nodeID)
	if errors.Is(err, archives.ErrLost) {
		return m.settleArchive(ctx, sb)
	}
	if err != nil {
		return 0, fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if live, _ := m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch }); !live {
		return 0, ErrUnknownSandbox
	}
	return epoch, nil
}

func (m *Manager) settleArchive(ctx context.Context, sb *types.Sandbox) (int64, error) {
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	if err != nil {
		return 0, fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	epoch, kept, err := m.keepOwnRow(ctx, sb, rec, ok)
	switch {
	case err != nil:
		return 0, fmt.Errorf("wake %s: %w", sb.ID, err)
	case !kept:
		m.dropAdopted(ctx, sb)
		return 0, ErrUnknownSandbox
	}
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
	return epoch, nil
}

func (m *Manager) keepOwnRow(ctx context.Context, sb *types.Sandbox, rec archives.Record, ok bool) (int64, bool, error) {
	if !ok || rec.Owner != m.nodeID {
		return 0, false, nil
	}
	if !rec.Consumed {
		return rec.Epoch, true, nil
	}
	epoch, err := m.putArchive(ctx, sb, sb.ArchiveCk)
	if errors.Is(err, archives.ErrLost) {
		return 0, false, nil
	}
	return epoch, err == nil, err
}

func (m *Manager) consumeRow(ctx context.Context, sb *types.Sandbox, epoch int64, op string) (int64, error) {
	if epoch == 0 {
		return 0, nil
	}
	consumed, err := m.archives.Consume(ctx, sb.ID, epoch)
	if errors.Is(err, archives.ErrLost) {
		consumed, err = m.settleConsume(ctx, sb)
	}
	switch {
	case errors.Is(err, ErrUnknownSandbox):
		return 0, err
	case err != nil:
		return 0, fmt.Errorf("%s %s: %w", op, sb.ID, err)
	}
	return consumed, nil
}

func (m *Manager) dropTombstone(ctx context.Context, id string, epoch int64) {
	if epoch == 0 {
		return
	}
	if err := m.archives.Delete(ctx, id, epoch); err != nil && !errors.Is(err, archives.ErrLost) {
		log.WithFunc("pool.dropTombstone").Warnf(ctx, "drop archive tombstone %s: %v", id, err)
	}
}

func (m *Manager) reputArchive(ctx context.Context, sb *types.Sandbox) {
	epoch, err := m.putRow(ctx, sb, cmp.Or(sb.ArchiveCk, sb.ReplicaCk), sb.ArchiveCk == "", false)
	if err != nil {
		if !errors.Is(err, ErrVolumeCapture) {
			log.WithFunc("pool.reputArchive").Warnf(ctx, "re-put archive row %s: %v", sb.ID, err)
		}
		return
	}
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
}

func (m *Manager) releaseRowed(ctx context.Context, id string, sb *types.Sandbox) error {
	if err := m.lockTransition(sb); err != nil {
		return err
	}
	defer sb.Transition.Unlock()
	if err := m.resolveIntent(context.WithoutCancel(ctx), sb); err != nil {
		return err
	}
	replica := m.replicaOf(sb)
	consumed, err := m.consumeArchive(ctx, sb)
	if err != nil {
		return err
	}
	if err = m.releaseResolved(ctx, id, sb); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	m.dropTombstone(ctx, id, consumed)
	if replica != "" {
		m.deleteOrphanArchiveCk(ctx, replica)
	}
	return nil
}

func (m *Manager) consumeArchive(ctx context.Context, sb *types.Sandbox) (int64, error) {
	m.mu.Lock()
	rowed, epoch := m.archiveRowed(sb) || m.replicaRowed(sb), sb.ArchiveEpoch
	m.mu.Unlock()
	if !rowed {
		return 0, nil
	}
	ctx = context.WithoutCancel(ctx)
	consumed, err := m.consumeRow(ctx, sb, epoch, "consume archive row")
	if err != nil {
		return 0, err
	}
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = 0 })
	return consumed, nil
}

func (m *Manager) settleConsume(ctx context.Context, sb *types.Sandbox) (int64, error) {
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	switch {
	case err != nil:
		return 0, err
	case !ok || rec.Owner != m.nodeID:
		m.dropAdopted(ctx, sb)
		return 0, ErrUnknownSandbox
	case rec.Consumed:
		return rec.Epoch, nil
	}
	consumed, err := m.archives.Consume(ctx, sb.ID, rec.Epoch)
	if errors.Is(err, archives.ErrLost) {
		m.dropAdopted(ctx, sb)
		return 0, ErrUnknownSandbox
	}
	return consumed, err
}

// unrowExpired drops an expired claim's row, so the next reap purges or destroys it as a row-less claim.
func (m *Manager) unrowExpired(ctx context.Context, sb *types.Sandbox) {
	defer m.untrack(m.archiving, sb.ID)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	var err error
	m.mu.Lock()
	replicaHeld := sb.HasReplica()
	m.mu.Unlock()
	if replicaHeld {
		err = m.dropReplica(ctx, sb)
	} else {
		_, err = m.consumeArchive(ctx, sb)
	}
	if err != nil && !errors.Is(err, ErrUnknownSandbox) {
		log.WithFunc("pool.unrowExpired").Warnf(ctx, "purge %s: %v", sb.ID, err)
	}
}

// dropAdopted forgets a claim another node owns now and removes its local VM; an archive checkpoint is the new owner's.
func (m *Manager) dropAdopted(ctx context.Context, sb *types.Sandbox) {
	if teardown := m.forgetAdopted(ctx, sb); teardown != nil {
		teardown()
	}
}

// forgetAdopted forgets the claim at once and returns the removal of its VM; nil when the claim was already gone.
func (m *Manager) forgetAdopted(ctx context.Context, sb *types.Sandbox) func() {
	logger := log.WithFunc("pool.forgetAdopted")
	m.mu.Lock()
	ck, vm, snap := sb.ArchiveCk, sb.VMName, sb.HibernateSnap
	m.mu.Unlock()
	if ck != "" {
		if err := m.ckMarks.clear(ck); err != nil {
			logger.Warnf(ctx, "clear archive ck %s: %v", ck, err)
		}
	}
	holds, ok := m.forgetClaim(ctx, sb)
	if !ok {
		return nil
	}
	return func() {
		m.out.Disarm(sb.ID, true)
		if vm != "" {
			m.removeOrRetry(ctx, vm, "", volumeTeardown{holds: holds})
		}
		m.dropSnap(ctx, snap)
		logger.Infof(ctx, "sandbox %s moved to another node", sb.ID)
	}
}

func (m *Manager) forgetClaim(ctx context.Context, sb *types.Sandbox) ([]types.Volume, bool) {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		return nil, false
	}
	holds := m.cancelVolumeRun(sb)
	m.dropClaimLocked(sb)
	js := m.store.del(sb.ID)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
	}
	return holds, true
}

func (m *Manager) sweepOrphanArchives(ctx context.Context) {
	if !m.sweeps.orphan.CompareAndSwap(false, true) {
		return
	}
	defer m.sweeps.orphan.Store(false)
	m.settleIntents(ctx)
	now := time.Now()
	recs, err := m.archives.Sweepable(ctx, now, m.nodeID)
	if err != nil {
		log.WithFunc("pool.sweepOrphanArchives").Warnf(ctx, "list sweepable archives: %v", err)
		return
	}
	for _, rec := range recs {
		switch {
		case rec.Owner == m.nodeID:
			m.reconcileOwnRow(ctx, rec, now)
		case !rec.Consumed:
			m.purgeArchiveRow(ctx, rec)
		}
	}
}

func (m *Manager) reconcileOwnRow(ctx context.Context, rec archives.Record, now time.Time) {
	m.mu.Lock()
	sb := m.claimed[rec.ID]
	m.mu.Unlock()
	if sb == nil {
		switch {
		case rec.Consumed || !rec.Deadline.IsZero() && now.After(rec.Deadline):
			m.purgeArchiveRow(ctx, rec)
		case rec.Deadline.IsZero():
			if err := m.startRowRetention(ctx, rec.ID, rec.Epoch); err != nil && !errors.Is(err, archives.ErrLost) {
				log.WithFunc("pool.reconcileOwnRow").Warnf(ctx, "start retention of archive row %s: %v", rec.ID, err)
			}
		}
		return
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	held, refs, epoch := m.claimed[rec.ID] == sb, sb.ArchiveCk == rec.Checkpoint || sb.ReplicaCk == rec.Checkpoint || sb.ReplicaPending == rec.Checkpoint, sb.ArchiveEpoch
	m.mu.Unlock()
	switch {
	case !held:
	case refs && rec.Consumed && epoch != 0:
		m.reputArchive(ctx, sb)
	case refs:
	case rec.Consumed:
		m.purgeArchiveRow(ctx, rec)
	case !m.archiveCkPinned(rec.Checkpoint):
		m.abandonArchive(ctx, sb, rec.Epoch, rec.Checkpoint)
	}
}

func (m *Manager) startRowRetention(ctx context.Context, id string, epoch int64) error {
	rec, ok, err := m.archives.Get(ctx, id)
	if err != nil || !ok {
		return err
	}
	sb := &types.Sandbox{}
	if err = json.Unmarshal(rec.Claim, sb); err != nil {
		return err
	}
	m.mu.Lock()
	rec.Deadline = m.retentionDeadline(sb.Key)
	m.mu.Unlock()
	if rec.Deadline.IsZero() {
		return nil
	}
	_, err = m.archives.Replace(ctx, rec, epoch)
	return err
}

func (m *Manager) purgeArchiveRow(ctx context.Context, rec archives.Record) {
	logger := log.WithFunc("pool.purgeArchiveRow")
	deleted, err := m.deleteArchiveCk(ctx, rec.Checkpoint)
	if err != nil {
		logger.Warnf(ctx, "purge archive %s: delete ck %s: %v", rec.ID, rec.Checkpoint, err)
		return
	}
	if !deleted {
		return
	}
	switch err := m.archives.Delete(ctx, rec.ID, rec.Epoch); {
	case errors.Is(err, archives.ErrLost):
		return
	case err != nil:
		logger.Warnf(ctx, "purge archive %s: %v", rec.ID, err)
		return
	}
	if !rec.Consumed {
		m.counters.archiveDeletes.Add(1)
		m.recordUsage(ctx, usage.Event{Event: "archive_delete", ID: rec.ID, Reference: rec.Checkpoint, Tenant: rec.Tenant})
		m.releaseRowTun(ctx, rec)
	}
	logger.Infof(ctx, "purged archive %s of %s", rec.ID, rec.Owner)
}

// fenceArchived settles loaded claims against their rows; false when the registry did not answer.
func (m *Manager) fenceArchived(ctx context.Context, claims map[string]*types.Sandbox) bool {
	if m.archives == nil {
		return true
	}
	logger := log.WithFunc("pool.fenceArchived")
	rows, err := m.archives.Rows(ctx, slices.Collect(maps.Keys(claims)))
	if err != nil {
		logger.Warnf(ctx, "archive fencing and orphan reclaim skipped: %v", err)
		return false
	}
	for id, sb := range claims {
		rec, ok := rows[id]
		drop := false
		switch {
		case sb.ArchiveCk != "" && sb.ArchiveEpoch == 0:
		case sb.ArchiveCk != "":
			epoch, kept, err := m.keepOwnRow(ctx, sb, rec, ok)
			switch {
			case err != nil:
				logger.Warnf(ctx, "re-arm archive row %s: %v", id, err)
			case kept:
				sb.ArchiveEpoch = epoch
			default:
				drop = true
			}
		case ok && rec.Owner != m.nodeID:
			drop = true
		case sb.HasReplica() && sb.ArchivePending == "":
			var moved bool
			var err error
			if !ok {
				moved, err = m.replicaGone(ctx, sb)
			}
			switch {
			case err != nil:
				logger.Warnf(ctx, "replica intent of %s left for the next settle: %v", id, err)
			case moved:
				drop = true
			default:
				if ok && !rec.Consumed {
					sb.ReplicaCk, sb.ArchiveEpoch = rec.Checkpoint, rec.Epoch
				}
				sb.ReplicaPending = ""
			}
		case sb.ArchivePending != "" || ok && !rec.Consumed:
			moved, err := m.settleIntent(ctx, sb, sb.ArchivePending, rec, ok)
			switch {
			case err != nil:
				logger.Warnf(ctx, "archive intent of %s left for the next restart: %v", id, err)
			case moved:
				drop = true
			default:
				sb.ArchivePending = ""
			}
		}
		if drop {
			if sb.ArchiveCk != "" {
				if err := m.ckMarks.clear(sb.ArchiveCk); err != nil {
					logger.Warnf(ctx, "clear archive ck %s: %v", sb.ArchiveCk, err)
				}
			}
			delete(claims, id)
			logger.Infof(ctx, "dropped sandbox %s: another node holds it", id)
		}
	}
	return true
}

func (m *Manager) settleIntent(ctx context.Context, sb *types.Sandbox, pending string, rec archives.Record, ok bool) (moved bool, err error) {
	m.mu.Lock()
	replica := sb.ReplicaCk
	m.mu.Unlock()
	if ok && rec.Owner == m.nodeID && replica != "" {
		return false, m.restoreReplicaRow(ctx, sb, replica, rec)
	}
	if ok && rec.Owner == m.nodeID {
		err = m.archives.Delete(ctx, sb.ID, rec.Epoch)
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, archives.ErrLost) {
			return false, err
		}
		if rec, ok, err = m.archives.Get(ctx, sb.ID); err != nil {
			return false, err
		}
		if ok && rec.Owner == m.nodeID {
			return false, fmt.Errorf("archive row %s moved under its settle", sb.ID)
		}
	}
	switch {
	case ok:
		return true, nil
	case pending == "":
		return false, nil
	}
	_, err = m.ckpts.ReadMeta(ctx, pending)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}
	return false, err
}

func (m *Manager) restoreReplicaRow(ctx context.Context, sb *types.Sandbox, replica string, rec archives.Record) error {
	m.mu.Lock()
	sb.ArchiveEpoch = rec.Epoch
	m.mu.Unlock()
	if rec.Checkpoint == replica || rec.Consumed {
		return nil
	}
	epoch, err := m.replaceReplica(ctx, sb, replica)
	if err != nil {
		return err
	}
	m.mu.Lock()
	sb.ArchiveEpoch = epoch
	m.mu.Unlock()
	return nil
}

func (m *Manager) resolveIntent(ctx context.Context, sb *types.Sandbox) error {
	if m.archives == nil {
		return nil
	}
	if err := m.settleReplicaIntent(ctx, sb); err != nil {
		return err
	}
	m.mu.Lock()
	pending := sb.ArchivePending
	m.mu.Unlock()
	if pending == "" {
		return nil
	}
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	moved, err := m.settleIntent(ctx, sb, pending, rec, ok)
	switch {
	case err != nil:
		return err
	case moved:
		m.dropAdopted(ctx, sb)
		return ErrUnknownSandbox
	}
	if m.clearArchiveIntent(ctx, sb, pending) {
		m.deleteOrphanArchiveCk(ctx, pending)
	}
	return nil
}

func (m *Manager) settleIntents(ctx context.Context) {
	m.mu.Lock()
	var pending []*types.Sandbox
	for id, sb := range m.claimed {
		if _, busy := m.archiving[id]; (sb.ArchivePending != "" || sb.ReplicaPending != "") && !busy {
			pending = append(pending, sb)
		}
	}
	m.mu.Unlock()
	for _, sb := range pending {
		sb.Transition.Lock()
		if err := m.resolveIntent(ctx, sb); err != nil && !errors.Is(err, ErrUnknownSandbox) {
			log.WithFunc("pool.settleIntents").Warnf(ctx, "settle archive intent of %s: %v", sb.ID, err)
		}
		sb.Transition.Unlock()
	}
}

func (m *Manager) archiveRowed(sb *types.Sandbox) bool {
	return m.archives != nil && sb.ArchiveCk != "" && sb.ArchiveEpoch != 0
}

func (m *Manager) rowFollows(sb *types.Sandbox) bool {
	return m.archiveRowed(sb) || m.replicaRowed(sb) && sb.HibernateSnap != ""
}

func (m *Manager) rowRewriteDue(sb *types.Sandbox) bool {
	return m.rowFollows(sb) || sb.ArchivePending != "" || sb.ReplicaPending != "" && sb.HibernateSnap != ""
}

func (m *Manager) expiryDropsRow(sb *types.Sandbox) bool {
	return m.archiveRowed(sb) || m.archives != nil && sb.HasReplica() && !m.archivesAtDeadline(sb)
}
