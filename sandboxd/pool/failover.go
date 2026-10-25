package pool

import (
	"context"
	"crypto/subtle"
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
)

// AdoptArchived takes over a claim whose owner's lease expired and reports whether this node owns it now.
func (m *Manager) AdoptArchived(ctx context.Context, id, token string) (bool, error) {
	if m.archives == nil {
		return false, nil
	}
	adopted, err, _ := m.adoptFlights.Do(id+"\x00"+token, func() (any, error) { return m.adoptArchived(ctx, id, token) })
	return adopted.(bool), err
}

func (m *Manager) adoptArchived(ctx context.Context, id, token string) (bool, error) {
	m.mu.Lock()
	draining := m.draining
	m.mu.Unlock()
	if draining {
		return false, nil
	}
	ctx = context.WithoutCancel(ctx)
	rec, ok, err := m.archives.Get(ctx, id)
	if err != nil || !ok {
		return false, err
	}
	sum := tenants.TokenSHA256(token)
	if rec.Consumed || subtle.ConstantTimeCompare([]byte(sum), []byte(rec.TokenSHA256)) != 1 || (!rec.Deadline.IsZero() && time.Now().After(rec.Deadline)) {
		return false, nil
	}
	sb := &types.Sandbox{}
	if err = json.Unmarshal(rec.Claim, sb); err != nil {
		return false, fmt.Errorf("adopt %s: decode claim: %w", id, err)
	}
	epoch, err := m.archives.Take(ctx, id, rec.Epoch, m.nodeID)
	if errors.Is(err, archives.ErrLost) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("adopt %s: %w", id, err)
	}
	sb.Token, sb.ArchiveCk, sb.ArchiveEpoch, sb.Deadline = token, rec.Checkpoint, epoch, rec.Deadline
	sb.SetEnv(sb.Env)
	m.mu.Lock()
	if sb.Deadline.IsZero() {
		sb.Deadline = m.retentionDeadline(sb.Key)
	}
	if cur, dup := m.claimed[id]; dup {
		m.mu.Unlock()
		return m.adoptHeld(ctx, cur, epoch), nil
	}
	m.addClaimLocked(sb)
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.forgetClaim(ctx, sb)
		return false, fmt.Errorf("adopt %s: persist claims: %w", id, err)
	}
	log.WithFunc("pool.AdoptArchived").Infof(ctx, "adopted archived sandbox %s from %s", id, rec.Owner)
	return true, nil
}

func (m *Manager) adoptHeld(ctx context.Context, cur *types.Sandbox, epoch int64) bool {
	cur.Transition.Lock()
	defer cur.Transition.Unlock()
	m.mu.Lock()
	held := m.claimed[cur.ID] == cur
	m.mu.Unlock()
	if held {
		_, _ = m.commitIfLive(ctx, cur, func() { cur.ArchiveEpoch = epoch })
		return true
	}
	if rec, ok, err := m.archives.Get(ctx, cur.ID); err == nil && ok && rec.Owner == m.nodeID && !rec.Consumed {
		if consumed, err := m.archives.Consume(ctx, cur.ID, rec.Epoch); err == nil {
			rec.Epoch, rec.Consumed = consumed, true
			m.purgeArchiveRow(ctx, rec)
		}
	}
	return false
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

// putReplica writes a live claim's row with no deadline: retention starts when the claim is archived or adopted.
func (m *Manager) putReplica(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	return m.putRow(ctx, sb, ck, true, false)
}

// replaceReplica moves the row from the epoch this node holds; a row another node took or deleted refuses the write.
func (m *Manager) replaceReplica(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	return m.putRow(ctx, sb, ck, true, true)
}

func (m *Manager) putRow(ctx context.Context, sb *types.Sandbox, ck string, replica, replace bool) (int64, error) {
	if m.archives == nil {
		return 0, nil
	}
	m.mu.Lock()
	if replica && m.hasVolumeHolds(sb) {
		m.mu.Unlock()
		return 0, ErrVolumeCapture
	}
	dto, deadline, held := dtoOf(sb), sb.Deadline, sb.ArchiveEpoch
	switch {
	case replica:
		deadline = time.Time{}
	case sb.ArchiveCk == "":
		deadline = m.retentionDeadline(sb.Key)
	}
	m.mu.Unlock()
	dto.Token, dto.VMName, dto.VsockSocket, dto.HibernateSnap, dto.PendingSnap, dto.ArchivePending, dto.ReplicaCk, dto.ReplicaPending = "", "", "", "", "", "", "", ""
	dto.ArchiveCk, dto.Deadline = ck, deadline
	dto.ReaderSafe = dto.ReaderSafe && !replica
	claim, err := json.Marshal(&dto)
	if err != nil {
		return 0, fmt.Errorf("encode claim: %w", err)
	}
	rec := archives.Record{
		ID: sb.ID, Tenant: sb.Tenant, TokenSHA256: tenants.TokenSHA256(sb.Token), Owner: m.nodeID,
		Checkpoint: ck, Deadline: deadline, Claim: claim,
	}
	if replace {
		return m.archives.Replace(ctx, rec, held)
	}
	return m.archives.Put(ctx, rec)
}

// refusedReplace settles a refused replace: a row this node still owns only had a stale epoch, anything else means the claim moved.
func (m *Manager) refusedReplace(ctx context.Context, sb *types.Sandbox) error {
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	switch {
	case err != nil:
		return err
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
	if err == nil && ok && rec.Owner != m.nodeID {
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
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
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

func (m *Manager) reputArchive(ctx context.Context, sb *types.Sandbox, held int64) {
	if held == 0 {
		return
	}
	var epoch int64
	var err error
	if sb.ArchiveCk != "" {
		epoch, err = m.putArchive(ctx, sb, sb.ArchiveCk)
	} else {
		epoch, err = m.putReplica(ctx, sb, sb.ReplicaCk)
	}
	if err != nil {
		if !errors.Is(err, ErrVolumeCapture) {
			log.WithFunc("pool.reputArchive").Warnf(ctx, "re-put archive row %s: %v", sb.ID, err)
		}
		return
	}
	_, _ = m.commitIfLive(ctx, sb, func() { sb.ArchiveEpoch = epoch })
}

func (m *Manager) releaseRowed(ctx context.Context, id string, sb *types.Sandbox) error {
	sb.Transition.Lock()
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
	if !m.archiveRowed(sb) && !m.replicaRowed(sb) {
		return 0, nil
	}
	ctx = context.WithoutCancel(ctx)
	consumed, err := m.consumeRow(ctx, sb, sb.ArchiveEpoch, "consume archive row")
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
	if m.replicaHeld(sb) {
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
		if err := m.clearArchiveCk(ck); err != nil {
			logger.Warnf(ctx, "clear archive ck %s: %v", ck, err)
		}
	}
	if !m.forgetClaim(ctx, sb) {
		return nil
	}
	return func() {
		if vm != "" {
			m.destroy(ctx, vm)
		}
		m.dropSnap(ctx, snap)
		logger.Infof(ctx, "sandbox %s moved to another node", sb.ID)
	}
}

func (m *Manager) forgetClaim(ctx context.Context, sb *types.Sandbox) bool {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		return false
	}
	m.dropClaimLocked(sb)
	js := m.store.del(sb.ID)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
	}
	return true
}

func (m *Manager) sweepOrphanArchives(ctx context.Context) {
	if !m.orphanSweep.CompareAndSwap(false, true) {
		return
	}
	defer m.orphanSweep.Store(false)
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
		if rec.Consumed || !rec.Deadline.IsZero() && now.After(rec.Deadline) {
			m.purgeArchiveRow(ctx, rec)
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
		m.reputArchive(ctx, sb, epoch)
	case refs:
	case rec.Consumed:
		m.purgeArchiveRow(ctx, rec)
	default:
		m.dropStaleRow(ctx, sb, rec)
	}
}

func (m *Manager) dropStaleRow(ctx context.Context, sb *types.Sandbox, rec archives.Record) {
	if !m.archiveCkPinned(rec.Checkpoint) {
		m.abandonArchive(ctx, sb, rec.Epoch, rec.Checkpoint)
	}
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
		m.recordUsage(ctx, usageEvent{Event: "archive_delete", ID: rec.ID, Reference: rec.Checkpoint, Tenant: rec.Tenant})
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
			moved, err := m.settleIntent(ctx, id, sb.ArchivePending, rec, ok)
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
				if err := m.clearArchiveCk(sb.ArchiveCk); err != nil {
					logger.Warnf(ctx, "clear archive ck %s: %v", sb.ArchiveCk, err)
				}
			}
			delete(claims, id)
			logger.Infof(ctx, "dropped sandbox %s: another node holds it", id)
		}
	}
	return true
}

func (m *Manager) settleIntent(ctx context.Context, id, pending string, rec archives.Record, ok bool) (bool, error) {
	if ok && rec.Owner == m.nodeID {
		err := m.archives.Delete(ctx, id, rec.Epoch)
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, archives.ErrLost) {
			return false, err
		}
		if rec, ok, err = m.archives.Get(ctx, id); err != nil {
			return false, err
		}
		if ok && rec.Owner == m.nodeID {
			return false, fmt.Errorf("archive row %s moved under its settle", id)
		}
	}
	switch {
	case ok:
		return true, nil
	case pending == "":
		return false, nil
	}
	_, err := m.ckpts.ReadMeta(ctx, pending)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}
	return false, err
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
	moved, err := m.settleIntent(ctx, sb.ID, pending, rec, ok)
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

func (m *Manager) expiryDropsRow(sb *types.Sandbox) bool {
	return m.archiveRowed(sb) || m.archives != nil && sb.HasReplica() && !m.archivesAtDeadline(sb)
}
