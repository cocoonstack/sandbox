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
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// OwnerAlive reports whether the mesh counts node as a live member.
type OwnerAlive func(node string) bool

// WithArchiveOwners wires the mesh view that archive adoption and the departed-owner sweep consult.
func (m *Manager) WithArchiveOwners(alive OwnerAlive) {
	m.ownerAlive.Store(&alive)
}

// AdoptArchived takes over an archived claim whose owner left the mesh and reports whether this node owns it now.
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
	sum := config.TokenSHA256(token)
	if rec.Consumed || subtle.ConstantTimeCompare([]byte(sum), []byte(rec.TokenSHA256)) != 1 || (!rec.Deadline.IsZero() && time.Now().After(rec.Deadline)) ||
		(rec.Owner != m.nodeID && !m.ownerGone(rec.Owner)) {
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
	if cur, dup := m.claimed[id]; dup {
		m.mu.Unlock()
		return m.adoptHeld(ctx, cur, epoch), nil
	}
	m.claimed[id] = sb
	m.tenantDelta(sb.Tenant, 1)
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
		m.setArchiveEpoch(ctx, cur, epoch)
		return true
	}
	if rec, ok, err := m.archives.Get(ctx, cur.ID); err == nil && ok && rec.Owner == m.nodeID && !rec.Consumed {
		if consumed, err := m.archives.Consume(ctx, cur.ID, rec.Epoch); err == nil {
			m.dropTombstone(ctx, cur.ID, consumed)
		}
	}
	return false
}

func (m *Manager) putArchive(ctx context.Context, sb *types.Sandbox, ck string) (int64, error) {
	if m.archives == nil {
		return 0, nil
	}
	m.mu.Lock()
	dto, deadline := dtoOf(sb), sb.Deadline
	if sb.ArchiveCk == "" {
		deadline = m.retentionDeadline(sb.Key)
	}
	m.mu.Unlock()
	dto.Token, dto.VMName, dto.VsockSocket, dto.HibernateSnap, dto.PendingSnap = "", "", "", "", ""
	dto.ArchiveCk, dto.Deadline = ck, deadline
	claim, err := json.Marshal(&dto)
	if err != nil {
		return 0, fmt.Errorf("encode claim: %w", err)
	}
	return m.archives.Put(ctx, archives.Record{
		ID: sb.ID, Tenant: sb.Tenant, TokenSHA256: config.TokenSHA256(sb.Token), Owner: m.nodeID,
		Checkpoint: ck, Deadline: deadline, Claim: claim,
	})
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

func (m *Manager) abandonFailedPut(ctx context.Context, id, ck string, putErr error) {
	if errors.Is(putErr, archives.ErrLost) {
		m.deleteOrphanArchiveCk(ctx, ck)
		return
	}
	rec, ok, err := m.archives.Get(ctx, id)
	switch {
	case err != nil:
		log.WithFunc("pool.abandonFailedPut").Warnf(ctx, "archive %s: keeping ck %s, its row is unknown: %v", id, ck, err)
	case ok && rec.Owner == m.nodeID && rec.Checkpoint == ck:
		m.abandonArchive(ctx, id, rec.Epoch, ck)
	default:
		m.deleteOrphanArchiveCk(ctx, ck)
	}
}

func (m *Manager) abandonArchive(ctx context.Context, id string, epoch int64, ck string) {
	if epoch == 0 {
		m.deleteOrphanArchiveCk(ctx, ck)
		return
	}
	if err := m.dropArchive(ctx, id, epoch, ck); err != nil {
		log.WithFunc("pool.abandonArchive").Warnf(ctx, "drop archive row %s, keeping ck %s: %v", id, ck, err)
	}
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
	m.setArchiveEpoch(ctx, sb, epoch)
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
	m.setArchiveEpoch(ctx, sb, epoch)
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

func (m *Manager) setArchiveEpoch(ctx context.Context, sb *types.Sandbox, epoch int64) {
	m.mu.Lock()
	var js claimSnapshot
	if m.claimed[sb.ID] == sb {
		sb.ArchiveEpoch = epoch
		js = m.store.set(sb)
	}
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
}

func (m *Manager) fenceWake(ctx context.Context, sb *types.Sandbox, epoch int64) (int64, error) {
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
		return 0, fmt.Errorf("wake %s: %w", sb.ID, err)
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

func (m *Manager) reputArchive(ctx context.Context, sb *types.Sandbox, epoch int64) {
	if epoch == 0 {
		return
	}
	epoch, err := m.putArchive(ctx, sb, sb.ArchiveCk)
	if err != nil {
		log.WithFunc("pool.reputArchive").Warnf(ctx, "re-put archive row %s: %v", sb.ID, err)
		return
	}
	m.setArchiveEpoch(ctx, sb, epoch)
}

func (m *Manager) consumeArchive(ctx context.Context, sb *types.Sandbox) (int64, error) {
	if !m.archiveRowed(sb) {
		return 0, nil
	}
	ctx = context.WithoutCancel(ctx)
	consumed, err := m.archives.Consume(ctx, sb.ID, sb.ArchiveEpoch)
	if errors.Is(err, archives.ErrLost) {
		consumed, err = m.settleConsume(ctx, sb)
	}
	switch {
	case errors.Is(err, ErrUnknownSandbox):
		return 0, err
	case err != nil:
		return 0, fmt.Errorf("consume archive row %s: %w", sb.ID, err)
	}
	m.setArchiveEpoch(ctx, sb, 0)
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

// unrowExpired drops an expired archive's row, so the next reap purges it as a row-less archive.
func (m *Manager) unrowExpired(ctx context.Context, sb *types.Sandbox) {
	defer m.untrack(m.archiving, sb.ID)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if _, err := m.consumeArchive(ctx, sb); err != nil && !errors.Is(err, ErrUnknownSandbox) {
		log.WithFunc("pool.unrowExpired").Warnf(ctx, "purge %s: %v", sb.ID, err)
	}
}

// dropAdopted forgets an archived claim another node owns now; its checkpoint is the new owner's.
func (m *Manager) dropAdopted(ctx context.Context, sb *types.Sandbox) {
	logger := log.WithFunc("pool.dropAdopted")
	if err := m.clearArchiveCk(sb.ArchiveCk); err != nil {
		logger.Warnf(ctx, "clear archive ck %s: %v", sb.ArchiveCk, err)
	}
	if m.forgetClaim(ctx, sb) {
		logger.Infof(ctx, "archived sandbox %s moved to another node", sb.ID)
	}
}

func (m *Manager) forgetClaim(ctx context.Context, sb *types.Sandbox) bool {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		return false
	}
	delete(m.claimed, sb.ID)
	m.tenantDelta(sb.Tenant, -1)
	js := m.store.del(sb.ID)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
	return true
}

// sweepOrphanArchives runs on every node; the epoch compare-and-set lets one win each row.
func (m *Manager) sweepOrphanArchives(ctx context.Context) {
	if !m.orphanSweep.CompareAndSwap(false, true) {
		return
	}
	defer m.orphanSweep.Store(false)
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
		case m.ownerGone(rec.Owner) && (!rec.Consumed || now.Sub(rec.UpdatedAt) > tombstoneGrace):
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
	held, refs, epoch := m.claimed[rec.ID] == sb, sb.ArchiveCk == rec.Checkpoint, sb.ArchiveEpoch
	m.mu.Unlock()
	switch {
	case !held:
	case refs && rec.Consumed && epoch != 0:
		m.reputArchive(ctx, sb, epoch)
	case refs:
	case rec.Consumed:
		m.purgeArchiveRow(ctx, rec)
	default:
		m.dropStaleRow(ctx, rec)
	}
}

func (m *Manager) dropStaleRow(ctx context.Context, rec archives.Record) {
	if err := m.archives.Delete(ctx, rec.ID, rec.Epoch); err != nil {
		if !errors.Is(err, archives.ErrLost) {
			log.WithFunc("pool.dropStaleRow").Warnf(ctx, "drop stale archive row %s: %v", rec.ID, err)
		}
		return
	}
	if !m.archiveCkPinned(rec.Checkpoint) {
		m.deleteOrphanArchiveCk(ctx, rec.Checkpoint)
	}
}

func (m *Manager) purgeArchiveRow(ctx context.Context, rec archives.Record) {
	logger := log.WithFunc("pool.purgeArchiveRow")
	if err := m.deleteArchiveCk(ctx, rec.Checkpoint); err != nil {
		logger.Warnf(ctx, "purge archive %s: delete ck %s: %v", rec.ID, rec.Checkpoint, err)
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

func (m *Manager) dropArchive(ctx context.Context, id string, epoch int64, ck string) error {
	if err := m.archives.Delete(ctx, id, epoch); err != nil {
		return err
	}
	m.deleteOrphanArchiveCk(ctx, ck)
	return nil
}

// fenceArchived settles loaded claims against their rows and returns the rows; false when the registry did not answer.
func (m *Manager) fenceArchived(ctx context.Context, claims map[string]*types.Sandbox) (map[string]archives.Record, bool) {
	if m.archives == nil {
		return nil, true
	}
	logger := log.WithFunc("pool.fenceArchived")
	rows, err := m.archives.Rows(ctx, slices.Collect(maps.Keys(claims)))
	if err != nil {
		logger.Warnf(ctx, "archive fencing and orphan reclaim skipped: %v", err)
		return nil, false
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
		case ok && !rec.Consumed:
			drop = m.settleInterruptedArchive(ctx, rows, rec)
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
	return rows, true
}

func (m *Manager) settleInterruptedArchive(ctx context.Context, rows map[string]archives.Record, rec archives.Record) bool {
	logger := log.WithFunc("pool.settleInterruptedArchive")
	for range 2 {
		err := m.archives.Delete(ctx, rec.ID, rec.Epoch)
		if err == nil {
			delete(rows, rec.ID)
			return false
		}
		if !errors.Is(err, archives.ErrLost) {
			logger.Warnf(ctx, "drop interrupted archive row %s: %v", rec.ID, err)
			return false
		}
		cur, ok, err := m.archives.Get(ctx, rec.ID)
		switch {
		case err != nil:
			logger.Warnf(ctx, "read interrupted archive row %s: %v", rec.ID, err)
			return false
		case !ok:
			delete(rows, rec.ID)
			return false
		case cur.Owner != m.nodeID:
			return true
		case cur.Consumed:
			rows[rec.ID] = cur
			return false
		}
		rec = cur
	}
	return false
}

func (m *Manager) archiveRowed(sb *types.Sandbox) bool {
	return m.archives != nil && sb.ArchiveCk != "" && sb.ArchiveEpoch != 0
}

func (m *Manager) ownerGone(node string) bool {
	alive := m.ownerAlive.Load()
	return alive != nil && !(*alive)(node)
}
