package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type replicaState struct {
	tried      time.Time
	skipLogged bool
}

func (m *Manager) replicateOnce(ctx context.Context) {
	if m.archives == nil || m.isFenced() || !m.replicating.CompareAndSwap(false, true) {
		return
	}
	now, v := time.Now(), m.view.Load()
	var due, retire []*types.Sandbox
	var withVolumes []string
	next := make(map[string]replicaState, len(m.replicas))
	m.mu.Lock()
	for id, sb := range m.claimed {
		every, st := v.poolReplicate[sb.PolicyKey()], m.replicas[id]
		switch {
		case every <= 0:
			if sb.HasReplica() {
				retire = append(retire, sb)
			}
		case !m.replicable(sb):
		case m.hasVolumeHolds(sb):
			if sb.HasReplica() {
				retire = append(retire, sb)
			}
			if !st.skipLogged {
				withVolumes = append(withVolumes, id)
			}
			next[id] = replicaState{skipLogged: true}
		case st.tried.IsZero():
			next[id] = replicaState{tried: now}
		case now.Sub(st.tried) < every:
			next[id] = st
		default:
			next[id] = replicaState{tried: now}
			due = append(due, sb)
		}
	}
	m.mu.Unlock()
	m.replicas = next
	logger := log.WithFunc("pool.replicateOnce")
	for _, id := range withVolumes {
		logger.Infof(ctx, "sandbox %s holds volumes and is not replicated", id)
	}
	if len(due)+len(retire) == 0 {
		m.replicating.Store(false)
		return
	}
	go func() {
		defer m.replicating.Store(false)
		m.runBounded(ctx, len(due)+len(retire), func(ctx context.Context, i int) {
			if i >= len(due) {
				m.retireReplica(ctx, retire[i-len(due)])
				return
			}
			sb := due[i]
			if err := m.replicate(ctx, sb); err != nil && !benignSweepErr(err) {
				logger.Warnf(ctx, "replicate %s, retrying next interval: %v", sb.ID, err)
			}
		}).Wait()
	}()
}

// replicable excludes a claim that is paused, archived or archiving, failed, or whose tenant was removed; callers hold m.mu.
func (m *Manager) replicable(sb *types.Sandbox) bool {
	_, archiving := m.archiving[sb.ID]
	return !archiving && sb.HibernateSnap == "" && sb.PendingSnap == "" && sb.ArchiveCk == "" && sb.ArchivePending == "" && sb.Failed == "" && !m.tenantGone(sb.Tenant)
}

func (m *Manager) replicate(ctx context.Context, sb *types.Sandbox) error {
	if err := m.diskErr(); err != nil {
		return fmt.Errorf("replicate %s: %w", sb.ID, err)
	}
	sb.Transition.Lock()
	settleErr := m.settleReplicaIntent(ctx, sb)
	sb.Transition.Unlock()
	if settleErr != nil {
		return fmt.Errorf("replicate %s: settle intent: %w", sb.ID, settleErr)
	}
	if err := m.checkArchiveRow(ctx, sb.ID); err != nil {
		if errors.Is(err, archives.ErrLost) {
			sb.Transition.Lock()
			m.dropAdopted(ctx, sb)
			sb.Transition.Unlock()
		}
		return fmt.Errorf("replicate %s: registry: %w", sb.ID, err)
	}
	ckID := store.CheckpointID(randHex(8))
	m.mu.Lock()
	m.pendingCks[ckID] = struct{}{}
	m.mu.Unlock()
	defer m.untrack(m.pendingCks, ckID)
	if _, _, err := m.publishCheckpoint(ctx, sb, ckID, "replica", sb.Tenant, true); err != nil {
		return err
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	var intent claimSnapshot
	var refusal error
	switch {
	case m.claimed[sb.ID] != sb || sb.ArchiveCk != "" || sb.ArchivePending != "":
		refusal = errWokeMeanwhile
	case m.hasVolumeHolds(sb):
		refusal = ErrVolumeCapture
	default:
		sb.ReplicaPending = ckID
		intent = m.store.set(sb)
	}
	m.mu.Unlock()
	if refusal != nil {
		m.deleteOrphanArchiveCk(ctx, ckID)
		return refusal
	}
	if err := m.store.commit(intent); err != nil {
		_ = m.clearReplicaIntent(ctx, sb, ckID)
		return fmt.Errorf("replicate %s: persist intent: %w", sb.ID, err)
	}
	epoch, err := m.replaceReplica(ctx, sb, ckID)
	switch {
	case errors.Is(err, archives.ErrLost):
		_ = m.clearReplicaIntent(ctx, sb, ckID)
		if settleErr := m.refusedReplace(ctx, sb); settleErr != nil && !errors.Is(settleErr, ErrUnknownSandbox) {
			return fmt.Errorf("replicate %s: settle refused write: %w", sb.ID, settleErr)
		}
		return fmt.Errorf("replicate %s: registry: %w", sb.ID, err)
	case err != nil:
		return fmt.Errorf("replicate %s: registry: %w", sb.ID, err)
	}
	var prev string
	live, _ := m.commitIfLive(ctx, sb, func() {
		prev, sb.ReplicaCk, sb.ReplicaPending, sb.ArchiveEpoch = sb.ReplicaCk, ckID, "", epoch
	})
	if !live {
		m.abandonArchive(ctx, sb, epoch, ckID)
		return errWokeMeanwhile
	}
	if prev != "" {
		m.deleteOrphanArchiveCk(ctx, prev)
	}
	return nil
}

// settleReplicaIntent resolves a round whose registry write may or may not have landed; the caller holds sb.Transition.
func (m *Manager) settleReplicaIntent(ctx context.Context, sb *types.Sandbox) error {
	m.mu.Lock()
	pending := sb.ReplicaPending
	m.mu.Unlock()
	if pending == "" || m.archives == nil {
		return nil
	}
	rec, ok, err := m.archives.Get(ctx, sb.ID)
	if err != nil {
		return err
	}
	gone := false
	if !ok {
		if gone, err = m.replicaGone(ctx, sb); err != nil {
			return err
		}
	}
	switch {
	case gone:
		m.dropAdopted(ctx, sb)
		return ErrUnknownSandbox
	case ok && rec.Owner != m.nodeID:
		if rec.Checkpoint != pending {
			m.deleteOrphanArchiveCk(ctx, pending)
		}
		m.dropAdopted(ctx, sb)
		return ErrUnknownSandbox
	case ok && !rec.Consumed && rec.Checkpoint == pending:
		var prev string
		live, commitErr := m.commitIfLive(ctx, sb, func() {
			prev, sb.ReplicaCk, sb.ReplicaPending, sb.ArchiveEpoch = sb.ReplicaCk, pending, "", rec.Epoch
		})
		if !live {
			return ErrUnknownSandbox
		}
		if prev != "" {
			m.deleteOrphanArchiveCk(ctx, prev)
		}
		if commitErr != nil {
			return fmt.Errorf("persist settled replica: %w", commitErr)
		}
		return nil
	}
	return m.clearReplicaIntent(ctx, sb, pending)
}

// replicaGone reports whether the absence of sb's row means another node adopted the claim and finished with it.
func (m *Manager) replicaGone(ctx context.Context, sb *types.Sandbox) (bool, error) {
	m.mu.Lock()
	confirmed, pending := sb.ArchiveEpoch != 0, sb.ReplicaPending
	m.mu.Unlock()
	if confirmed {
		return true, nil
	}
	if pending == "" {
		return false, nil
	}
	_, err := m.ckpts.ReadMeta(ctx, pending)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}
	return false, err
}

func (m *Manager) clearReplicaIntent(ctx context.Context, sb *types.Sandbox, ck string) error {
	live, err := m.commitIfLive(ctx, sb, func() {
		if sb.ReplicaPending == ck {
			sb.ReplicaPending = ""
		}
	})
	m.deleteOrphanArchiveCk(ctx, ck)
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		return fmt.Errorf("persist replica intent clear: %w", err)
	}
	return nil
}

func (m *Manager) retireReplica(ctx context.Context, sb *types.Sandbox) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err := m.dropReplica(ctx, sb); err != nil && !benignSweepErr(err) {
		log.WithFunc("pool.retireReplica").Warnf(ctx, "stop replicating %s: %v", sb.ID, err)
	}
}

// dropReplica ends sb's replication; the caller holds sb.Transition.
func (m *Manager) dropReplica(ctx context.Context, sb *types.Sandbox) error {
	if err := m.settleReplicaIntent(ctx, sb); err != nil {
		return err
	}
	if m.replicaOf(sb) == "" {
		return nil
	}
	consumed, err := m.consumeArchive(ctx, sb)
	if err != nil {
		return err
	}
	var ck string
	live, err := m.commitIfLive(ctx, sb, func() {
		ck, sb.ReplicaCk = sb.ReplicaCk, ""
	})
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		m.deleteOrphanArchiveCk(ctx, ck)
		return fmt.Errorf("persist replica retirement: %w", err)
	}
	m.dropTombstone(ctx, sb.ID, consumed)
	m.deleteOrphanArchiveCk(ctx, ck)
	return nil
}

func (m *Manager) replicaRowed(sb *types.Sandbox) bool {
	return m.archives != nil && sb.ReplicaCk != "" && sb.ArchiveEpoch != 0
}

func (m *Manager) replicaHeld(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.HasReplica()
}

func (m *Manager) replicaOf(sb *types.Sandbox) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.ReplicaCk
}
