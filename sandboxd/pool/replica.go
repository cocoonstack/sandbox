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
	if m.archives == nil || m.fenced.Load() || !m.replicating.CompareAndSwap(false, true) {
		return
	}
	now, v := time.Now(), m.view.Load()
	var due, retire []*types.Sandbox
	var withVolumes []string
	next := make(map[string]replicaState, len(m.replicas))
	m.mu.Lock()
	for id, sb := range m.claimed {
		every, st := v.poolReplicate[sb.Key], m.replicas[id]
		switch {
		case every <= 0:
			if sb.ReplicaCk != "" {
				retire = append(retire, sb)
			}
		case !m.replicable(sb):
		case m.hasVolumeHolds(sb):
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

// replicable reports whether sb is a running claim; callers hold m.mu.
func (m *Manager) replicable(sb *types.Sandbox) bool {
	_, archiving := m.archiving[sb.ID]
	return !archiving && sb.HibernateSnap == "" && sb.PendingSnap == "" && sb.ArchiveCk == "" && sb.ArchivePending == "" && sb.Failed == "" && !m.tenantGone(sb.Tenant)
}

func (m *Manager) replicate(ctx context.Context, sb *types.Sandbox) error {
	if err := m.diskErr(); err != nil {
		return fmt.Errorf("replicate %s: %w", sb.ID, err)
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
	ok := m.claimed[sb.ID] == sb && sb.ArchiveCk == "" && sb.ArchivePending == ""
	m.mu.Unlock()
	if !ok {
		m.deleteOrphanArchiveCk(ctx, ckID)
		return errWokeMeanwhile
	}
	epoch, err := m.putArchive(ctx, sb, ckID)
	if err != nil {
		m.deleteOrphanArchiveCk(ctx, ckID)
		if errors.Is(err, archives.ErrLost) {
			m.dropAdopted(ctx, sb)
		}
		return fmt.Errorf("replicate %s: registry: %w", sb.ID, err)
	}
	var prev string
	live, _ := m.commitIfLive(ctx, sb, func() {
		prev, sb.ReplicaCk, sb.ArchiveEpoch = sb.ReplicaCk, ckID, epoch
		sb.SyncReplicated()
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

func (m *Manager) retireReplica(ctx context.Context, sb *types.Sandbox) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err := m.dropReplica(ctx, sb); err != nil && !benignSweepErr(err) {
		log.WithFunc("pool.retireReplica").Warnf(ctx, "stop replicating %s: %v", sb.ID, err)
	}
}

// dropReplica ends sb's replication; the caller holds sb.Transition.
func (m *Manager) dropReplica(ctx context.Context, sb *types.Sandbox) error {
	consumed, err := m.consumeArchive(ctx, sb)
	if err != nil {
		return err
	}
	var ck string
	live, _ := m.commitIfLive(ctx, sb, func() {
		ck, sb.ReplicaCk = sb.ReplicaCk, ""
		sb.SyncReplicated()
	})
	if !live {
		return ErrUnknownSandbox
	}
	m.dropTombstone(ctx, sb.ID, consumed)
	m.deleteOrphanArchiveCk(ctx, ck)
	return nil
}

func (m *Manager) replicaRowed(sb *types.Sandbox) bool {
	return m.archives != nil && sb.ReplicaCk != "" && sb.ArchiveEpoch != 0
}

func (m *Manager) replicaOf(sb *types.Sandbox) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.ReplicaCk
}

func (m *Manager) replicaConfirmed(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.ArchiveEpoch != 0
}
