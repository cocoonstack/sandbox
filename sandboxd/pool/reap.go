package pool

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	reapDestroy reapAction = iota
	reapArchive
	reapPurge
	reapPause
)

type reapAction int

func (m *Manager) reapOnce(ctx context.Context) {
	now := time.Now()
	type victim struct {
		action                       reapAction
		id, vmName, snap, ck, tenant string
		deadline                     time.Time
		sb                           *types.Sandbox
		holds                        []types.Volume
	}
	m.mu.Lock()
	var expired []victim
	var dropped, purges []string
	var unrows []*types.Sandbox
	for id, sb := range m.claimed {
		// A zero deadline means no expiry (an archived claim kept forever).
		if sb.Deadline.IsZero() || !now.After(sb.Deadline) || m.midMoveLocked(sb) {
			continue
		}
		switch {
		case m.expiryDropsRow(sb):
			if _, busy := m.archiving[id]; !busy {
				m.archiving[id] = struct{}{}
				unrows = append(unrows, sb)
			}
		case sb.ArchiveCk != "":
			expired = append(expired, victim{action: reapPurge, id: id, ck: sb.ArchiveCk, tenant: sb.Tenant, deadline: sb.Deadline, sb: sb})
			purges = append(purges, sb.ArchiveCk)
		case m.archivesAtDeadline(sb):
			// Archive instead of destroy, kept in m.claimed for archive() to move
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
			expired = append(expired, victim{action: reapDestroy, id: id, vmName: sb.VMName, snap: sb.HibernateSnap, tenant: sb.Tenant, deadline: sb.Deadline, sb: sb, holds: m.cancelVolumeRun(sb)})
			m.dropClaimLocked(sb)
			dropped = append(dropped, id)
		}
	}
	if len(dropped) > 0 {
		m.store.del(dropped...)
	}
	m.mu.Unlock()
	if len(unrows) > 0 {
		m.runBounded(ctx, len(unrows), func(ctx context.Context, i int) { m.unrowExpired(ctx, unrows[i]) })
	}
	if len(expired) == 0 {
		return
	}

	logger := log.WithFunc("pool.reapOnce")
	failed := m.ckMarks.markAll(purges)
	expired = slices.DeleteFunc(expired, func(v victim) bool {
		markErr, bad := failed[v.ck]
		if bad {
			logger.Errorf(ctx, markErr, "mark archive ck %s; keeping %s", v.ck, v.id)
		}
		return bad
	})
	m.mu.Lock()
	keep := expired[:0]
	var purgeCks, purged []string
	for _, v := range expired {
		if v.action == reapPurge {
			if m.claimed[v.id] != v.sb || v.sb.ArchiveCk != v.ck {
				continue
			}
			m.pendingCks[v.ck] = struct{}{}
			m.dropClaimLocked(v.sb)
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
				m.addClaimLocked(v.sb)
				m.restoreVolumeHolds(v.sb, v.holds)
				delete(m.pendingCks, v.ck)
				restored = append(restored, v.sb)
			}
		}
		rb := m.store.set(restored...)
		m.mu.Unlock()
		for _, ck := range purgeCks {
			if clearErr := m.ckMarks.clear(ck); clearErr != nil {
				logger.Warnf(ctx, "clear archive ck %s: %v", ck, clearErr)
			}
		}
		m.store.recommit(ctx, rb)
		logger.Error(ctx, saveErr, "persist reap; rolled back")
		return
	}
	// Engine teardown can take minutes, so fan out bounded and never stall the ticker loop
	m.runBounded(ctx, len(expired), func(ctx context.Context, i int) {
		v := expired[i]
		switch v.action {
		case reapPurge:
			m.out.Disarm(v.id, true)
			m.purgeArchiveCk(ctx, v.id, v.ck, v.tenant)
			m.untrack(m.pendingCks, v.ck)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usage.Event{Event: reapEvent, ID: v.id, Tenant: v.tenant, Deadline: v.deadline})
			logger.Infof(ctx, "purged archived sandbox %s", v.id)
		case reapArchive:
			logSweepResult(ctx, logger, m.archive(ctx, v.sb), "archived expired sandbox "+v.id, "archive expired sandbox "+v.id)
		case reapPause:
			logSweepResult(ctx, logger, m.pauseArchive(ctx, v.sb), "archived expired sandbox "+v.id, "archive expired sandbox "+v.id)
		default:
			td := m.quiesceVolumes(ctx, v.sb)
			td.holds = v.holds
			m.out.Disarm(v.id, m.removeOrRetry(ctx, v.vmName, v.id, td))
			m.dropSnap(ctx, v.snap)
			m.counters.reaps.Add(1)
			m.recordUsage(ctx, usage.Event{Event: reapEvent, ID: v.id, VMName: v.vmName, Tenant: v.tenant, Deadline: v.deadline})
			logger.Infof(ctx, "reaped expired sandbox %s (%s)", v.id, v.vmName)
		}
	})
}

func (m *Manager) archivesAtDeadline(sb *types.Sandbox) bool {
	return sb.Failed == "" && !m.tenantGone(sb.Tenant) && (sb.OnExpire == types.ExpireArchive || sb.HibernateSnap != "" && m.archiveAfterFor(sb.Key) > 0)
}

func (m *Manager) pauseArchive(ctx context.Context, sb *types.Sandbox) error {
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
	if _, err := m.finishArchiveCkDelete(ctx, ck); err != nil {
		logger.Warnf(ctx, "delete archive ck %s: %v", ck, err)
	}
	m.counters.archiveDeletes.Add(1)
	m.recordUsage(ctx, usage.Event{Event: "archive_delete", ID: id, Reference: ck, Tenant: tenant})
}
