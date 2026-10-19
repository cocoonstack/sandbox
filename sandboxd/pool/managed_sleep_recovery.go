package pool

import (
	"context"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (m *Manager) managedSleepSources() map[string]bool {
	sources := map[string]bool{}
	if m.managed == nil {
		return sources
	}
	m.managed.mu.Lock()
	defer m.managed.mu.Unlock()
	for _, h := range m.managed.data.Holds {
		if h.Sleep != nil {
			sources[h.Sleep.Snapshot] = true
		}
	}
	return sources
}

func (m *Manager) releaseManagedSleepLocked(ctx context.Context, sb *types.Sandbox, expiredAt time.Time) error {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	if !expiredAt.IsZero() && (sb.Deadline.IsZero() || !expiredAt.After(sb.Deadline)) {
		m.mu.Unlock()
		return nil
	}
	if err := m.retireManaged(sb.ID); err != nil {
		m.mu.Unlock()
		return err
	}
	m.dropClaimLocked(sb)
	js := m.store.del(sb.ID)
	m.mu.Unlock()
	if err := m.store.commitDurable(js); err != nil {
		m.mu.Lock()
		m.addClaimLocked(sb)
		m.store.set(sb)
		m.mu.Unlock()
		m.closeManagedStreams(sb)
		return err
	}
	ctx = context.WithoutCancel(ctx)
	h, _ := m.managedHold(sb.ID)
	td := volumeTeardown{managedID: sb.ID}
	if h.Sleep == nil {
		td = m.quiesceVolumes(ctx, sb)
	}
	removed := m.removeOrRetry(ctx, sb.VMName, sb.ID, td)
	m.out.Disarm(sb.ID, removed)
	if !removed {
		return sleepRecoveryError()
	}
	m.counters.releases.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "release", ID: sb.ID, VMName: sb.VMName})
	return nil
}

func (m *Manager) removeManagedSleep(ctx context.Context, h volumeHold) bool {
	current, present, err := m.observeManagedRemoval(ctx, h)
	if err != nil {
		return false
	}
	h = current
	if present {
		_ = m.eng.Remove(ctx, h.VMName)
	}

	if !m.confirmGone(ctx, h.VMName) || !m.managed.processGone(h.Process) || !m.managed.processGone(h.Sleep.BeforeRestore) {
		return false
	}
	_ = m.eng.SnapshotRemove(ctx, h.Sleep.Snapshot)
	_, exists, inspectErr := m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if inspectErr != nil || exists {
		return false
	}

	return true
}

func (m *Manager) observeManagedRemoval(ctx context.Context, h volumeHold) (volumeHold, bool, error) {
	rec, present, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil || !present {
		return h, present, err
	}
	if rec.Config.Name != h.VMName || rec.ID != h.Sleep.VMID {
		return h, true, sleepRecoveryError()
	}
	if rec.State != vmStateRunning {
		if !m.managed.processGone(h.Process) {
			return h, true, sleepRecoveryError()
		}
		return h, true, nil
	}
	restored := h.Sleep.Phase == sleepWaking || h.Sleep.Phase == sleepResuming
	if _, err = m.sleepVM(ctx, h, restored); err != nil {
		return h, true, err
	}
	p, err := m.managed.process(rec.PID)
	if err != nil || p == h.Process {
		return h, true, err
	}
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	h = r.data.Holds[h.SandboxID]
	h.Process = p
	next := *h.Sleep
	next.Restored = p
	h.Sleep = &next
	r.data.Holds[h.SandboxID] = h
	return h, true, r.commit()
}

func (m *Manager) retryManagedSleepRemoval(ctx context.Context, name string, pending pendingRemoval) bool {
	h, held := m.managedHold(pending.volumes.managedID)
	if !held || h.Sleep == nil {
		return false
	}
	m.mu.Lock()
	sb := m.claimed[h.SandboxID]
	m.mu.Unlock()
	if sb != nil {
		sb.Transition.Lock()
		defer sb.Transition.Unlock()
	}
	if m.removeVM(ctx, name) {
		m.finishRemoval(ctx, pending)
		if _, held = m.managedHold(h.SandboxID); !held {
			return true
		}
	}
	m.queueRemoval(name, pending.sandboxID, pending.tap, pending.volumes, time.Time{})
	return true
}
