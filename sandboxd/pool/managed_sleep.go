package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type managedSleepTarget bool

const (
	managedRunning  managedSleepTarget = false
	managedSleeping managedSleepTarget = true
)

func sleepRecoveryError() error {
	return volumeError(409, "managed_sleep_recovery_required", "managed sleep ownership or VM state is uncertain; retain the backing and retry or release the sandbox")
}

func (m *Manager) managedSleepPreflight(ctx context.Context, sb *types.Sandbox) (volumeHold, types.VMRecord, error) {
	h, ok := m.managedHold(sb.ID)
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	busy := m.volumeRuns[sb] != nil
	_, outbound := m.view.Load().out.Resolve(sb, m.claimPooledLocked)
	m.mu.Unlock()
	if m.growthOwnsHold(sb.ID) {
		return h, types.VMRecord{}, growBusy()
	}
	if !ok || m.managedSleepHook == nil || h.State != holdClaimed || !sleepMatchesClaim(h, sb) || h.Refs[0].Volume.Mount == "" || !h.Refs[0].Volume.RW() || h.Refs[0].ID == "" || busy || outbound {
		return h, types.VMRecord{}, ErrVolumeCapture
	}
	if !live {
		return h, types.VMRecord{}, ErrUnknownSandbox
	}
	if err := m.diskErr(); err != nil {
		return h, types.VMRecord{}, err
	}
	if err := m.verifySleepBacking(h); err != nil {
		return h, types.VMRecord{}, err
	}
	if err := m.eng.CheckLiveVolumeSupport(ctx); err != nil {
		return h, types.VMRecord{}, err
	}
	rec, present, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return h, rec, err
	}
	if !present || rec.Config.Name != h.VMName || rec.ID == "" || rec.LastTransitionReason == "" || rec.TransitionGeneration == 0 || rec.TransitionGeneration > ^uint64(0)-2 || rec.State != vmStateRunning || !exactSleepDisk(rec, h.Refs[0]) {
		return h, rec, sleepRecoveryError()
	}
	p, err := m.managed.process(rec.PID)
	if err != nil || p != h.Process {
		return h, rec, sleepRecoveryError()
	}
	if err := m.eng.VerifyMountedVolume(ctx, rec.VsockSocket, h.Refs[0].Volume.Name, h.Refs[0].Volume.Mount); err != nil {
		return h, rec, err
	}
	return h, rec, nil
}

func (m *Manager) driveManagedSleepLocked(ctx context.Context, sb *types.Sandbox, target managedSleepTarget) error {
	h, ok := m.managedHold(sb.ID)
	if m.growthOwnsHold(sb.ID) {
		return growBusy()
	}
	if !ok {
		return ErrVolumeCapture
	}
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	m.mu.Unlock()
	if !live {
		return ErrUnknownSandbox
	}
	if err := m.ManagedVolumeStatus(); err != nil {
		return err
	}
	if h.State != holdClaimed || !sleepMatchesClaim(h, sb) {
		return sleepRecoveryError()
	}
	if _, outbound := m.view.Load().out.Resolve(sb, m.claimPooled); outbound {
		return ErrVolumeCapture
	}
	ctx = context.WithoutCancel(ctx)
	if h.Sleep == nil {
		if err := m.beginManagedSleep(ctx, sb); err != nil {
			return err
		}
	}

	for {
		h, _ = m.managedHold(sb.ID)
		if h.Sleep == nil {
			return nil
		}
		if err := m.verifySleepBacking(h); err != nil {
			return err
		}
		switch h.Sleep.Phase {
		case sleepPreparing:
			if err := m.setSleepPhase(h, sleepAborting); err != nil {
				return err
			}
		case sleepAborting:
			if err := m.resumeManagedSleep(ctx, sb, h, false); err != nil {
				return err
			}
			if target == managedRunning {
				return nil
			}
			return m.driveManagedSleepLocked(ctx, sb, target)
		case sleepCapturing:
			if err := m.captureManagedSleep(ctx, sb, h); err != nil {
				return err
			}
		case sleepSleeping:
			if err := m.verifySleepingVM(ctx, h); err != nil {
				return err
			}
			if err := m.projectManagedSleep(sb, h); err != nil {
				return err
			}
			if target == managedSleeping {
				return nil
			}
			if err := m.setSleepPhase(h, sleepWaking); err != nil {
				return err
			}
		case sleepWaking:
			if err := m.wakeManagedSleep(ctx, sb, h); err != nil {
				return err
			}
		case sleepResuming:
			if err := m.resumeManagedSleep(ctx, sb, h, true); err != nil {
				return err
			}
			if target == managedRunning {
				return nil
			}
			return m.driveManagedSleepLocked(ctx, sb, target)
		default:
			return sleepRecoveryError()
		}
	}
}

func (m *Manager) beginManagedSleep(ctx context.Context, sb *types.Sandbox) error {
	h, rec, err := m.managedSleepPreflight(ctx, sb)
	if err != nil {
		return err
	}
	if err = m.store.commitDurable(m.store.mark()); err != nil {
		return err
	}
	hook := *m.managedSleepHook
	hook.HookArgv = slices.Clone(hook.HookArgv)
	next := &managedSleep{Operation: randHex(16), Phase: sleepPreparing, Snapshot: hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3), Hook: hook, VMID: rec.ID, Generation: rec.TransitionGeneration, BeforeRestore: h.Process}
	if err = m.managed.commitManagedSleep(h, next); err != nil {
		return err
	}
	h.Sleep = next
	if err = m.projectManagedSleep(sb, h); err != nil {
		return err
	}
	m.closeManagedStreams(sb)
	m.out.Disarm(sb.ID, false)
	return m.prepareManagedSleep(ctx, sb, h)
}

func (m *Manager) setSleepPhase(h volumeHold, phase sleepPhase) error {
	next := *h.Sleep
	next.Phase = phase
	return m.managed.commitManagedSleep(h, &next)
}

func (m *Manager) prepareManagedSleep(ctx context.Context, sb *types.Sandbox, h volumeHold) error {
	rec, err := m.sleepVM(ctx, h, false)
	if err == nil {
		err = m.sleepHook(ctx, h, rec.VsockSocket, "prepare")
	}
	if err == nil {
		ref := h.Refs[0]
		err = m.eng.VerifyMountedVolume(ctx, rec.VsockSocket, ref.Volume.Name, ref.Volume.Mount)
		if err == nil {
			err = m.eng.DetachVolume(ctx, h.VMName, rec.VsockSocket, ref.Volume.Name, ref.Volume.Mount)
		}
	}
	if err == nil {
		rec, err = m.sleepVM(ctx, h, false)
		if err == nil && len(rec.AttachedDevices.Disks) != 0 {
			err = sleepRecoveryError()
		}
	}
	if err != nil {
		if persistErr := m.setSleepPhase(h, sleepAborting); persistErr != nil {
			return errors.Join(err, persistErr)
		}
		h, _ = m.managedHold(sb.ID)
		return errors.Join(err, m.resumeManagedSleep(ctx, sb, h, false))
	}
	return m.setSleepPhase(h, sleepCapturing)
}

func (m *Manager) captureManagedSleep(ctx context.Context, sb *types.Sandbox, h volumeHold) error {
	rec, found, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return err
	}
	if !found || rec.ID != h.Sleep.VMID || rec.Config.Name != h.VMName {
		return sleepRecoveryError()
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if err != nil {
		return err
	}
	if rec.State == vmStateRunning {
		if exists || len(rec.AttachedDevices.Disks) != 0 {
			return sleepRecoveryError()
		}
		return m.captureRunningManagedSleep(ctx, sb, h)
	}

	if !found || rec.ID != h.Sleep.VMID || rec.Config.Name != h.VMName || rec.State != vmStateStopped || rec.TransitionGeneration != h.Sleep.Generation+1 || rec.LastTransitionReason != vmReasonStopUser || !exists || !m.managed.processGone(h.Sleep.BeforeRestore) {
		return sleepRecoveryError()
	}
	next := *h.Sleep
	next.Phase, next.SourceID = sleepSleeping, source.ID
	if err = m.managed.commitManagedSleep(h, &next); err != nil {
		return err
	}
	h.Sleep = &next
	if err = m.projectManagedSleep(sb, h); err != nil {
		return err
	}
	m.recordHibernate(ctx, sb)
	return nil
}

func (m *Manager) captureRunningManagedSleep(ctx context.Context, sb *types.Sandbox, h volumeHold) error {
	if _, err := m.sleepVM(ctx, h, false); err != nil {
		return err
	}
	effectErr := m.eng.Hibernate(ctx, h.VMName, h.Sleep.Snapshot)
	rec, found, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return errors.Join(effectErr, err)
	}
	_, exists, err := m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if err != nil {
		return errors.Join(effectErr, err)
	}
	if !found {
		return errors.Join(effectErr, sleepRecoveryError())
	}
	if rec.State != vmStateRunning {
		return m.captureManagedSleep(ctx, sb, h)
	}
	if exists {
		return errors.Join(effectErr, sleepRecoveryError())
	}
	if err = m.setSleepPhase(h, sleepAborting); err != nil {
		return errors.Join(effectErr, err)
	}
	h, _ = m.managedHold(sb.ID)
	return errors.Join(effectErr, sleepRecoveryError(), m.resumeManagedSleep(ctx, sb, h, false))
}

func (m *Manager) wakeManagedSleep(ctx context.Context, sb *types.Sandbox, h volumeHold) error {
	rec, err := m.restoreManagedSleep(ctx, h)
	if err != nil {
		return err
	}
	if h.Sleep.Restored == (processIdentity{}) {
		p, identityErr := m.managed.process(rec.PID)
		if identityErr != nil {
			return identityErr
		}
		next := *h.Sleep
		next.Restored = p
		h.Process = p
		if err = m.managed.commitManagedSleep(h, &next); err != nil {
			return err
		}
		h.Sleep = &next
	}
	rec, err = m.consumeManagedSleepSource(ctx, h)
	if err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, h.VMName, rec.VsockSocket, claimProbeTimeout)
	if err != nil {
		return err
	}
	if err = m.attachManagedSleep(ctx, h, sock, true); err != nil {
		return err
	}
	m.mu.Lock()
	sb.VsockSocket = sock
	m.mu.Unlock()
	return m.setSleepPhase(h, sleepResuming)
}

func (m *Manager) resumeManagedSleep(ctx context.Context, sb *types.Sandbox, h volumeHold, restored bool) error {
	var rec types.VMRecord
	var err error
	if restored {
		rec, err = m.consumeManagedSleepSource(ctx, h)
	} else {
		rec, err = m.sleepVM(ctx, h, false)
	}
	if err != nil {
		return err
	}
	if err = m.attachManagedSleep(ctx, h, rec.VsockSocket, restored); err != nil {
		return err
	}
	if err = m.sleepHook(ctx, h, rec.VsockSocket, "resume"); err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, h.VMName, rec.VsockSocket, claimProbeTimeout)
	if err != nil {
		return err
	}
	rec, err = m.sleepVM(ctx, h, restored)
	if err != nil {
		return err
	}
	if !exactSleepDisk(rec, h.Refs[0]) {
		return sleepRecoveryError()
	}
	if err = m.eng.VerifyMountedVolume(ctx, sock, h.Refs[0].Volume.Name, h.Refs[0].Volume.Mount); err != nil {
		return err
	}
	m.closeManagedStreams(sb)
	m.mu.Lock()
	sb.VsockSocket, sb.HibernateSnap, sb.PendingSnap, sb.ReaderSafe = sock, "", "", false
	js := m.store.set(sb)
	m.mu.Unlock()
	if err = m.store.commitDurable(js); err != nil {
		m.pauseManagedSleep(sb, h)
		return err
	}
	m.mu.Lock()
	_, outbound := m.view.Load().out.Resolve(sb, m.claimPooledLocked)
	m.mu.Unlock()
	if outbound {
		return errors.Join(ErrVolumeCapture, m.projectManagedSleep(sb, h))
	}
	if err = m.managed.commitManagedSleep(h, nil); err != nil {
		m.pauseManagedSleep(sb, h)
		return err
	}
	m.mu.Lock()
	_, outbound = m.view.Load().out.Resolve(sb, m.claimPooledLocked)
	if !outbound {
		m.managedStreams[sb] = make(chan struct{})
	}
	m.mu.Unlock()
	if outbound {
		return errors.Join(ErrVolumeCapture, m.managed.commitManagedSleep(h, h.Sleep), m.projectManagedSleep(sb, h))
	}
	if restored {
		m.counters.wakes.Add(1)
		m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
	}
	return nil
}

func (m *Manager) pauseManagedSleep(sb *types.Sandbox, h volumeHold) claimSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	projectSleepState(sb, h.Sleep)
	return m.store.set(sb)
}

func (m *Manager) projectManagedSleep(sb *types.Sandbox, h volumeHold) error {
	if err := m.store.commitDurable(m.pauseManagedSleep(sb, h)); err != nil {
		return fmt.Errorf("persist managed sleep: %w", err)
	}
	return nil
}

func (m *Manager) closeManagedStreams(sb *types.Sandbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch := m.managedStreams[sb]; ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}
