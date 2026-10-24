package pool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
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

// managedSleepVolume resolves the one mounted rw managed volume of a claim whose shape can sleep.
func (m *Manager) managedSleepVolume(sb *types.Sandbox) (resolvedVolume, bool) {
	r := m.managed
	if r == nil {
		return resolvedVolume{}, false
	}
	m.mu.Lock()
	volumes, want := slices.Clone(sb.Volumes), sb.VolumesWant
	shaped := len(volumes) == 1 && slices.Equal(want, volumes) && volumes[0].RW() && volumes[0].Mount != "" && sb.ArchiveCk == "" && sb.ArchivePending == "" && !sb.HasReplica() && !sb.Replicated() && sb.Key.Net == types.NetNone
	m.mu.Unlock()
	if !shaped {
		return resolvedVolume{}, false
	}
	r.mu.Lock()
	v, ok := r.data.Volumes[volumes[0].Name]
	r.mu.Unlock()
	if !ok {
		return resolvedVolume{}, false
	}
	return resolvedVolume{disk: engine.VolumeSpec{Name: v.Name, Path: r.path(v.ID), RW: true}, applied: volumes[0], managedID: v.ID}, true
}

// sleepHolds reports whether a sleeping claim holds name; its wake reconciles the backing.
func (m *Manager) sleepHolds(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sb := range m.claimed {
		if sb.VolumeSleep != nil && volumeIndex(sb.Volumes, name) >= 0 {
			return true
		}
	}
	return false
}

func (m *Manager) verifySleepBacking(sb *types.Sandbox, vol resolvedVolume) error {
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.data.Volumes[vol.applied.Name]
	if !ok || v.ID != vol.managedID || v.State != volumeReady || (sb.Tenant != "" && v.Tenant != sb.Tenant) {
		return managedStorageError()
	}
	r.mu.Unlock()
	err := r.verify(v)
	r.mu.Lock()
	if errors.Is(err, errBackingMismatch) {
		return managedRecoveryError()
	}
	if err != nil {
		return managedStorageError()
	}
	return nil
}

func (m *Manager) managedSleepPreflight(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) (types.VMRecord, error) {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	busy := m.volumeRuns[sb] != nil
	_, outbound := m.view.Load().out.Resolve(sb, m.claimPooledLocked)
	m.mu.Unlock()
	if m.managedSleepHook == nil || busy || outbound {
		return types.VMRecord{}, ErrVolumeCapture
	}
	if !live {
		return types.VMRecord{}, ErrUnknownSandbox
	}
	if err := m.diskErr(); err != nil {
		return types.VMRecord{}, err
	}
	if err := m.verifySleepBacking(sb, vol); err != nil {
		return types.VMRecord{}, err
	}
	if err := m.eng.CheckLiveVolumeSupport(ctx); err != nil {
		return types.VMRecord{}, err
	}
	rec, present, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return rec, err
	}
	if !present || rec.Config.Name != sb.VMName || rec.ID == "" || rec.LastTransitionReason == "" || rec.TransitionGeneration == 0 || rec.State != vmStateRunning || !exactSleepDisk(rec, vol) {
		return rec, sleepRecoveryError()
	}
	if err := m.eng.VerifyMountedVolume(ctx, rec.VsockSocket, vol.applied.Name, vol.applied.Mount); err != nil {
		return rec, err
	}
	return rec, nil
}

func (m *Manager) driveManagedSleepLocked(ctx context.Context, sb *types.Sandbox, target managedSleepTarget) error {
	vol, shaped := m.managedSleepVolume(sb)
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	m.mu.Unlock()
	switch {
	case !live:
		return ErrUnknownSandbox
	case !shaped && sb.VolumeSleep == nil:
		return ErrVolumeCapture
	case !shaped:
		return sleepRecoveryError()
	}
	ctx = context.WithoutCancel(ctx)
	start := time.Now()
	if sb.VolumeSleep == nil {
		if err := m.beginManagedSleep(ctx, sb, vol); err != nil {
			return err
		}
	} else if err := m.store.commit(m.store.mark()); err != nil {
		// an intent whose write failed must land before this call adds effects
		return fmt.Errorf("persist managed sleep: %w", err)
	}
	for {
		s := sb.VolumeSleep
		if s == nil {
			return nil
		}
		if err := m.verifySleepBacking(sb, vol); err != nil {
			return err
		}
		switch s.Phase {
		case types.SleepPreparing:
			if err := m.setSleepPhase(ctx, sb, types.SleepAborting); err != nil {
				return err
			}
		case types.SleepAborting:
			if err := m.resumeManagedSleep(ctx, sb, vol, false); err != nil {
				return err
			}
			if target == managedRunning {
				return nil
			}
			return m.driveManagedSleepLocked(ctx, sb, target)
		case types.SleepCapturing:
			if err := m.captureManagedSleep(ctx, sb, vol); err != nil {
				return err
			}
		case types.SleepSleeping:
			if err := m.verifySleepingVM(ctx, sb); err != nil {
				return err
			}
			if target == managedSleeping {
				return nil
			}
			if err := m.setSleepPhase(ctx, sb, types.SleepWaking); err != nil {
				return err
			}
		case types.SleepWaking:
			if err := m.wakeManagedSleep(ctx, sb, vol); err != nil {
				return err
			}
		case types.SleepResuming:
			if err := m.resumeManagedSleep(ctx, sb, vol, true); err != nil {
				return err
			}
			m.counters.wakes.Add(1)
			m.counters.wakeNanos.Add(uint64(time.Since(start))) //nolint:gosec // durations are positive
			m.recordUsage(ctx, usageEvent{Event: "wake", ID: sb.ID, VMName: sb.VMName})
			if target == managedRunning {
				return nil
			}
			return m.driveManagedSleepLocked(ctx, sb, target)
		default:
			return sleepRecoveryError()
		}
	}
}

func (m *Manager) beginManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) error {
	rec, err := m.managedSleepPreflight(ctx, sb, vol)
	if err != nil {
		return err
	}
	hook := types.VolumeHook(*m.managedSleepHook)
	hook.HookArgv = slices.Clone(hook.HookArgv)
	next := &types.VolumeSleep{Operation: randHex(16), Phase: types.SleepPreparing, Snapshot: hibernatePrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3), Hook: hook, VMID: rec.ID, Generation: rec.TransitionGeneration}
	live, err := m.commitIfLive(ctx, sb, func() {
		sb.VolumeSleep = next
		projectSleepState(sb, next)
	})
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		// nothing has touched the guest yet, so the intent can still be withdrawn
		m.mu.Lock()
		sb.VolumeSleep, sb.PendingSnap = nil, ""
		m.store.set(sb)
		m.mu.Unlock()
		return fmt.Errorf("persist managed sleep intent: %w", err)
	}
	m.out.Disarm(sb.ID, false)
	return m.prepareManagedSleep(ctx, sb, vol)
}

// journalSleep publishes next on the claim row; a failed write keeps memory and recommits but ends this call.
func (m *Manager) journalSleep(ctx context.Context, sb *types.Sandbox, next *types.VolumeSleep, sock string) error {
	live, err := m.commitIfLive(ctx, sb, func() {
		sb.VolumeSleep = next
		projectSleepState(sb, next)
		if sock != "" {
			sb.VsockSocket = sock
		}
	})
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		return fmt.Errorf("persist managed sleep: %w", err)
	}
	return nil
}

func (m *Manager) setSleepPhase(ctx context.Context, sb *types.Sandbox, phase types.SleepPhase) error {
	next := *sb.VolumeSleep
	next.Phase = phase
	return m.journalSleep(ctx, sb, &next, "")
}

func (m *Manager) prepareManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) error {
	rec, err := m.sleepVM(ctx, sb, false)
	if err == nil {
		err = m.sleepHook(ctx, sb, vol, rec.VsockSocket, "prepare")
	}
	if err == nil {
		err = m.eng.VerifyMountedVolume(ctx, rec.VsockSocket, vol.applied.Name, vol.applied.Mount)
		if err == nil {
			err = m.eng.DetachVolume(ctx, sb.VMName, rec.VsockSocket, vol.applied.Name, vol.applied.Mount)
		}
	}
	if err == nil {
		rec, err = m.sleepVM(ctx, sb, false)
		if err == nil && len(rec.AttachedDevices.Disks) != 0 {
			err = sleepRecoveryError()
		}
	}
	if err != nil {
		if persistErr := m.setSleepPhase(ctx, sb, types.SleepAborting); persistErr != nil {
			return errors.Join(err, persistErr)
		}
		if resumeErr := m.resumeManagedSleep(ctx, sb, vol, false); resumeErr != nil {
			return errors.Join(err, sleepRecoveryError(), resumeErr)
		}
		return err
	}
	return m.setSleepPhase(ctx, sb, types.SleepCapturing)
}

func (m *Manager) captureManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) error {
	s := sb.VolumeSleep
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return err
	}
	if !found || rec.ID != s.VMID || rec.Config.Name != sb.VMName {
		return sleepRecoveryError()
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, s.Snapshot)
	if err != nil {
		return err
	}
	if rec.State == vmStateRunning {
		if exists || len(rec.AttachedDevices.Disks) != 0 {
			return sleepRecoveryError()
		}
		return m.captureRunningManagedSleep(ctx, sb, vol)
	}
	if rec.State != vmStateStopped || rec.TransitionGeneration <= s.Generation || rec.LastTransitionReason != vmReasonStopUser || !exists {
		return sleepRecoveryError()
	}
	next := *s
	next.Phase, next.SourceID = types.SleepSleeping, source.ID
	err = m.journalSleep(ctx, sb, &next, "")
	if errors.Is(err, ErrUnknownSandbox) {
		return err
	}
	// the VM is captured either way, so the billing window closes here
	m.recordHibernate(ctx, sb)
	return err
}

func (m *Manager) captureRunningManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) error {
	if _, err := m.sleepVM(ctx, sb, false); err != nil {
		return err
	}
	snapshot := sb.VolumeSleep.Snapshot
	effectErr := m.eng.Hibernate(ctx, sb.VMName, snapshot)
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return errors.Join(effectErr, err)
	}
	_, exists, err := m.eng.InspectHibernateSource(ctx, snapshot)
	if err != nil {
		return errors.Join(effectErr, err)
	}
	if !found {
		return errors.Join(effectErr, sleepRecoveryError())
	}
	if rec.State != vmStateRunning {
		return m.captureManagedSleep(ctx, sb, vol)
	}
	if exists {
		return errors.Join(effectErr, sleepRecoveryError())
	}
	if err = m.setSleepPhase(ctx, sb, types.SleepAborting); err != nil {
		return errors.Join(effectErr, err)
	}
	if err = m.resumeManagedSleep(ctx, sb, vol, false); err != nil {
		return errors.Join(effectErr, sleepRecoveryError(), err)
	}
	return cmp.Or(effectErr, errors.New("managed hibernate did not take effect"))
}

func (m *Manager) wakeManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume) error {
	rec, err := m.restoreManagedSleep(ctx, sb)
	if err != nil {
		return err
	}
	rec, err = m.consumeManagedSleepSource(ctx, sb)
	if err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, sb.VMName, rec.VsockSocket, claimProbeTimeout)
	if err != nil {
		return err
	}
	if err = m.attachManagedSleep(ctx, sb, vol, sock, true); err != nil {
		return err
	}
	next := *sb.VolumeSleep
	next.Phase = types.SleepResuming
	return m.journalSleep(ctx, sb, &next, sock)
}

func (m *Manager) resumeManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume, restored bool) error {
	var rec types.VMRecord
	var err error
	if restored {
		rec, err = m.consumeManagedSleepSource(ctx, sb)
	} else {
		rec, err = m.sleepVM(ctx, sb, false)
	}
	if err != nil {
		return err
	}
	if err = m.attachManagedSleep(ctx, sb, vol, rec.VsockSocket, restored); err != nil {
		return err
	}
	logger := log.WithFunc("pool.resumeManagedSleep")
	// the host verified the mount, so a failed hook leaves only the guest's writers paused
	if err = m.sleepHook(ctx, sb, vol, rec.VsockSocket, "resume"); err != nil {
		logger.Errorf(ctx, err, "resume hook of %s failed; its writers stay paused", sb.ID)
	}
	sock, err := m.probeReady(ctx, sb.VMName, rec.VsockSocket, claimProbeTimeout)
	if err != nil {
		return err
	}
	rec, err = m.sleepVM(ctx, sb, restored)
	if err != nil {
		return err
	}
	if !exactSleepDisk(rec, vol) {
		return sleepRecoveryError()
	}
	if err = m.eng.VerifyMountedVolume(ctx, sock, vol.applied.Name, vol.applied.Mount); err != nil {
		return err
	}
	live, err := m.commitIfLive(ctx, sb, func() {
		sb.VolumeSleep = nil
		sb.VsockSocket, sb.HibernateSnap, sb.PendingSnap, sb.ReaderSafe = sock, "", "", false
	})
	if !live {
		return ErrUnknownSandbox
	}
	if err != nil {
		// memory never rolls back past a verified mount; the recommit converges the row
		logger.Warnf(ctx, "persist resume of %s: %v; retrying", sb.ID, err)
	}
	return nil
}

func projectSleepState(sb *types.Sandbox, s *types.VolumeSleep) {
	sb.ReaderSafe = false
	switch s.Phase {
	case types.SleepPreparing, types.SleepAborting, types.SleepCapturing:
		sb.PendingSnap, sb.HibernateSnap = s.Snapshot, ""
	default:
		sb.HibernateSnap, sb.PendingSnap = s.Snapshot, ""
	}
}
