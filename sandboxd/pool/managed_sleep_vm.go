package pool

import (
	"context"
	"errors"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const vmReasonStopUser = "stop-user"

func exactSleepDisk(rec types.VMRecord, ref volumeRef) bool {
	return len(rec.AttachedDevices.Disks) == 1 && rec.AttachedDevices.Disks[0].Name == ref.Volume.Name && rec.AttachedDevices.Disks[0].Path == ref.Path && !rec.AttachedDevices.Disks[0].ReadOnly
}

func (m *Manager) sleepVM(ctx context.Context, h volumeHold, restored bool) (types.VMRecord, error) {
	rec, found, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return rec, err
	}
	s := h.Sleep
	generation := s.Generation
	if restored {
		generation += 2
	}
	if !found || rec.Config.Name != h.VMName || rec.ID != s.VMID || rec.State != vmStateRunning || rec.TransitionGeneration != generation || rec.LastTransitionReason == "" || (restored && rec.LastTransitionReason != "restore") {
		return rec, sleepRecoveryError()
	}
	p, err := m.managed.process(rec.PID)
	if err != nil || (s.Restored != (processIdentity{}) && p != s.Restored) || (!restored && p != s.BeforeRestore) {
		return rec, sleepRecoveryError()
	}
	return rec, nil
}

func (m *Manager) sleepHook(ctx context.Context, h volumeHold, sock, action string) error {
	s, ref := h.Sleep, h.Refs[0]
	return m.eng.RunVolumeHook(ctx, sock, engine.VolumeHookCall{Argv: s.Hook.HookArgv, Action: action, Operation: s.Operation, VolumeID: string(ref.ID), VolumeName: ref.Volume.Name, Mount: ref.Volume.Mount, Timeout: types.Seconds(s.Hook.HookTimeoutSeconds)})
}

func (m *Manager) restoreManagedSleep(ctx context.Context, h volumeHold) (types.VMRecord, error) {
	rec, found, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return rec, err
	}
	if !found || rec.ID != h.Sleep.VMID || rec.Config.Name != h.VMName {
		return rec, sleepRecoveryError()
	}
	if rec.State == vmStateRunning {
		return m.sleepVM(ctx, h, true)
	}
	if h.Sleep.Restored != (processIdentity{}) || rec.State != vmStateStopped || rec.TransitionGeneration != h.Sleep.Generation+1 || rec.LastTransitionReason != vmReasonStopUser || !m.managed.processGone(h.Sleep.BeforeRestore) {
		return rec, sleepRecoveryError()
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if err != nil {
		return rec, err
	}
	if !exists || source.ID != h.Sleep.SourceID {
		return rec, sleepRecoveryError()
	}
	if err = m.diskErr(); err != nil {
		return rec, err
	}
	_, effectErr := m.eng.Restore(ctx, h.VMName, h.Sleep.Snapshot)
	rec, err = m.sleepVM(ctx, h, true)
	if err != nil {
		return rec, errors.Join(effectErr, err)
	}
	return rec, nil
}

func (m *Manager) consumeManagedSleepSource(ctx context.Context, h volumeHold) (types.VMRecord, error) {
	if h.Sleep.Restored == (processIdentity{}) || h.Process != h.Sleep.Restored {
		return types.VMRecord{}, sleepRecoveryError()
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if err != nil {
		return types.VMRecord{}, err
	}
	if exists && source.ID != h.Sleep.SourceID {
		return types.VMRecord{}, sleepRecoveryError()
	}
	var effectErr error
	if exists {
		effectErr = m.eng.SnapshotRemove(ctx, h.Sleep.Snapshot)
	}
	source, exists, err = m.eng.InspectHibernateSource(ctx, h.Sleep.Snapshot)
	if err != nil {
		return types.VMRecord{}, errors.Join(effectErr, err)
	}
	if exists {
		return types.VMRecord{}, errors.Join(effectErr, sleepRecoveryError())
	}
	return m.sleepVM(ctx, h, true)
}

func (m *Manager) attachManagedSleep(ctx context.Context, h volumeHold, sock string, restored bool) error {
	if err := m.verifySleepBacking(h); err != nil {
		return err
	}
	rec, err := m.sleepVM(ctx, h, restored)
	if err != nil {
		return err
	}
	ref := h.Refs[0]
	if len(rec.AttachedDevices.Disks) == 0 {
		effectErr := m.eng.DiskAttach(ctx, h.VMName, engine.VolumeSpec{Name: ref.Volume.Name, Path: ref.Path, RW: true})
		rec, err = m.sleepVM(ctx, h, restored)
		if err != nil {
			return errors.Join(effectErr, err)
		}
		if !exactSleepDisk(rec, ref) {
			return errors.Join(effectErr, sleepRecoveryError())
		}
	} else if !exactSleepDisk(rec, ref) {
		return sleepRecoveryError()
	}
	if err := m.eng.MountLiveVolume(ctx, sock, ref.Volume.Name, ref.Volume.Mount, true); err != nil {
		return err
	}
	return m.eng.VerifyMountedVolume(ctx, sock, ref.Volume.Name, ref.Volume.Mount)
}

func (m *Manager) verifySleepingVM(ctx context.Context, h volumeHold) error {
	rec, found, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil {
		return err
	}
	if !found || rec.Config.Name != h.VMName || rec.ID != h.Sleep.VMID || rec.State != vmStateStopped || rec.TransitionGeneration != h.Sleep.Generation+1 || rec.LastTransitionReason != vmReasonStopUser || !m.managed.processGone(h.Sleep.BeforeRestore) {
		return sleepRecoveryError()
	}
	return nil
}
