package pool

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	vmReasonStopUser = "stop-user"
	vmReasonRestore  = "restore"
)

func exactSleepDisk(rec types.VMRecord, vol resolvedVolume) bool {
	return len(rec.AttachedDevices.Disks) == 1 && rec.AttachedDevices.Disks[0].Name == vol.applied.Name && rec.AttachedDevices.Disks[0].Path == vol.disk.Path && !rec.AttachedDevices.Disks[0].ReadOnly
}

// sleepVM returns the running VM the sleep record names: the original before capture, the restored one after.
func (m *Manager) sleepVM(ctx context.Context, sb *types.Sandbox, restored bool) (types.VMRecord, error) {
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return rec, err
	}
	s := sb.VolumeSleep
	current := rec.TransitionGeneration == s.Generation
	if restored {
		current = rec.TransitionGeneration > s.Generation && rec.LastTransitionReason == vmReasonRestore
	}
	if !found || rec.Config.Name != sb.VMName || rec.ID != s.VMID || rec.State != vmStateRunning || rec.LastTransitionReason == "" || !current {
		return rec, sleepRecoveryError()
	}
	return rec, nil
}

func (m *Manager) sleepHook(ctx context.Context, sb *types.Sandbox, vol resolvedVolume, sock, action string) error {
	s := sb.VolumeSleep
	err := m.eng.RunVolumeHook(ctx, sock, engine.VolumeHookCall{Argv: s.Hook.HookArgv, Action: action, Operation: s.Operation, VolumeID: string(vol.managedID), VolumeName: vol.applied.Name, Mount: vol.applied.Mount, Timeout: types.Seconds(s.Hook.HookTimeoutSeconds)})
	// only the guest's own exit is the tenant's to see; a transport error names host paths
	if _, exited := errors.AsType[*engine.VolumeHookExit](err); !exited {
		return err
	}
	return volumeError(http.StatusConflict, "volume_hook_failed", fmt.Sprintf("guest %s hook failed: %.512s", action, err.Error()))
}

func (m *Manager) restoreManagedSleep(ctx context.Context, sb *types.Sandbox) (types.VMRecord, error) {
	s := sb.VolumeSleep
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return rec, err
	}
	if !found || rec.ID != s.VMID || rec.Config.Name != sb.VMName {
		return rec, sleepRecoveryError()
	}
	if rec.State == vmStateRunning {
		return m.sleepVM(ctx, sb, true)
	}
	// a stopped origin or a restore that failed before any guest ran; the intact source guards the retry
	if rec.TransitionGeneration <= s.Generation {
		return rec, sleepRecoveryError()
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, s.Snapshot)
	if err != nil {
		return rec, err
	}
	if !exists || source.ID != s.SourceID {
		return rec, sleepRecoveryError()
	}
	if err = m.diskErr(); err != nil {
		return rec, err
	}
	if err = m.eng.CheckLiveVolumeSupport(ctx); err != nil {
		return rec, err
	}
	_, effectErr := m.eng.Restore(ctx, sb.VMName, s.Snapshot)
	rec, err = m.sleepVM(ctx, sb, true)
	if err != nil {
		return rec, errors.Join(effectErr, err)
	}
	return rec, nil
}

// consumeManagedSleepSource removes the memory image only under the restored VM it produced.
func (m *Manager) consumeManagedSleepSource(ctx context.Context, sb *types.Sandbox) (types.VMRecord, error) {
	s := sb.VolumeSleep
	if _, err := m.sleepVM(ctx, sb, true); err != nil {
		return types.VMRecord{}, err
	}
	source, exists, err := m.eng.InspectHibernateSource(ctx, s.Snapshot)
	if err != nil {
		return types.VMRecord{}, err
	}
	if exists && source.ID != s.SourceID {
		return types.VMRecord{}, sleepRecoveryError()
	}
	var effectErr error
	if exists {
		effectErr = m.eng.SnapshotRemove(ctx, s.Snapshot)
	}
	_, exists, err = m.eng.InspectHibernateSource(ctx, s.Snapshot)
	if err != nil {
		return types.VMRecord{}, errors.Join(effectErr, err)
	}
	if exists {
		return types.VMRecord{}, errors.Join(effectErr, sleepRecoveryError())
	}
	return m.sleepVM(ctx, sb, true)
}

func (m *Manager) attachManagedSleep(ctx context.Context, sb *types.Sandbox, vol resolvedVolume, sock string, restored bool) error {
	if err := m.verifySleepBacking(sb, vol); err != nil {
		return err
	}
	rec, err := m.sleepVM(ctx, sb, restored)
	if err != nil {
		return err
	}
	if len(rec.AttachedDevices.Disks) == 0 {
		effectErr := m.eng.DiskAttach(ctx, sb.VMName, vol.disk)
		rec, err = m.sleepVM(ctx, sb, restored)
		if err != nil {
			return errors.Join(effectErr, err)
		}
		if !exactSleepDisk(rec, vol) {
			return errors.Join(effectErr, sleepRecoveryError())
		}
	} else if !exactSleepDisk(rec, vol) {
		return sleepRecoveryError()
	}
	if err := m.eng.MountLiveVolume(ctx, sock, vol.applied.Name, vol.applied.Mount, true); err != nil {
		return err
	}
	return m.eng.VerifyMountedVolume(ctx, sock, vol.applied.Name, vol.applied.Mount)
}

func (m *Manager) verifySleepingVM(ctx context.Context, sb *types.Sandbox) error {
	s := sb.VolumeSleep
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return err
	}
	if !found || rec.Config.Name != sb.VMName || rec.ID != s.VMID || rec.State != vmStateStopped || rec.TransitionGeneration <= s.Generation || rec.LastTransitionReason != vmReasonStopUser {
		return sleepRecoveryError()
	}
	return nil
}
