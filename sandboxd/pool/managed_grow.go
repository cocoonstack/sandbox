package pool

import (
	"context"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func growInProgress(name string) error {
	return &VolumeError{Status: 409, Code: volumeBusyCode, Message: "volume growth is in progress", Volume: name}
}

// GrowVolume grows an unheld managed volume to an absolute size; repeating the current size is a no-op.
func (m *Manager) GrowVolume(ctx context.Context, name, expectedID string, sizeGiB int64, tenant string) (types.ManagedVolumeInfo, error) {
	if !types.ValidVolumeName(name) || sizeGiB < 1 || sizeGiB > 1024 {
		return types.ManagedVolumeInfo{}, volumeError(400, "invalid_volume_grow", "invalid volume name or size_gib (1 through 1024)")
	}
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	if err := ctx.Err(); err != nil {
		return types.ManagedVolumeInfo{}, err
	}
	target := sizeGiB << 30
	m.mu.Lock()
	r.mu.Lock()
	defer r.mu.Unlock()
	v, err := r.admitGrow(name, expectedID, tenant, target)
	if err == nil && target > v.Size {
		err = m.volumeRefusalLocked(v)
	}
	if err == nil && target > v.Size {
		// recorded under m.mu, so a claim that reserves the name afterwards refuses on grow_to
		v.GrowTo = target
		r.data.Volumes[name] = v
	}
	m.mu.Unlock()
	if err != nil {
		return types.ManagedVolumeInfo{}, err
	}
	if v.Size == target {
		return r.info(v), nil
	}
	if err := r.commitAndRelock(); err != nil {
		current := r.data.Volumes[name]
		current.GrowTo = 0
		r.data.Volumes[name] = current
		return types.ManagedVolumeInfo{}, err
	}
	r.mu.Unlock()
	actual, growErr := r.growBacking(v)
	r.mu.Lock()
	v = settleGrowth(r.data.Volumes[name], actual)
	r.data.Volumes[name] = v
	if err := r.commitAndRelock(); err != nil {
		return types.ManagedVolumeInfo{}, err
	}
	if growErr != nil {
		return types.ManagedVolumeInfo{}, managedStorageError()
	}
	return r.info(v), nil
}

func (r *volumeRegistry) admitGrow(name, expectedID, tenant string, target int64) (managedVolume, error) {
	if r.closed || r.writeFault != nil {
		return managedVolume{}, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok || tenant != "" && tenant != v.Tenant {
		return managedVolume{}, managedNotFound()
	}
	switch {
	case expectedID == "":
		return v, volumeError(428, "volume_precondition_required", "If-Match must identify the volume")
	case string(v.ID) != expectedID:
		return v, volumeError(412, "volume_precondition_failed", "volume identity does not match")
	case v.GrowTo != 0:
		return v, growInProgress(name)
	case v.State == volumeRecovery:
		return v, managedRecoveryError()
	case v.State != volumeReady:
		return v, volumeError(409, "volume_not_ready", "volume is being created or deleted")
	case target < v.Size:
		return v, volumeError(409, "volume_grow_conflict", "target is below the volume's capacity; volumes cannot shrink")
	}
	return v, nil
}

// growBacking extends the recorded backing to v.GrowTo and reports the size it left behind.
func (r *volumeRegistry) growBacking(v managedVolume) (backingIdentity, error) {
	if err := r.verifyDirectories(); err != nil {
		return backingIdentity{}, err
	}
	f, err := openPrivateFile(r.path(v.ID), syscall.O_RDWR)
	if err != nil {
		return backingIdentity{}, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return backingIdentity{}, err
	}
	raw := st.Sys().(*syscall.Stat_t)
	actual := backingIdentity{Device: backingDeviceID(raw.Dev), Inode: raw.Ino, Size: st.Size()}
	if actual.Device != v.Backing.Device || actual.Inode != v.Backing.Inode {
		return actual, errBackingMismatch
	}
	if err := f.Truncate(v.GrowTo); err != nil {
		return actual, err
	}
	actual.Size = v.GrowTo
	return actual, f.Sync()
}

// settleGrowth adopts GrowTo only when the backing already has that size, then clears the intent.
func settleGrowth(v managedVolume, actual backingIdentity) managedVolume {
	if v.GrowTo != 0 && actual.Device == v.Backing.Device && actual.Inode == v.Backing.Inode && actual.Size == v.GrowTo {
		v.Size, v.Backing.Size = v.GrowTo, v.GrowTo
	}
	v.GrowTo = 0
	return v
}
