package pool

import (
	"context"
	"fmt"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// AttachVolumes adds mounted, read-only volumes to a running claim. Batches
// commit in order: retry the request after a partial failure to finish it.
func (m *Manager) AttachVolumes(ctx context.Context, id string, cred Cred, requested []types.Volume) ([]types.Volume, error) {
	if types.VolumesAttachOnly(requested) || slices.ContainsFunc(requested, types.Volume.RW) {
		return nil, fmt.Errorf("%w: live attach requires read-only mounts", ErrBadVolume)
	}
	requested, err := types.ValidateVolumes(requested, false)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadVolume, err)
	}
	sb, ok := m.resolve(id, cred)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	sb.VolumeMu.Lock()
	defer sb.VolumeMu.Unlock()
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err = m.mutableVolumes(sb); err != nil {
		return nil, err
	}
	if err = m.tenantRemoved(ctx, sb.Tenant); err != nil {
		return nil, err
	}
	if pending := sb.PendingVolume; pending != nil && (pending.Detach || !slices.Contains(requested, pending.Volume)) {
		return nil, ErrVolumePending
	}
	toAdd, err := liveVolumeAdditions(heldVolumes(sb), requested)
	if err != nil {
		return nil, err
	}
	// A pending attach already holds admission but still needs convergence.
	if pending := sb.PendingVolume; pending != nil {
		toAdd = append([]types.Volume{pending.Volume}, toAdd...)
	}
	resolved, err := m.resolveVolumes(ctx, sb.Tenant, toAdd)
	if err != nil {
		return nil, err
	}
	for _, volume := range resolved {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if sb.PendingVolume == nil {
			if err := m.startVolumeAttach(ctx, sb, volume); err != nil {
				return nil, err
			}
		} else if err := m.confirmVolumesClean([]resolvedVolume{volume}); err != nil {
			return nil, err
		}
		if err := m.eng.AttachVolume(ctx, sb.VMName, sb.VsockSocket, volume.disk, volume.applied.Mount); err != nil {
			return nil, fmt.Errorf("attach volume %q (retry or detach to recover): %w", volume.applied.Name, err)
		}
		if err := m.finishVolumeMutation(ctx, sb); err != nil {
			return nil, err
		}
	}
	return slices.Clone(sb.Volumes), nil
}

// DetachVolumes removes read-only volumes, including an interrupted attach.
// An absent valid name is a no-op; batches may partially complete on failure.
func (m *Manager) DetachVolumes(ctx context.Context, id string, cred Cred, names []string) ([]types.Volume, error) {
	if err := validateDetachNames(names); err != nil {
		return nil, err
	}
	sb, ok := m.resolve(id, cred)
	if !ok {
		return nil, ErrUnknownSandbox
	}
	sb.VolumeMu.Lock()
	defer sb.VolumeMu.Unlock()
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err := m.mutableVolumes(sb); err != nil {
		return nil, err
	}
	if pending := sb.PendingVolume; pending != nil && !slices.Contains(names, pending.Volume.Name) {
		return nil, ErrVolumePending
	}
	held := heldVolumes(sb)
	for _, volume := range held {
		if slices.Contains(names, volume.Name) && volume.RW() {
			return nil, fmt.Errorf("%w: live detach of writable volumes is not supported", ErrBadVolume)
		}
	}
	// Recover the pending operation first, even if it was last in the request.
	if pending := sb.PendingVolume; pending != nil {
		if err := m.detachLiveVolume(ctx, sb, pending.Volume); err != nil {
			return nil, err
		}
	}
	for _, volume := range slices.Clone(sb.Volumes) {
		if slices.Contains(names, volume.Name) {
			if err := m.detachLiveVolume(ctx, sb, volume); err != nil {
				return nil, err
			}
		}
	}
	return slices.Clone(sb.Volumes), nil
}

// mutableVolumes runs under Transition; manager-owned fields need m.mu too.
func (m *Manager) mutableVolumes(sb *types.Sandbox) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.claimed[sb.ID] != sb:
		return ErrUnknownSandbox
	case sb.Failed != "":
		return ErrFailed
	case sb.ArchiveCk != "":
		return ErrArchived
	case sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.VsockSocket == "":
		return ErrPaused
	}
	return nil
}

func liveVolumeAdditions(current, requested []types.Volume) ([]types.Volume, error) {
	combined := slices.Clone(current)
	var added []types.Volume
	for _, volume := range requested {
		i := slices.IndexFunc(current, func(v types.Volume) bool { return v.Name == volume.Name })
		if i >= 0 {
			if current[i] != volume {
				return nil, fmt.Errorf("%w: volume %q already has a different mount or mode", ErrBadVolume, volume.Name)
			}
			continue
		}
		combined = append(combined, volume)
		added = append(added, volume)
	}
	if len(combined) > types.MaxClaimVolumes {
		return nil, fmt.Errorf("%w: too many volumes", ErrBadVolume)
	}
	// Validate only mounted entries, preserving existing attach-only devices.
	mounted := slices.DeleteFunc(slices.Clone(combined), func(v types.Volume) bool { return v.Mount == "" })
	if _, err := types.ValidateVolumes(mounted, false); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadVolume, err)
	}
	return added, nil
}

func validateDetachNames(names []string) error {
	if len(names) > types.MaxClaimVolumes {
		return fmt.Errorf("%w: too many volume names", ErrBadVolume)
	}
	for i, name := range names {
		if !types.ValidVolumeName(name) || slices.Contains(names[:i], name) {
			return fmt.Errorf("%w: invalid or duplicate volume name %q", ErrBadVolume, name)
		}
	}
	return nil
}

func (m *Manager) startVolumeAttach(ctx context.Context, sb *types.Sandbox, volume resolvedVolume) error {
	m.mu.Lock()
	if sb.OnExpire == types.ExpireArchive {
		m.mu.Unlock()
		return ErrVolumeCapture
	}
	err := m.reserveVolumes([]types.Volume{volume.applied})
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if err = m.confirmVolumesClean([]resolvedVolume{volume}); err == nil {
		err = m.setVolumeIntent(ctx, sb, &types.VolumeMutation{Volume: volume.applied})
	}
	if err != nil {
		m.unreserveVolumes([]types.Volume{volume.applied})
	}
	return err
}

func (m *Manager) detachLiveVolume(ctx context.Context, sb *types.Sandbox, volume types.Volume) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pending := sb.PendingVolume; pending == nil || !pending.Detach {
		if err := m.setVolumeIntent(ctx, sb, &types.VolumeMutation{Volume: volume, Detach: true}); err != nil {
			return err
		}
	}
	if err := m.eng.DetachVolume(ctx, sb.VMName, sb.VsockSocket, volume.Name, volume.Mount); err != nil {
		return fmt.Errorf("detach volume %q (retry to recover): %w", volume.Name, err)
	}
	return m.finishVolumeMutation(ctx, sb)
}

func (m *Manager) setVolumeIntent(ctx context.Context, sb *types.Sandbox, next *types.VolumeMutation) error {
	m.mu.Lock()
	previous := sb.PendingVolume
	sb.PendingVolume = next
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		sb.PendingVolume = previous
		rb := m.store.set(sb)
		m.mu.Unlock()
		m.recommit(ctx, rb)
		return fmt.Errorf("persist volume intent: %w", err)
	}
	return nil
}

func (m *Manager) finishVolumeMutation(ctx context.Context, sb *types.Sandbox) error {
	m.mu.Lock()
	previous, pending := sb.Volumes, sb.PendingVolume
	next := slices.Clone(previous)
	if pending.Detach {
		next = slices.DeleteFunc(next, func(v types.Volume) bool { return v.Name == pending.Volume.Name })
	} else {
		next = append(next, pending.Volume)
	}
	sb.Volumes, sb.PendingVolume = next, nil
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		// Keep both the intent and its admission hold until a retry commits.
		m.mu.Lock()
		sb.Volumes, sb.PendingVolume = previous, pending
		rb := m.store.set(sb)
		m.mu.Unlock()
		m.recommit(ctx, rb)
		return fmt.Errorf("persist volume result: %w", err)
	}
	verb := "volume_attach"
	if pending.Detach {
		m.unreserveVolumes([]types.Volume{pending.Volume})
		verb = "volume_detach"
	}
	sb.Touch()
	m.recordUsage(ctx, usageEvent{Event: verb, ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Volumes: []string{pending.Volume.Name}})
	return nil
}

// heldVolumes includes a possibly attached device even before it is committed.
// Callers hold m.mu or Transition (both are required for mutations).
func heldVolumes(sb *types.Sandbox) []types.Volume {
	volumes := slices.Clone(sb.Volumes)
	if pending := sb.PendingVolume; pending != nil && !slices.ContainsFunc(volumes, func(v types.Volume) bool { return v.Name == pending.Volume.Name }) {
		volumes = append(volumes, pending.Volume)
	}
	return volumes
}

func cloneVolumeMutation(op *types.VolumeMutation) *types.VolumeMutation {
	if op == nil {
		return nil
	}
	cloned := *op
	return &cloned
}
