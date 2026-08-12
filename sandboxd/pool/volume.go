package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// Sidecar, not data_dir: the marker travels with the image and survives a
	// data_dir wipe.
	volumeDirtySuffix = ".dirty"
	// Bounds the whole quiesce: teardown must not hang on a wedged guest.
	volumeQuiesceTimeout = 5 * time.Second
)

type catalogVolume struct {
	disk     engine.VolumeSpec
	tenants  []string
	writable bool
}

func (v catalogVolume) allowed(tenant string) bool {
	return tenant == "" || len(v.tenants) == 0 || slices.Contains(v.tenants, tenant)
}

// volumeHolders is one name's live admission state: a writer excludes every
// other claim, readers only exclude a writer.
type volumeHolders struct {
	writers int
	readers int
}

type resolvedVolume struct {
	disk    engine.VolumeSpec
	applied types.Volume
}

// Volumes reports the caller-visible fleet catalog, projected through this
// node's ACL metadata and local path state. Empty tenant means root.
func (m *Manager) Volumes(tenant string, holders map[string]int) []types.VolumeInfo {
	infos := make([]types.VolumeInfo, 0, len(m.volumes))
	for name, volume := range m.volumes {
		if !volume.allowed(tenant) {
			continue
		}
		info := types.VolumeInfo{
			Name:         name,
			DefaultMount: types.DefaultVolumeMount(name),
			Nodes:        holders[name],
			Writable:     volume.writable,
		}
		if st, err := os.Stat(volume.disk.Path); err == nil {
			info.SizeBytes = st.Size()
			info.Available = true
			info.Nodes = max(info.Nodes, 1)
		}
		infos = append(infos, info)
	}
	// Root can safely see peer-only names. Tenant callers need the local catalog
	// metadata so the access list can be applied without gossiping it.
	if tenant == "" {
		for name, nodes := range holders {
			if _, ok := m.volumes[name]; !ok {
				infos = append(infos, types.VolumeInfo{
					Name:         name,
					DefaultMount: types.DefaultVolumeMount(name),
					Nodes:        nodes,
				})
			}
		}
	}
	slices.SortFunc(infos, func(a, b types.VolumeInfo) int { return strings.Compare(a.Name, b.Name) })
	return infos
}

// VolumeNames returns the sorted names this node can currently serve.
func (m *Manager) VolumeNames() []string {
	names := make([]string, 0, len(m.volumes))
	for name, volume := range m.volumes {
		if _, err := os.Stat(volume.disk.Path); err == nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// VolumePlacement checks catalog access and reports whether every named image
// is currently available on this node. Empty tenant means root.
func (m *Manager) VolumePlacement(key types.PoolKey, tenant string, names []string) (bool, error) {
	if err := m.validate(key); err != nil {
		return false, err
	}
	if len(names) == 0 {
		return true, nil
	}
	local := true
	for _, name := range names {
		volume, ok := m.volumes[name]
		if !ok {
			if tenant == "" {
				local = false
				continue
			}
			return false, ErrVolumeUnavailable
		}
		if !volume.allowed(tenant) {
			return false, ErrVolumeUnavailable
		}
		if _, err := os.Stat(volume.disk.Path); err != nil {
			local = false
		}
	}
	return local, nil
}

func (m *Manager) resolveVolumes(key types.PoolKey, tenant string, requested []types.Volume) ([]resolvedVolume, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	applied, err := types.ValidateVolumes(requested)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadVolume, err)
	}
	if key.Engine != types.EngineCH {
		return nil, fmt.Errorf("%w: volumes require engine ch", ErrBadVolume)
	}
	resolved := make([]resolvedVolume, 0, len(applied))
	for _, volume := range applied {
		entry, ok := m.volumes[volume.Name]
		if !ok || !entry.allowed(tenant) {
			return nil, ErrVolumeUnavailable
		}
		if volume.RW() && !entry.writable {
			return nil, fmt.Errorf("%w: volume %q is not writable", ErrBadVolume, volume.Name)
		}
		if _, statErr := os.Stat(entry.disk.Path); statErr != nil {
			return nil, fmt.Errorf("volume %q path %q: %w", volume.Name, entry.disk.Path, statErr)
		}
		// A live writer's own marker is expected; admission answers that conflict.
		if !volume.RW() && volumeDirty(entry.disk.Path) && !m.volumeHeld(volume.Name) {
			return nil, fmt.Errorf("%w: volume %q", ErrVolumeNeedsRecovery, volume.Name)
		}
		disk := entry.disk
		disk.RW = volume.RW()
		resolved = append(resolved, resolvedVolume{disk: disk, applied: volume})
	}
	return resolved, nil
}

func (m *Manager) applyVolumes(ctx context.Context, sb *types.Sandbox, volumes []resolvedVolume) error {
	if len(volumes) == 0 {
		return nil
	}
	for _, volume := range volumes {
		// Write-ahead: the marker must be durable before any guest write can be.
		if volume.disk.RW {
			if err := markVolumeDirty(volume.disk.Path); err != nil {
				return fmt.Errorf("mark volume %q dirty: %w", volume.applied.Name, err)
			}
		}
		if err := m.eng.DiskAttach(ctx, sb.VMName, volume.disk); err != nil {
			return fmt.Errorf("attach volume %q: %w", volume.applied.Name, err)
		}
		if err := m.eng.MountVolume(ctx, sb.VsockSocket, volume.applied.Name, volume.applied.Mount, volume.disk.RW); err != nil {
			return fmt.Errorf("setup volume %q: %w", volume.applied.Name, err)
		}
	}
	sb.Volumes = appliedVolumes(volumes)
	return nil
}

// teardownVolumes quiesces the guest (paths that still own a live VM, before
// it is removed) and releases the admission holds. Exactly once per claim: a
// leaked hold keeps the name unclaimable until the daemon restarts.
func (m *Manager) teardownVolumes(ctx context.Context, sb *types.Sandbox, quiesce bool) {
	if len(sb.Volumes) == 0 {
		return
	}
	if quiesce {
		m.quiesceVolumes(ctx, sb)
	}
	m.unreserveVolumes(sb.Volumes)
}

// quiesceVolumes unmounts the writable mounts in reverse order, clearing the
// marker of each image that unmounted cleanly. A failure never blocks
// teardown: the surviving marker routes the image to a recovering writer.
func (m *Manager) quiesceVolumes(ctx context.Context, sb *types.Sandbox) {
	if !slices.ContainsFunc(sb.Volumes, types.Volume.RW) {
		return
	}
	logger := log.WithFunc("pool.quiesceVolumes")
	// Cancellation-immune like removal: a caller hanging up must not skip the flush.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), volumeQuiesceTimeout)
	defer cancel()
	for _, volume := range slices.Backward(sb.Volumes) {
		if !volume.RW() {
			continue
		}
		if err := m.eng.UnmountVolume(ctx, sb.VsockSocket, volume.Mount); err != nil {
			logger.Errorf(ctx, err, "unmount volume %s of %s", volume.Name, sb.ID)
			continue
		}
		entry, ok := m.volumes[volume.Name]
		if !ok {
			continue
		}
		if err := clearVolumeDirty(entry.disk.Path); err != nil {
			logger.Errorf(ctx, err, "clear dirty marker of volume %s", volume.Name)
		}
	}
}

// reserveVolumes admits one claim's volumes; every name is checked before any
// is taken, so a refusal leaves the registry untouched. Callers hold m.mu.
func (m *Manager) reserveVolumes(volumes []types.Volume) error {
	for _, volume := range volumes {
		holders := m.volumeAdmission[volume.Name]
		if holders.writers > 0 || (volume.RW() && holders.readers > 0) {
			return fmt.Errorf("%w: volume %q", ErrVolumeBusy, volume.Name)
		}
	}
	m.adoptVolumes(volumes)
	return nil
}

// adoptVolumes counts volumes an adopted claim already holds, with no
// admission check. Callers hold m.mu.
func (m *Manager) adoptVolumes(volumes []types.Volume) {
	for _, volume := range volumes {
		holders := m.volumeAdmission[volume.Name]
		if volume.RW() {
			holders.writers++
		} else {
			holders.readers++
		}
		m.volumeAdmission[volume.Name] = holders
	}
}

// releaseVolumes drops one claim's admission holds; callers hold m.mu.
func (m *Manager) releaseVolumes(volumes []types.Volume) {
	for _, volume := range volumes {
		holders := m.volumeAdmission[volume.Name]
		if volume.RW() {
			holders.writers--
		} else {
			holders.readers--
		}
		if holders.writers < 1 && holders.readers < 1 {
			delete(m.volumeAdmission, volume.Name)
			continue
		}
		m.volumeAdmission[volume.Name] = holders
	}
}

func (m *Manager) volumeHeld(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volumeAdmission[name].writers > 0
}

func (m *Manager) unreserveVolumes(volumes []types.Volume) {
	if len(volumes) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseVolumes(volumes)
}

func appliedVolumes(volumes []resolvedVolume) []types.Volume {
	applied := make([]types.Volume, len(volumes))
	for i, volume := range volumes {
		applied[i] = volume.applied
	}
	return applied
}

func markVolumeDirty(path string) error {
	f, err := os.OpenFile(volumeDirtyPath(path), os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // sidecar of an operator-configured image path
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func clearVolumeDirty(path string) error {
	if err := os.Remove(volumeDirtyPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_ = syncDir(filepath.Dir(path)) // a lost removal re-converges on the next clean release
	return nil
}

func volumeDirty(path string) bool {
	_, err := os.Stat(volumeDirtyPath(path))
	return err == nil
}

func volumeDirtyPath(path string) string {
	return path + volumeDirtySuffix
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // parent of an operator-configured image path
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
