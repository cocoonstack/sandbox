package pool

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type catalogVolume struct {
	disk    engine.VolumeSpec
	tenants []string
}

func (v catalogVolume) allowed(tenant string) bool {
	return tenant == "" || len(v.tenants) == 0 || slices.Contains(v.tenants, tenant)
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
		info := types.VolumeInfo{Name: name, DefaultMount: types.DefaultVolumeMount(name), Nodes: holders[name]}
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
		if _, statErr := os.Stat(entry.disk.Path); statErr != nil {
			return nil, fmt.Errorf("volume %q path %q: %w", volume.Name, entry.disk.Path, statErr)
		}
		resolved = append(resolved, resolvedVolume{disk: entry.disk, applied: volume})
	}
	return resolved, nil
}

func (m *Manager) applyVolumes(ctx context.Context, sb *types.Sandbox, volumes []resolvedVolume) error {
	for _, volume := range volumes {
		if err := m.eng.DiskAttach(ctx, sb.VMName, volume.disk); err != nil {
			return fmt.Errorf("attach volume %q: %w", volume.applied.Name, err)
		}
		if err := m.eng.MountVolume(ctx, sb.VsockSocket, volume.applied.Name, volume.applied.Mount); err != nil {
			return fmt.Errorf("setup volume %q: %w", volume.applied.Name, err)
		}
	}
	if len(volumes) > 0 {
		sb.Volumes = make([]types.Volume, len(volumes))
		for i, volume := range volumes {
			sb.Volumes[i] = volume.applied
		}
	}
	return nil
}
