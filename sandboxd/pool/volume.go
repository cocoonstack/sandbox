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
	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// Sidecar, not data_dir: the marker travels with the image and survives a
	// data_dir wipe.
	volumeDirtySuffix = ".dirty"
	// Caps the per-mount budget below, so teardown can never hang for long.
	volumeQuiesceMax = 10 * time.Second
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

// volumeTeardown is what a quiesced claim leaves for after its VM is confirmed
// gone: the admission holds to drop, and the marker paths — resolved through
// the catalog while the claim still existed — of the mounts that came down
// cleanly.
type volumeTeardown struct {
	holds  []types.Volume
	clears []string
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
		disk := entry.disk
		disk.RW = volume.RW()
		resolved = append(resolved, resolvedVolume{disk: disk, applied: volume})
	}
	return resolved, nil
}

// confirmVolumesClean refuses a read-only claim of an image a writer left
// dirty. It is the only marker check, and runs once the holds are taken: no
// writer can be admitted alongside, so the marker is stable and it can only
// mean a crashed writer — a live one is answered by admission as busy.
func (m *Manager) confirmVolumesClean(volumes []resolvedVolume) error {
	for _, volume := range volumes {
		if !volume.applied.RW() && volumeDirty(volume.disk.Path) {
			return fmt.Errorf("%w: volume %q", ErrVolumeNeedsRecovery, volume.applied.Name)
		}
	}
	return nil
}

// applyVolumes brings the resolved set up concurrently: cocoon serializes the
// attach per VM, so what overlaps is the CLI spawns, settle waits and guest
// mounts. applied is the request-shaped set, recorded once every mount is up.
func (m *Manager) applyVolumes(ctx context.Context, sb *types.Sandbox, volumes []resolvedVolume, applied []types.Volume) error {
	switch len(volumes) {
	case 0:
		return nil
	case 1:
		if err := m.applyVolume(ctx, sb, volumes[0]); err != nil {
			return err
		}
	default:
		group, groupCtx := errgroup.WithContext(ctx)
		for _, volume := range volumes {
			group.Go(func() error { return m.applyVolume(groupCtx, sb, volume) })
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}
	sb.Volumes = applied
	return nil
}

// applyVolume keeps one volume's steps strictly ordered; siblings overlap freely.
func (m *Manager) applyVolume(ctx context.Context, sb *types.Sandbox, volume resolvedVolume) error {
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
	return nil
}

// quiesceVolumes unmounts a claim's writable mounts in reverse order and
// returns the teardown its VM removal must finish. A failed unmount never
// blocks teardown: the image keeps its marker and waits for a recovering
// writer. Runs while the guest is still live, before the VM is removed.
func (m *Manager) quiesceVolumes(ctx context.Context, sb *types.Sandbox) volumeTeardown {
	td := volumeTeardown{holds: sb.Volumes}
	mounts := len(types.VolumeRWNames(sb.Volumes))
	if mounts == 0 {
		return td
	}
	logger := log.WithFunc("pool.quiesceVolumes")
	// Cancellation-immune like removal: a caller hanging up must not skip the flush.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quiesceBudget(mounts))
	defer cancel()
	stuck := false
	for _, volume := range slices.Backward(sb.Volumes) {
		if !volume.RW() {
			continue
		}
		if err := m.eng.UnmountVolume(ctx, sb.VsockSocket, volume.Mount); err != nil {
			logger.Errorf(ctx, err, "unmount volume %s of %s", volume.Name, sb.ID)
			stuck = true
			continue
		}
		if entry, ok := m.volumes[volume.Name]; ok {
			td.clears = append(td.clears, entry.disk.Path)
		}
	}
	if stuck {
		if err := m.eng.SyncGuest(ctx, sb.VsockSocket); err != nil {
			logger.Errorf(ctx, err, "sync guest of %s", sb.ID)
		}
	}
	return td
}

// finishVolumeTeardown completes teardown once the VM is confirmed gone: only
// then is the hypervisor's image lock released, so an earlier marker clear or
// hold release would hand the name to a claim the still-live writer blocks.
func (m *Manager) finishVolumeTeardown(ctx context.Context, td volumeTeardown) {
	if len(td.holds) == 0 {
		return
	}
	logger := log.WithFunc("pool.finishVolumeTeardown")
	for _, path := range td.clears {
		if err := clearVolumeDirty(path); err != nil {
			logger.Errorf(ctx, err, "clear dirty marker %s", path)
		}
	}
	m.unreserveVolumes(td.holds)
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

func (m *Manager) unreserveVolumes(volumes []types.Volume) {
	if len(volumes) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseVolumes(volumes)
}

// quiesceBudget bounds one quiesce: a slice per writable mount plus one for the
// sync fallback, so a wedged multi-volume guest still gets every umount tried.
func quiesceBudget(mounts int) time.Duration {
	return min(time.Duration(mounts+1)*engine.VolumeCallTimeout, volumeQuiesceMax)
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

// volumeDirty fails closed: an unreadable marker must not read as a clean image.
func volumeDirty(path string) bool {
	_, err := os.Stat(volumeDirtyPath(path))
	return !errors.Is(err, os.ErrNotExist)
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
