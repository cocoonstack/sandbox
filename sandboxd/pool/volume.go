package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/projecteru2/core/log"
	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// Sidecar, not data_dir: the marker travels with the image and survives a data_dir wipe
const volumeDirtySuffix = ".dirty"

// volumeHolders is one name's live admission state: a writer excludes every other claim.
type volumeHolders struct {
	writers int
	readers int
}

type resolvedVolume struct {
	entry   *catalogVolume
	disk    engine.VolumeSpec
	applied types.Volume
}

// volumeTeardown captures marker paths at quiesce time; a later config can re-point the entry.
type volumeTeardown struct {
	holds  []types.Volume
	clears []string
}

type catalogVolume struct {
	runtime  bool
	disk     engine.VolumeSpec
	tenants  []string
	writable bool
}

func (v *catalogVolume) allowed(tenant string) bool {
	return tenant == "" || len(v.tenants) == 0 || slices.Contains(v.tenants, tenant)
}

// Volumes reports the caller-visible fleet catalog; an empty tenant means root.
func (m *Manager) Volumes(tenant string, holders map[string]int) []types.VolumeInfo {
	catalog := m.catalogSnapshot()
	infos := make([]types.VolumeInfo, 0, len(catalog))
	for name, volume := range catalog {
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
	// Root can see peer-only names; a tenant needs local catalog metadata to apply the access list
	if tenant == "" {
		for name, nodes := range holders {
			if _, ok := catalog[name]; !ok {
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
	catalog := m.catalogSnapshot()
	names := make([]string, 0, len(catalog))
	for name, volume := range catalog {
		if _, err := os.Stat(volume.disk.Path); err == nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// VolumePlacement checks catalog access and reports whether every named image is local.
func (m *Manager) VolumePlacement(key types.PoolKey, tenant string, names []string) (bool, error) {
	if err := m.validate(key); err != nil {
		return false, err
	}
	if len(names) == 0 {
		return true, nil
	}
	local := true
	catalog := m.catalogSnapshot()
	for _, name := range names {
		volume, ok := catalog[name]
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

func (m *Manager) resolveVolumes(ctx context.Context, tenant string, requested []types.Volume) ([]resolvedVolume, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	applied, err := types.ValidateVolumes(requested)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	catalog := m.catalogSnapshot()
	resolved := make([]resolvedVolume, 0, len(applied))
	for _, volume := range applied {
		entry := catalog[volume.Name]
		if entry == nil || !entry.allowed(tenant) {
			return nil, ErrVolumeUnavailable
		}
		resolvedEntry, resolveErr := m.resolveCatalogEntry(ctx, volume, entry)
		if resolveErr != nil {
			return nil, resolveErr
		}
		resolved = append(resolved, resolvedEntry)
	}
	return resolved, nil
}

func (m *Manager) resolveCatalogEntry(ctx context.Context, volume types.Volume, entry *catalogVolume) (resolvedVolume, error) {
	if entry == nil {
		return resolvedVolume{}, ErrVolumeUnavailable
	}
	if volume.RW() && !entry.writable {
		return resolvedVolume{}, fmt.Errorf("%w: volume %q is not writable", ErrBadVolume, volume.Name)
	}
	if _, statErr := os.Stat(entry.disk.Path); statErr != nil {
		// The operator's path stays server-side; the refusal matches the unknown one
		log.WithFunc("pool.resolveCatalogEntry").Warnf(ctx, "stat volume %q image %s: %v", volume.Name, entry.disk.Path, statErr)
		return resolvedVolume{}, ErrVolumeUnavailable
	}
	disk := entry.disk
	disk.RW = volume.RW()
	return resolvedVolume{entry: entry, disk: disk, applied: volume}, nil
}

// confirmVolumesClean refuses a read-only claim of a dirty image; run after holds are taken.
func (m *Manager) confirmVolumesClean(volumes []resolvedVolume) error {
	for _, volume := range volumes {
		if !volume.applied.RW() && volumeDirty(volume.disk.Path) {
			return fmt.Errorf("%w: volume %q", ErrVolumeNeedsRecovery, volume.applied.Name)
		}
	}
	return nil
}

// applyVolumes brings the resolved set up concurrently; cocoon serializes the attach per VM.
func (m *Manager) applyVolumes(ctx context.Context, sb *types.Sandbox, volumes []resolvedVolume, applied []types.Volume) error {
	if len(volumes) == 0 {
		return nil
	}
	group, groupCtx := errgroup.WithContext(ctx)
	for _, volume := range volumes {
		group.Go(func() error { return m.applyVolume(groupCtx, sb, volume, false) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	sb.Volumes, sb.VolumesWant = applied, applied
	return nil
}

// applyVolume keeps one volume's steps strictly ordered; siblings overlap freely.
func (m *Manager) applyVolume(ctx context.Context, sb *types.Sandbox, volume resolvedVolume, live bool) error {
	// Attach-only mounts nothing, so there is no umount to verify and no marker.
	attachOnly := volume.applied.AttachOnly
	// Write-ahead: the marker must be durable before any guest write can be.
	if volume.disk.RW && !attachOnly {
		if err := markVolumeDirty(volume.disk.Path); err != nil {
			return fmt.Errorf("mark volume %q dirty: %w", volume.applied.Name, err)
		}
	}
	if err := m.eng.DiskAttach(ctx, sb.VMName, volume.disk); err != nil {
		return fmt.Errorf("attach volume %q: %w", volume.applied.Name, withCtxErr(ctx, err))
	}
	if attachOnly {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mount := m.eng.MountVolume
	if live {
		mount = m.eng.MountLiveVolume
	}
	if err := mount(ctx, sb.VsockSocket, volume.applied.Name, volume.applied.Mount, volume.disk.RW); err != nil {
		return fmt.Errorf("setup volume %q: %w", volume.applied.Name, withCtxErr(ctx, err))
	}
	return nil
}

// quiesceVolumes unmounts a claim's writable mounts in reverse; an unclean unmount keeps the marker.
func (m *Manager) quiesceVolumes(ctx context.Context, sb *types.Sandbox) volumeTeardown {
	td := volumeTeardown{holds: heldVolumes(sb)}
	if !slices.ContainsFunc(sb.Volumes, needsUnmount) {
		return td
	}
	catalog := m.catalogSnapshot()
	logger := log.WithFunc("pool.quiesceVolumes")
	// Cancellation-immune like removal: a caller hanging up must not skip the flush.
	ctx = context.WithoutCancel(ctx)
	stuck := false
	for _, volume := range slices.Backward(sb.Volumes) {
		if !needsUnmount(volume) {
			continue // attach-only: no mount to bring down, VM removal closes the device
		}
		unmountCtx, cancel := context.WithTimeout(ctx, engine.VolumeMutationTimeout)
		clean, err := m.eng.UnmountVolume(unmountCtx, sb.VsockSocket, volume.Mount)
		wedged := unmountCtx.Err() != nil
		cancel()
		if err != nil {
			logger.Errorf(ctx, err, "unmount volume %s of %s", volume.Name, sb.ID)
			stuck = true
			if wedged {
				break
			}
			continue
		}
		if entry, ok := catalog[volume.Name]; ok && clean {
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

// finishVolumeTeardown runs only once the VM is gone, when its image lock is finally released.
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

func (m *Manager) adoptVolumes(volumes []types.Volume) {
	for _, volume := range volumes {
		holders := m.catalog.holders[volume.Name]
		if volume.RW() {
			holders.writers++
		} else {
			holders.readers++
		}
		m.catalog.holders[volume.Name] = holders
	}
}

func (m *Manager) releaseVolumes(volumes []types.Volume) {
	for _, volume := range volumes {
		holders := m.catalog.holders[volume.Name]
		if volume.RW() {
			holders.writers--
		} else {
			holders.readers--
		}
		if holders.writers < 1 && holders.readers < 1 {
			delete(m.catalog.holders, volume.Name)
			continue
		}
		m.catalog.holders[volume.Name] = holders
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

func (m *Manager) catalogSnapshot() map[string]*catalogVolume {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.catalog.entries
}

func (m *Manager) checkResolvedLocked(volumes []resolvedVolume) error {
	for _, resolved := range volumes {
		volume := resolved.applied
		if m.catalog.entries[volume.Name] != resolved.entry {
			return ErrVolumeUnavailable
		}
		holders := m.catalog.holders[volume.Name]
		if holders.writers > 0 || (volume.RW() && holders.readers > 0) {
			return fmt.Errorf("%w: volume %q", ErrVolumeBusy, volume.Name)
		}
	}
	return nil
}

func needsUnmount(volume types.Volume) bool {
	return volume.RW() && !volume.AttachOnly
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
	return utils.SyncDir(filepath.Dir(path))
}

func clearVolumeDirty(path string) error {
	if err := os.Remove(volumeDirtyPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_ = utils.SyncDir(filepath.Dir(path)) // a lost removal re-converges on the next clean release
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

func withCtxErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return err
}
