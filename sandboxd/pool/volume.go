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
	// sidecar, not data_dir: the marker travels with the image and survives a data_dir wipe
	volumeDirtySuffix = ".dirty"
	// caps the per-mount budget below, so teardown can never hang for long.
	volumeQuiesceMax = 10 * time.Second
)

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
	// root can see peer-only names; a tenant needs local catalog metadata to apply the access list
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
		// the operator's path stays server-side; the refusal matches the unknown one
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
	// attach-only mounts nothing, so there is no umount to verify and no marker.
	attachOnly := volume.applied.AttachOnly
	// write-ahead: the marker must be durable before any guest write can be.
	if volume.disk.RW && !attachOnly {
		if err := markVolumeDirty(volume.disk.Path); err != nil {
			return fmt.Errorf("mark volume %q dirty: %w", volume.applied.Name, err)
		}
	}
	if err := m.eng.DiskAttach(ctx, sb.VMName, volume.disk); err != nil {
		return fmt.Errorf("attach volume %q: %w", volume.applied.Name, err)
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
		return fmt.Errorf("setup volume %q: %w", volume.applied.Name, err)
	}
	return nil
}

// quiesceVolumes unmounts a claim's writable mounts in reverse; a failed unmount keeps the marker.
func (m *Manager) quiesceVolumes(ctx context.Context, sb *types.Sandbox) volumeTeardown {
	td := volumeTeardown{holds: heldVolumes(sb)}
	mounts := writableMounts(sb.Volumes)
	if mounts == 0 {
		return td
	}
	catalog := m.catalogSnapshot()
	logger := log.WithFunc("pool.quiesceVolumes")
	// cancellation-immune like removal: a caller hanging up must not skip the flush.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quiesceBudget(mounts))
	defer cancel()
	stuck := false
	for _, volume := range slices.Backward(sb.Volumes) {
		if !needsUnmount(volume) {
			continue // attach-only: no mount to bring down, VM removal closes the device
		}
		if err := m.eng.UnmountVolume(ctx, sb.VsockSocket, volume.Mount); err != nil {
			logger.Errorf(ctx, err, "unmount volume %s of %s", volume.Name, sb.ID)
			stuck = true
			continue
		}
		if entry, ok := catalog[volume.Name]; ok {
			td.clears = append(td.clears, entry.disk.Path)
		}
	}
	if stuck {
		// the unmounts may have spent the whole budget; the fallback flush gets its own
		syncCtx, cancelSync := context.WithTimeout(context.WithoutCancel(ctx), engine.VolumeCallTimeout)
		defer cancelSync()
		if err := m.eng.SyncGuest(syncCtx, sb.VsockSocket); err != nil {
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

// adoptVolumes counts volumes an adopted claim already holds; callers hold m.mu.
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

// releaseVolumes drops one claim's admission holds; callers hold m.mu.
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

// quiesceBudget bounds one quiesce: a slice per writable mount plus one for the sync fallback.
func quiesceBudget(mounts int) time.Duration {
	return min(time.Duration(mounts+1)*engine.VolumeCallTimeout, volumeQuiesceMax)
}

func writableMounts(volumes []types.Volume) int {
	mounts := 0
	for _, volume := range volumes {
		if needsUnmount(volume) {
			mounts++
		}
	}
	return mounts
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
