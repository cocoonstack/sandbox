package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	volumeBusyCode    = "volume_busy"
	volumeBusyMessage = "volume is held by a sandbox"
)

// VolumeError carries caller-safe details without storage paths or process IDs.
type VolumeError struct {
	Status  int                  `json:"-"`
	Message string               `json:"error"`
	Code    string               `json:"code"`
	Volume  string               `json:"volume,omitempty"`
	Holders []types.VolumeHolder `json:"holders,omitempty"`
}

func (e *VolumeError) Error() string { return e.Message }
func (e *VolumeError) Is(target error) bool {
	return target == ErrVolumeBusy && e.Code == volumeBusyCode
}

func volumeError(status int, code, message string) *VolumeError {
	return &VolumeError{Status: status, Code: code, Message: message}
}

func managedStorageError() *VolumeError {
	return volumeError(503, "volume_storage_unavailable", "volume storage requires recovery; retry after recovery")
}

func managedNotFound() *VolumeError { return volumeError(404, "volume_not_found", "unknown volume") }

func (m *Manager) CreateVolume(ctx context.Context, name string, req types.VolumeCreate, tenant string) (types.ManagedVolumeInfo, bool, error) {
	owner, err := m.volumeCreateOwner(ctx, name, req, tenant)
	if err != nil {
		return types.ManagedVolumeInfo{}, false, err
	}
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	m.mu.Lock()
	admission := m.volumeAdmission[name]
	m.mu.Unlock()
	r.fileMu.Lock()
	defer r.fileMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.fault != nil || r.writeFault != nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	_, managedName := r.data.Volumes[name]
	if _, ok := m.volumes[name]; ok || r.retainedCatalogName(name) || !managedName && (admission.readers > 0 || admission.writers > 0) {
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_name_conflict", "volume name is unavailable")
	}
	v, exists := r.data.Volumes[name]
	if exists && tenant != "" && v.Tenant != tenant {
		return types.ManagedVolumeInfo{}, false, managedNotFound()
	}
	size := req.SizeGiB << 30
	if exists && (r.committedCapacity(v).size != size || v.Tenant != owner) {
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_create_conflict", "volume already exists with different size or owner")
	}
	if !exists {
		v = managedVolume{ID: volumeID("vol_" + randHex(16)), Name: name, Tenant: owner, Node: r.node, Size: size, State: volumeCreating, CreatedAt: time.Now().UTC()}
		r.data.Volumes[name] = v
		if err := r.commitAndRelock(); err != nil {
			return types.ManagedVolumeInfo{}, false, err
		}
	}
	switch v.State {
	case volumeCreating:
		r.mu.Unlock()
		err := r.completeCreate(v)
		r.mu.Lock()
		if err != nil {
			return types.ManagedVolumeInfo{}, false, err
		}
		v = r.data.Volumes[name]
	case volumeGrowing:
		return r.info(v), false, nil
	case volumeReady:
		r.mu.Unlock()
		err := r.verify(v)
		r.mu.Lock()
		if err != nil {
			if errors.Is(err, errBackingMismatch) || errors.Is(err, os.ErrNotExist) {
				return types.ManagedVolumeInfo{}, false, r.markRecovery(v)
			}
			return types.ManagedVolumeInfo{}, false, managedStorageError()
		}
	case volumeDeleting:
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_deleting", "volume deletion is pending")
	default:
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	return r.info(v), !exists, nil
}

func (r *volumeRegistry) completeCreate(v managedVolume) error {
	if err := r.verifyDirectories(); err != nil {
		return managedStorageError()
	}
	path := r.path(v.ID)
	f, err := openPrivateFile(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return volumeError(503, "volume_create_incomplete", "incomplete volume allocation requires operator quarantine before retry")
		}
		return managedStorageError()
	}
	if err = f.Truncate(v.Size); err == nil {
		err = f.Sync()
	}
	st, statErr := f.Stat()
	closeErr := f.Close()
	if err == nil {
		err = errors.Join(statErr, closeErr, syncDir(r.dir+"/disks"))
	}
	if err != nil {
		return managedStorageError()
	}
	raw := st.Sys().(*syscall.Stat_t)
	v.Backing = backingIdentity{Device: backingDeviceID(raw.Dev), Inode: raw.Ino, Size: st.Size()}
	v.State = volumeReady
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.Volumes[v.Name] = v
	return r.commitAndRelock()
}

func (r *volumeRegistry) markRecovery(v managedVolume) error {
	current, ok := r.data.Volumes[v.Name]
	if !ok || current.ID != v.ID || current.State != v.State || current.Size != v.Size ||
		current.Backing != v.Backing || current.CapacityGeneration != v.CapacityGeneration || current.Growth != v.Growth {
		return managedStorageError()
	}
	if r.sleepOwnsVolume(v.ID) || v.Growth.fenced() {
		return managedStorageError()
	}
	v.State = volumeRecovery
	r.data.Volumes[v.Name] = v
	if err := r.commitAndRelock(); err != nil {
		return err
	}
	return managedStorageError()
}

func (m *Manager) GetVolume(name, tenant string) (types.ManagedVolumeInfo, error) {
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.fault != nil || r.writeFault != nil {
		return types.ManagedVolumeInfo{}, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok || (tenant != "" && v.Tenant != tenant) {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	info := r.info(v)
	r.mu.Unlock()
	defer r.mu.Lock()
	if v.State == volumeGrowing {
		actual, err := inspectBacking(r.path(v.ID))
		if err != nil || r.verifyDirectories() != nil || actual.Device != v.Backing.Device || actual.Inode != v.Backing.Inode || (actual.Size != v.Size && actual.Size != v.Growth.Runtime.TargetBytes) {
			return types.ManagedVolumeInfo{}, managedStorageError()
		}
	}
	if v.State == volumeReady {
		if err := r.verify(v); err != nil {
			if !errors.Is(err, errBackingMismatch) && !errors.Is(err, os.ErrNotExist) {
				return types.ManagedVolumeInfo{}, managedStorageError()
			}
			info.State = string(volumeRecovery)
			info.Available = false
		}
	}
	return info, nil
}

func (m *Manager) DeleteVolume(ctx context.Context, name, expected, tenant string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r := m.managed
	if r == nil {
		return managedNotFound()
	}
	r.fileMu.Lock()
	defer r.fileMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.fault != nil || r.writeFault != nil {
		return managedStorageError()
	}
	if expected == "" {
		return volumeError(428, "volume_precondition_required", "If-Match must identify the volume")
	}
	id := volumeID(expected)
	if t, ok := r.data.Deleted[id]; ok && t.Name == name && (tenant == "" || tenant == t.Tenant) {
		return nil
	}
	v, ok := r.data.Volumes[name]
	if !ok || (tenant != "" && v.Tenant != tenant) {
		return managedNotFound()
	}
	if v.ID != id {
		return volumeError(412, "volume_precondition_failed", "volume identity does not match")
	}
	if v.Growth.fenced() {
		return growBusy()
	}
	if holders := r.holders(v.ID); len(holders) > 0 {
		return &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: name, Holders: holders}
	}
	r.mu.Unlock()
	dirErr := r.verifyDirectories()
	path := r.path(id)
	actual, err := inspectBacking(path)
	r.mu.Lock()
	if dirErr != nil {
		return managedStorageError()
	}
	if holders := r.holders(v.ID); len(holders) > 0 {
		return &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: name, Holders: holders}
	}
	missing := errors.Is(err, os.ErrNotExist)
	if missing && v.State != volumeCreating && v.State != volumeDeleting {
		return r.markRecovery(v)
	}
	if !missing && (err != nil || v.Backing.Inode == 0 || !growthBackingMatches(v, actual)) {
		return volumeError(503, "volume_backing_mismatch", "volume backing requires operator recovery")
	}
	if v.State != volumeDeleting {
		v.State = volumeDeleting
		r.data.Volumes[name] = v
		if err := r.commitAndRelock(); err != nil {
			return err
		}
	}
	r.mu.Unlock()
	removeErr := removeManagedBacking(path, r.dir+"/disks", missing)
	r.mu.Lock()
	if removeErr != nil {
		return managedStorageError()
	}
	r.data.Deleted[id] = volumeTombstone{ID: id, Name: name, Tenant: v.Tenant}
	delete(r.data.Volumes, name)
	return r.commitAndRelock()
}

func (r *volumeRegistry) holders(id volumeID) []types.VolumeHolder {
	holders := []types.VolumeHolder{}
	for _, h := range r.data.Holds {
		for _, ref := range h.Refs {
			if ref.ID == id {
				mode := "ro"
				if ref.Volume.RW() {
					mode = "rw"
				}
				holders = append(holders, types.VolumeHolder{SandboxID: h.SandboxID, Mode: mode, State: string(h.State)})
			}
		}
	}
	slices.SortFunc(holders, func(a, b types.VolumeHolder) int { return strings.Compare(a.SandboxID, b.SandboxID) })
	return holders
}

type volumeCapacity struct {
	size              int64
	generation        uint64
	completionPending bool
}

func (r *volumeRegistry) committedCapacity(v managedVolume) volumeCapacity {
	capacity := volumeCapacity{size: v.Size, generation: v.CapacityGeneration}
	g := v.Growth
	if g == nil || g.State != growthCompleted {
		return capacity
	}
	if run := r.activeGrowth(v.ID); run != nil && g.Request == run.request && run.completionSeq > r.written {
		return volumeCapacity{size: run.volume.Size, generation: run.volume.CapacityGeneration, completionPending: true}
	}
	return capacity
}

func (r *volumeRegistry) info(v managedVolume) types.ManagedVolumeInfo {
	capacity := r.committedCapacity(v)
	info := types.ManagedVolumeInfo{CapacityGeneration: capacity.generation, Name: v.Name, DefaultMount: types.DefaultVolumeMount(v.Name), SizeBytes: capacity.size, Available: v.State == volumeReady, Nodes: 1, Writable: true, Kind: "managed", ID: string(v.ID), Tenant: v.Tenant, Node: v.Node, State: string(v.State), CreatedAt: v.CreatedAt, Holders: r.holders(v.ID)}
	if g := v.Growth; g != nil {
		info.Growth = &types.VolumeGrowth{OperationID: g.Request.OperationID, ExpectedGeneration: g.Request.ExpectedGeneration, TargetBytes: g.Runtime.TargetBytes, State: g.State, RuntimeOutcome: g.Outcome, GuestSizeBytes: g.GuestSizeBytes, FilesystemStatus: "not_managed"}
		if capacity.completionPending {
			info.State, info.Available, info.Growth.State = string(volumeGrowing), false, growthPending
		}
	}
	return info
}

// ManagedVolumeStatus lets listing fail clearly rather than presenting a corrupt
// registry as an empty managed catalog. A disabled store has no catalog cost.
func (m *Manager) ManagedVolumeStatus() error {
	if m.managed == nil {
		return nil
	}
	m.managed.mu.Lock()
	defer m.managed.mu.Unlock()
	if m.managed.fault != nil || m.managed.writeFault != nil {
		return managedStorageError()
	}
	return nil
}

func (m *Manager) managedInfos(tenant string) []types.VolumeInfo {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed || r.fault != nil || r.writeFault != nil {
		r.mu.Unlock()
		return nil
	}
	var volumes []managedVolume
	for _, v := range r.data.Volumes {
		if tenant == "" || tenant == v.Tenant {
			volumes = append(volumes, v)
		}
	}
	infos := make([]types.VolumeInfo, len(volumes))
	for i, v := range volumes {
		infos[i] = r.info(v).VolumeInfo
	}
	r.mu.Unlock()
	for i, v := range volumes {
		if v.State == volumeReady && r.verify(v) != nil {
			infos[i].State = string(volumeRecovery)
			infos[i].Available = false
		}
	}
	return infos
}

func (m *Manager) lookupManaged(name string) (catalogVolume, bool, error) {
	r := m.managed
	if r == nil {
		return catalogVolume{}, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.fault != nil || r.writeFault != nil {
		return catalogVolume{}, false, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok {
		return catalogVolume{}, false, nil
	}
	entry := catalogVolume{disk: engine.VolumeSpec{Name: name, Path: r.path(v.ID), DirectIO: types.DirectIOOff}, tenants: []string{v.Tenant}, writable: true, managedID: v.ID}
	if v.State == volumeGrowing {
		return entry, true, &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: name, Holders: r.holders(v.ID)}
	}
	if v.State != volumeReady {
		return entry, true, managedStorageError()
	}
	r.mu.Unlock()
	err := r.verify(v)
	r.mu.Lock()
	if err != nil {
		return entry, true, managedStorageError()
	}
	return entry, true, nil
}

func (m *Manager) managedHold(id string) (volumeHold, bool) {
	r := m.managed
	if r == nil {
		return volumeHold{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	h.Refs = slices.Clone(h.Refs)
	return h, ok
}

func (m *Manager) managedHoldByVM(name string) (volumeHold, bool) {
	r := m.managed
	if r == nil {
		return volumeHold{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.data.Holds {
		if h.VMName == name {
			h.Refs = slices.Clone(h.Refs)
			return h, true
		}
	}
	return volumeHold{}, false
}

func (m *Manager) retireManaged(id string) error {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	if !ok {
		return nil
	}
	if r.fault != nil {
		return managedStorageError()
	}
	if r.growthOwnsHold(id) {
		return growBusy()
	}
	if h.State == holdRetiring {
		if r.writeFault != nil {
			return r.commitAndRelock()
		}
		return nil
	}
	h.State = holdRetiring
	r.data.Holds[id] = h
	return r.commitAndRelock()
}

func (m *Manager) managedRefPath(id, name string) (string, bool) {
	h, ok := m.managedHold(id)
	if !ok {
		return "", false
	}
	for _, ref := range h.Refs {
		if ref.Volume.Name == name {
			return ref.Path, true
		}
	}
	return "", false
}

func (m *Manager) ManagedVolumesEnabled() bool { return m.managed != nil }

func (m *Manager) managedCatalogAlias(path string) bool {
	r := m.managed
	if r == nil {
		return false
	}
	clean := filepath.Clean(path)
	if canonical, err := filepath.EvalSymlinks(path); err == nil {
		clean = canonical
	}
	if clean == r.dir || strings.HasPrefix(clean, r.dir+string(filepath.Separator)) {
		return true
	}
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	identity := info.Sys().(*syscall.Stat_t)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.aliases[backingIdentity{Device: backingDeviceID(identity.Dev), Inode: identity.Ino}]
}

func growthBackingMatches(v managedVolume, actual backingIdentity) bool {
	if actual == v.Backing {
		return true
	}
	return v.Growth.settled() && actual.Device == v.Backing.Device && actual.Inode == v.Backing.Inode && actual.Size == v.Growth.Runtime.TargetBytes
}

func (r *volumeRegistry) retainedCatalogName(name string) bool {
	for _, h := range r.data.Holds {
		for _, ref := range h.Refs {
			if ref.ID == "" && ref.Volume.Name == name {
				return true
			}
		}
	}
	return false
}

func removeManagedBacking(path, dir string, missing bool) error {
	if !missing {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return clearVolumeDirty(path)
}

func (m *Manager) volumeCreateOwner(ctx context.Context, name string, req types.VolumeCreate, tenant string) (string, error) {
	if !types.ValidVolumeName(name) || req.SizeGiB < 1 || req.SizeGiB > 1024 {
		return "", volumeError(400, "invalid_volume", "invalid volume name or size_gib (1 through 1024)")
	}
	if tenant != "" && req.Tenant != "" && tenant != req.Tenant {
		return "", volumeError(403, "volume_owner_forbidden", "tenant cannot select another volume owner")
	}
	owner := req.Tenant
	if tenant != "" {
		owner = tenant
	}
	if !types.NameRe.MatchString(owner) {
		return "", volumeError(400, "invalid_volume_owner", "an existing tenant owner is required")
	}
	if _, ok, err := m.tenants.Lookup(ctx, owner); err != nil {
		return "", ErrTenantStoreDown
	} else if !ok {
		return "", volumeError(400, "invalid_volume_owner", "an existing tenant owner is required")
	}
	return owner, nil
}
