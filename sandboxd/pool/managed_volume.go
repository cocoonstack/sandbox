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

// VolumeError carries only caller-safe details. Storage paths and process IDs
// remain in the registry and never become API diagnostics.
type VolumeError struct {
	Status  int                  `json:"-"`
	Message string               `json:"error"`
	Code    string               `json:"code"`
	Volume  string               `json:"volume,omitempty"`
	Holders []types.VolumeHolder `json:"holders,omitempty"`
}

func (e *VolumeError) Error() string { return e.Message }
func (e *VolumeError) Is(target error) bool {
	return target == ErrVolumeBusy && e.Code == "volume_busy"
}
func volumeError(status int, code, message string) *VolumeError {
	return &VolumeError{Status: status, Code: code, Message: message}
}
func managedStorageError() *VolumeError {
	return volumeError(503, "volume_storage_unavailable", "volume storage requires recovery; retry after recovery")
}
func managedNotFound() *VolumeError { return volumeError(404, "volume_not_found", "unknown volume") }

func (m *Manager) CreateVolume(ctx context.Context, name string, req types.VolumeCreate, tenant string) (types.ManagedVolumeInfo, bool, error) {
	if !types.ValidVolumeName(name) || req.SizeGiB < 1 || req.SizeGiB > 1024 {
		return types.ManagedVolumeInfo{}, false, volumeError(400, "invalid_volume", "invalid volume name or size_gib (1 through 1024)")
	}
	if tenant != "" && req.Tenant != "" && tenant != req.Tenant {
		return types.ManagedVolumeInfo{}, false, volumeError(403, "volume_owner_forbidden", "tenant cannot select another volume owner")
	}
	owner := req.Tenant
	if tenant != "" {
		owner = tenant
	}
	if !types.NameRe.MatchString(owner) {
		return types.ManagedVolumeInfo{}, false, volumeError(400, "invalid_volume_owner", "an existing tenant owner is required")
	}
	if _, ok, err := m.tenants.Lookup(ctx, owner); err != nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	} else if !ok {
		return types.ManagedVolumeInfo{}, false, volumeError(400, "invalid_volume_owner", "an existing tenant owner is required")
	}
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fault != nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	if _, ok := m.volumes[name]; ok {
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_name_conflict", "volume name is unavailable")
	}
	v, exists := r.data.Volumes[name]
	if exists && tenant != "" && v.Tenant != tenant {
		return types.ManagedVolumeInfo{}, false, managedNotFound()
	}
	size := req.SizeGiB << 30
	if exists && (v.Size != size || v.Tenant != owner) {
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_create_conflict", "volume already exists with different size or owner")
	}
	if !exists {
		v = managedVolume{ID: volumeID("vol_" + randHex(16)), Name: name, Tenant: owner, Node: r.node, Size: size, State: volumeCreating, CreatedAt: time.Now().UTC()}
		r.data.Volumes[name] = v
		if err := r.commit(); err != nil {
			return types.ManagedVolumeInfo{}, false, err
		}
	}
	switch v.State {
	case volumeCreating:
		if err := r.completeCreate(v); err != nil {
			return types.ManagedVolumeInfo{}, false, err
		}
		v = r.data.Volumes[name]
	case volumeReady:
		if err := r.verify(v); err != nil {
			return types.ManagedVolumeInfo{}, false, r.markRecovery(v)
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
	v.Backing = backingIdentity{Device: uint64(raw.Dev), Inode: raw.Ino, Size: st.Size()}
	v.State = volumeReady
	r.data.Volumes[v.Name] = v
	return r.commit()
}

func (r *volumeRegistry) markRecovery(v managedVolume) error {
	v.State = volumeRecovery
	r.data.Volumes[v.Name] = v
	if err := r.commit(); err != nil {
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
	if r.fault != nil {
		return types.ManagedVolumeInfo{}, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok || (tenant != "" && v.Tenant != tenant) {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	if v.State == volumeReady && r.verify(v) != nil {
		if err := r.markRecovery(v); r.fault != nil {
			return types.ManagedVolumeInfo{}, err
		}
		v = r.data.Volumes[name]
	}
	return r.info(v), nil
}

func (m *Manager) DeleteVolume(ctx context.Context, name, expected, tenant string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r := m.managed
	if r == nil {
		return managedNotFound()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fault != nil {
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
	if holders := r.holders(v.ID); len(holders) > 0 {
		return &VolumeError{Status: 409, Code: "volume_busy", Message: "volume is held by a sandbox", Volume: name, Holders: holders}
	}
	if err := r.verifyDirectories(); err != nil {
		return managedStorageError()
	}
	path := r.path(id)
	actual, err := inspectBacking(path)
	missing := errors.Is(err, os.ErrNotExist)
	if missing && v.State != volumeCreating && v.State != volumeDeleting {
		return r.markRecovery(v)
	}
	if !missing && (err != nil || v.Backing.Inode == 0 || actual != v.Backing) {
		return volumeError(503, "volume_backing_mismatch", "volume backing requires operator recovery")
	}
	if v.State != volumeDeleting {
		v.State = volumeDeleting
		r.data.Volumes[name] = v
		if err := r.commit(); err != nil {
			return err
		}
	}
	if !missing {
		if err := os.Remove(path); err != nil {
			return managedStorageError()
		}
	}
	if err := syncDir(r.dir + "/disks"); err != nil {
		return managedStorageError()
	}
	if err := clearVolumeDirty(path); err != nil {
		return managedStorageError()
	}
	r.data.Deleted[id] = volumeTombstone{ID: id, Name: name, Tenant: v.Tenant}
	delete(r.data.Volumes, name)
	return r.commit()
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
func (r *volumeRegistry) info(v managedVolume) types.ManagedVolumeInfo {
	return types.ManagedVolumeInfo{VolumeInfo: types.VolumeInfo{Name: v.Name, DefaultMount: types.DefaultVolumeMount(v.Name), SizeBytes: v.Size, Available: v.State == volumeReady, Nodes: 1, Writable: true, Kind: "managed", ID: string(v.ID), Tenant: v.Tenant, Node: v.Node, State: string(v.State), CreatedAt: v.CreatedAt}, Holders: r.holders(v.ID)}
}

// ManagedVolumeStatus lets listing fail clearly rather than presenting a corrupt
// registry as an empty managed catalog. A disabled store has no catalog cost.
func (m *Manager) ManagedVolumeStatus() error {
	if m.managed == nil {
		return nil
	}
	m.managed.mu.Lock()
	defer m.managed.mu.Unlock()
	if m.managed.fault != nil {
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
	defer r.mu.Unlock()
	if r.fault != nil {
		return nil
	}
	infos := []types.VolumeInfo{}
	for _, v := range r.data.Volumes {
		if tenant == "" || tenant == v.Tenant {
			info := r.info(v)
			if v.State == volumeReady && r.verify(v) != nil {
				info.State = string(volumeRecovery)
				info.Available = false
			}
			infos = append(infos, info.VolumeInfo)
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
	if r.fault != nil {
		return catalogVolume{}, false, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok {
		return catalogVolume{}, false, nil
	}
	entry := catalogVolume{disk: engine.VolumeSpec{Name: name, Path: r.path(v.ID), DirectIO: types.DirectIOOff}, tenants: []string{v.Tenant}, writable: true, managedID: v.ID}
	if v.State != volumeReady {
		return entry, true, managedStorageError()
	}
	if err := r.verify(v); err != nil {
		return entry, true, r.markRecovery(v)
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
	if h.State == holdRetiring {
		return nil
	}
	h.State = holdRetiring
	r.data.Holds[id] = h
	return r.commit()
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
	for _, v := range r.data.Volumes {
		if v.Backing.Inode != 0 && identity.Ino == v.Backing.Inode && uint64(identity.Dev) == v.Backing.Device {
			return true
		}
		if backing, err := os.Lstat(r.path(v.ID)); err == nil && os.SameFile(info, backing) {
			return true
		}
	}
	return false
}
