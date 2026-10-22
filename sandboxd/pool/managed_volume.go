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
	volumeKindManaged = "managed"
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

func managedRecoveryError() *VolumeError {
	return volumeError(503, "volume_recovery_required", "volume requires operator recovery; inspect the volume")
}

func managedNotFound() *VolumeError { return volumeError(404, "volume_not_found", "unknown volume") }

func (m *Manager) CreateVolume(ctx context.Context, name string, req types.VolumeCreate, tenant string) (types.ManagedVolumeInfo, bool, error) {
	owner, err := m.volumeCreateOwner(ctx, name, req, tenant)
	if err != nil {
		return types.ManagedVolumeInfo{}, false, err
	}
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, false, managedNotFound()
	}
	m.mu.Lock()
	admission := m.volumeAdmission[name]
	m.mu.Unlock()
	sleeping := m.sleepHolds(name)
	r.mu.Lock()
	if err := r.claimName(name); err != nil {
		r.mu.Unlock()
		return types.ManagedVolumeInfo{}, false, err
	}
	defer r.releaseName(name)
	defer r.mu.Unlock()
	if r.closed || r.writeFault != nil {
		return types.ManagedVolumeInfo{}, false, managedStorageError()
	}
	_, managedName := r.data.Volumes[name]
	if _, ok := m.volumes[name]; ok || !managedName && (admission.readers > 0 || admission.writers > 0) {
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
	case volumeReady:
		r.mu.Unlock()
		err := r.verify(v)
		r.mu.Lock()
		if err != nil {
			if errors.Is(err, errBackingMismatch) {
				return types.ManagedVolumeInfo{}, false, r.markRecovery(v, sleeping)
			}
			return types.ManagedVolumeInfo{}, false, managedStorageError()
		}
	case volumeDeleting:
		return types.ManagedVolumeInfo{}, false, volumeError(409, "volume_deleting", "volume deletion is pending")
	case volumeRecovery:
		return types.ManagedVolumeInfo{}, false, managedRecoveryError()
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
	if errors.Is(err, os.ErrExist) {
		// no recorded inode means no VM ever saw this file, so it is safe to start over
		if err = os.Remove(path); err == nil {
			f, err = openPrivateFile(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)
		}
	}
	if err != nil {
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

// A sleep or a growth reconciles its own backing, so a loss under either stays transient.
func (r *volumeRegistry) recoverable(v managedVolume, sleeping bool) bool {
	current, ok := r.data.Volumes[v.Name]
	return ok && current == v && v.GrowTo == 0 && !sleeping
}

func (r *volumeRegistry) markRecovery(v managedVolume, sleeping bool) error {
	if !r.recoverable(v, sleeping) {
		return managedStorageError()
	}
	v.State = volumeRecovery
	r.data.Volumes[v.Name] = v
	if err := r.commitAndRelock(); err != nil {
		return err
	}
	return managedRecoveryError()
}

func (m *Manager) GetVolume(name, tenant string) (types.ManagedVolumeInfo, error) {
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	m.mu.Lock()
	holders := m.volumeHoldersLocked(name)
	m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return types.ManagedVolumeInfo{}, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok || (tenant != "" && v.Tenant != tenant) {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	info := r.info(v)
	info.Holders = holders
	r.mu.Unlock()
	defer r.mu.Lock()
	if v.State == volumeReady {
		if err := r.verify(v); err != nil {
			if !errors.Is(err, errBackingMismatch) {
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
	r.mu.Lock()
	if err := r.claimName(name); err != nil {
		r.mu.Unlock()
		return err
	}
	defer r.releaseName(name)
	v, err := r.admitDelete(name, expected, tenant)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	path := r.path(v.ID)
	actual, inspectErr := r.inspectManaged(path)
	if errors.Is(inspectErr, errManagedDirectory) {
		return managedStorageError()
	}
	missing := errors.Is(inspectErr, os.ErrNotExist)
	m.mu.Lock()
	r.mu.Lock()
	defer r.mu.Unlock()
	v = r.data.Volumes[name]
	refusal := m.volumeRefusalLocked(v)
	mismatch := refusal == nil && !missing && v.State != volumeCreating && (inspectErr != nil || v.Backing.Inode == 0 || actual != v.Backing)
	marked := refusal == nil && !mismatch && v.State != volumeDeleting
	if marked {
		// marked under m.mu, so a claim that reserves the name afterwards refuses on the state
		v.State = volumeDeleting
		r.data.Volumes[name] = v
	}
	m.mu.Unlock()
	switch {
	case refusal != nil:
		return refusal
	case mismatch:
		return volumeError(503, "volume_backing_mismatch", "volume backing requires operator recovery")
	}
	if marked {
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
	delete(r.data.Volumes, name)
	return r.commitAndRelock()
}

func (r *volumeRegistry) admitDelete(name, expected, tenant string) (managedVolume, error) {
	if r.closed || r.writeFault != nil {
		return managedVolume{}, managedStorageError()
	}
	if expected == "" {
		return managedVolume{}, volumeError(428, "volume_precondition_required", "If-Match must identify the volume")
	}
	v, ok := r.data.Volumes[name]
	if !ok || (tenant != "" && v.Tenant != tenant) {
		return managedVolume{}, managedNotFound()
	}
	if v.ID != volumeID(expected) {
		return managedVolume{}, volumeError(412, "volume_precondition_failed", "volume identity does not match")
	}
	return v, nil
}

// volumeRefusalLocked refuses a delete or grow while growth is pending or any claim or queued removal holds the name; callers hold m.mu and r.mu.
func (m *Manager) volumeRefusalLocked(v managedVolume) error {
	if v.GrowTo != 0 {
		return growInProgress(v.Name)
	}
	if held := m.volumeAdmission[v.Name]; held.readers > 0 || held.writers > 0 {
		return &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: v.Name, Holders: m.volumeHoldersLocked(v.Name)}
	}
	return nil
}

const (
	holderClaimed   = "claimed"
	holderReleasing = "releasing"
)

// volumeHoldersLocked lists the claims and queued VM removals holding name, for refusal bodies; callers hold m.mu.
func (m *Manager) volumeHoldersLocked(name string) []types.VolumeHolder {
	holders := []types.VolumeHolder{}
	add := func(id string, volumes []types.Volume, state string) {
		if i := volumeIndex(volumes, name); i >= 0 {
			mode := types.VolumeModeRO
			if volumes[i].RW() {
				mode = types.VolumeModeRW
			}
			holders = append(holders, types.VolumeHolder{SandboxID: id, Mode: mode, State: state})
		}
	}
	for _, sb := range m.claimed {
		add(sb.ID, heldVolumes(sb), holderClaimed)
	}
	for _, pending := range m.pendingRemovals {
		add(pending.sandboxID, pending.volumes.holds, holderReleasing)
	}
	slices.SortFunc(holders, func(a, b types.VolumeHolder) int { return strings.Compare(a.SandboxID, b.SandboxID) })
	return holders
}

func (r *volumeRegistry) info(v managedVolume) types.ManagedVolumeInfo {
	return types.ManagedVolumeInfo{Name: v.Name, DefaultMount: types.DefaultVolumeMount(v.Name), SizeBytes: v.Size, Available: v.State == volumeReady, Nodes: 1, Writable: true, Kind: volumeKindManaged, ID: string(v.ID), Tenant: v.Tenant, Node: v.Node, State: string(v.State), CreatedAt: v.CreatedAt}
}

func (m *Manager) managedInfos(tenant string) []types.VolumeInfo {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
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
		if v.State != volumeReady {
			continue
		}
		if err := r.verify(v); err != nil {
			if errors.Is(err, errBackingMismatch) {
				infos[i].State = string(volumeRecovery)
			}
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
	// a write fault leaves memory ahead of disk, so no claim may build on an unrecorded volume
	if r.closed || r.writeFault != nil {
		return catalogVolume{}, false, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok {
		return catalogVolume{}, false, nil
	}
	entry := catalogVolume{disk: engine.VolumeSpec{Name: name, Path: r.path(v.ID), DirectIO: types.DirectIOOff}, tenants: []string{v.Tenant}, writable: true, managedID: v.ID}
	switch v.State {
	case volumeReady:
	case volumeRecovery:
		return entry, true, managedRecoveryError()
	default:
		return entry, true, managedStorageError()
	}
	if v.GrowTo != 0 {
		return entry, true, growInProgress(name)
	}
	r.mu.Unlock()
	err := r.verify(v)
	sleeping := err != nil && m.sleepHolds(name)
	r.mu.Lock()
	switch {
	case err == nil:
		return entry, true, nil
	case errors.Is(err, errBackingMismatch) && r.recoverable(v, sleeping):
		return entry, true, managedRecoveryError()
	default:
		return entry, true, managedStorageError()
	}
}

// managedVolumeNames holds every managed name rw, for a VM whose attached disks are unknown.
func (m *Manager) managedVolumeNames() []types.Volume {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]types.Volume, 0, len(r.data.Volumes))
	for name := range r.data.Volumes {
		names = append(names, types.Volume{Name: name, Mode: types.VolumeModeRW})
	}
	return names
}

func (m *Manager) managedGossipNames() []string {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.data.Volumes))
	for name, v := range r.data.Volumes {
		if v.State != volumeDeleting {
			names = append(names, types.ManagedVolumeGossipName(v.Tenant, name))
		}
	}
	return names
}

// managedPath returns the backing a managed name resolves to.
func (m *Manager) managedPath(name string) (string, bool) {
	r := m.managed
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.data.Volumes[name]
	return r.path(v.ID), ok
}

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
