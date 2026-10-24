package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const runtimeVolumesFile = "runtime-volumes.json"

type runtimeCatalog struct {
	Version int                                 `json:"version"`
	Volumes map[string]types.VolumeRegistration `json:"volumes"`
}

func normalizeRegistration(name string, spec types.VolumeRegistration) (types.VolumeRegistration, error) {
	if spec.DirectIO == "" {
		spec.DirectIO = types.DirectIOOff
	}
	if err := config.ValidateVolumeCatalog([]config.VolumeSpec{{Name: name, Path: spec.Path, DirectIO: spec.DirectIO}}); err != nil {
		return spec, fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	spec.Path = filepath.Clean(spec.Path)
	spec.Tenants = slices.Clone(spec.Tenants)
	slices.Sort(spec.Tenants)
	spec.Tenants = slices.Compact(spec.Tenants)
	return spec, nil
}

func sameRegistration(a, b types.VolumeRegistration) bool {
	return a.Path == b.Path && a.DirectIO == b.DirectIO && a.Writable == b.Writable && slices.Equal(a.Tenants, b.Tenants)
}

func (m *Manager) loadRuntimeVolumes() error {
	m.runtimeVolumes = map[string]types.VolumeRegistration{}
	path := filepath.Join(m.dataDir, runtimeVolumesFile)
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect runtime volume catalog: %w", err)
	}
	if err = protectedCatalogPath(m.dataDir); err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0o600 || st.Sys().(*syscall.Stat_t).Uid != 0 {
		return fmt.Errorf("runtime volume catalog must be a root-owned regular file with mode 0600")
	}
	raw, err := os.ReadFile(path) //nolint:gosec // root-owned metadata below a protected directory
	if err != nil {
		return fmt.Errorf("read runtime volume catalog: %w", err)
	}
	var record runtimeCatalog
	if err = json.Unmarshal(raw, &record, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("parse runtime volume catalog: %w", err)
	}
	if record.Version != 1 || record.Volumes == nil {
		return fmt.Errorf("invalid runtime volume catalog version or volumes")
	}
	merged := make([]config.VolumeSpec, 0, len(m.volumes)+len(record.Volumes))
	for name, entry := range m.volumes {
		merged = append(merged, config.VolumeSpec{Name: name, Path: entry.disk.Path, DirectIO: entry.disk.DirectIO})
	}
	for name, spec := range record.Volumes {
		normalized, err := normalizeRegistration(name, spec)
		if err != nil {
			return fmt.Errorf("runtime volume catalog: %w", err)
		}
		m.runtimeVolumes[name] = normalized
		merged = append(merged, config.VolumeSpec{Name: name, Path: normalized.Path, DirectIO: normalized.DirectIO})
	}
	if err := config.ValidateVolumeCatalog(merged); err != nil {
		return fmt.Errorf("runtime volume catalog: %w", err)
	}
	m.publishRuntimeCatalog(m.runtimeVolumes)
	return nil
}

// RegisterVolume durably binds a root-managed name to an existing backing file.
func (m *Manager) RegisterVolume(ctx context.Context, name string, spec types.VolumeRegistration) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("%w: runtime catalog mutation requires effective uid zero", ErrVolumeCatalogUnavailable)
	}
	m.catalogWriteMu.Lock()
	defer m.catalogWriteMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	normalized, err := normalizeRegistration(name, spec)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if m.unknownVolumes > 0 {
		m.mu.Unlock()
		return ErrVolumeCatalogUnavailable
	}
	if current, ok := m.runtimeVolumes[name]; ok {
		m.mu.Unlock()
		if sameRegistration(current, normalized) {
			return nil
		}
		return fmt.Errorf("%w: volume %q already has different semantics", ErrVolumeRegistrationConflict, name)
	}
	if m.volumeReferenced(name) {
		m.mu.Unlock()
		return fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	for existing, entry := range m.volumes {
		if existing == name || filepath.Clean(entry.disk.Path) == normalized.Path {
			m.mu.Unlock()
			return fmt.Errorf("%w: volume %q duplicates a catalog name or path", ErrVolumeRegistrationConflict, name)
		}
	}
	m.mu.Unlock()
	if _, err := m.statVolume(normalized.Path); err != nil {
		return fmt.Errorf("%w: backing for volume %q is unavailable", ErrBadVolume, name)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	// A claim can acquire an unexplained recovery name while the backing is checked.
	if m.unknownVolumes > 0 {
		m.mu.Unlock()
		return ErrVolumeCatalogUnavailable
	}
	if m.volumeReferenced(name) {
		m.mu.Unlock()
		return fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	proposal := maps.Clone(m.runtimeVolumes)
	proposal[name] = normalized
	m.catalogPending = proposal
	m.mu.Unlock()
	return m.commitPendingCatalog(ctx)
}

// UnregisterVolume retires an unreferenced runtime binding without touching its backing.
func (m *Manager) UnregisterVolume(ctx context.Context, name string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("%w: runtime catalog mutation requires effective uid zero", ErrVolumeCatalogUnavailable)
	}
	m.catalogWriteMu.Lock()
	defer m.catalogWriteMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	if !types.ValidVolumeName(name) {
		return fmt.Errorf("%w: invalid volume name", ErrBadVolume)
	}
	m.mu.Lock()
	if m.unknownVolumes > 0 {
		m.mu.Unlock()
		return ErrVolumeCatalogUnavailable
	}
	if _, runtime := m.runtimeVolumes[name]; !runtime {
		_, exists := m.volumes[name]
		referenced := m.volumeReferenced(name)
		m.mu.Unlock()
		if exists || referenced {
			return fmt.Errorf("%w: volume %q is static or referenced", ErrVolumeRegistrationConflict, name)
		}
		return nil
	}
	if m.volumeReferenced(name) {
		m.mu.Unlock()
		return fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	proposal := maps.Clone(m.runtimeVolumes)
	delete(proposal, name)
	m.catalogPending = proposal
	m.mu.Unlock()
	return m.commitPendingCatalog(ctx)
}

// volumeReferenced includes dispatched cleanup through its ordinary admission counts.
func (m *Manager) volumeReferenced(name string) bool {
	if holders := m.volumeAdmission[name]; holders.readers > 0 || holders.writers > 0 {
		return true
	}
	for _, run := range m.volumeRuns {
		if volumeIndex(run.holds, name) >= 0 {
			return true
		}
	}
	for _, sb := range m.claimed {
		if volumeIndex(heldVolumes(sb), name) >= 0 {
			return true
		}
	}
	for _, refs := range m.store.recoveryVolumes() {
		if volumeIndex(refs, name) >= 0 {
			return true
		}
	}
	return false
}

// commitPendingCatalog holds catalogWriteMu, but never m.mu across filesystem work.
func (m *Manager) commitPendingCatalog(ctx context.Context) error {
	m.mu.Lock()
	proposal := m.catalogPending
	unknown := m.unknownVolumes > 0
	m.mu.Unlock()
	if unknown {
		return ErrVolumeCatalogUnavailable
	}
	if proposal == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	if err := m.replaceRuntimeCatalog(proposal); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	m.mu.Lock()
	m.publishRuntimeCatalog(proposal)
	m.catalogPending = nil
	m.mu.Unlock()
	return nil
}

func (m *Manager) publishRuntimeCatalog(proposal map[string]types.VolumeRegistration) {
	for name := range m.runtimeVolumes {
		if _, ok := proposal[name]; !ok {
			delete(m.volumes, name)
		}
	}
	for name, spec := range proposal {
		if prior, ok := m.runtimeVolumes[name]; ok && sameRegistration(prior, spec) && m.volumes[name] != nil {
			continue
		}
		m.volumes[name] = &catalogVolume{disk: engine.VolumeSpec{Name: name, Path: spec.Path, DirectIO: spec.DirectIO}, tenants: slices.Clone(spec.Tenants), writable: spec.Writable}
	}
	m.runtimeVolumes = proposal
}

func (m *Manager) replaceRuntimeCatalog(proposal map[string]types.VolumeRegistration) error {
	if err := protectedCatalogPath(m.dataDir); err != nil {
		return err
	}
	path := filepath.Join(m.dataDir, runtimeVolumesFile)
	if st, err := os.Lstat(path); err == nil {
		if !st.Mode().IsRegular() || st.Mode().Perm() != 0o600 || st.Sys().(*syscall.Stat_t).Uid != 0 {
			return fmt.Errorf("runtime volume catalog must be a root-owned regular file with mode 0600")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, err := json.Marshal(runtimeCatalog{Version: 1, Volumes: proposal}, json.Deterministic(true))
	if err != nil {
		return err
	}
	if m.catalogReplace != nil {
		return m.catalogReplace(path, raw)
	}
	return utils.WriteFileSync(path, raw, 0o600)
}

func protectedCatalogPath(dir string) error {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for {
		st, err := os.Lstat(absolute)
		if err != nil {
			return fmt.Errorf("inspect runtime catalog directory: %w", err)
		}
		if !st.IsDir() || st.Sys().(*syscall.Stat_t).Uid != 0 || st.Mode().Perm()&0o022 != 0 && st.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf("runtime catalog directory %q must be protected and root-owned", absolute)
		}
		parent := filepath.Dir(absolute)
		if parent == absolute {
			return nil
		}
		absolute = parent
	}
}

func (m *Manager) retryVolumeCatalog(ctx context.Context) {
	if !m.catalogWriteMu.TryLock() {
		return
	}
	defer m.catalogWriteMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		log.WithFunc("pool.retryVolumeCatalog").Error(ctx, err, "persist runtime volume catalog")
	}
}
