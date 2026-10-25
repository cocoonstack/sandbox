package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const runtimeVolumesFile = "runtime-volumes.json"

type (
	runtimeCatalog struct {
		Version int                                 `json:"version"`
		Volumes map[string]types.VolumeRegistration `json:"volumes"`
	}

	volumeCatalog struct {
		entries    map[string]*catalogVolume
		generation uint64
		pending    *catalogWrite
		holders    map[string]volumeHolders
		unknown    int
		writeMu    sync.Mutex
		replace    func(string, []byte) error
		stat       func(string) (os.FileInfo, error)
	}

	catalogWrite struct {
		entries    map[string]*catalogVolume
		generation uint64
		raw        []byte
	}
)

func (c *volumeCatalog) admissionError() error {
	if c.pending != nil || c.unknown > 0 {
		return ErrVolumeCatalogUnavailable
	}
	return nil
}

func (v catalogVolume) registration() types.VolumeRegistration {
	return types.VolumeRegistration{Path: v.disk.Path, DirectIO: v.disk.DirectIO, Writable: v.writable, Tenants: v.tenants}
}

func catalogDefinitions(entries map[string]*catalogVolume) []config.VolumeSpec {
	volumes := make([]config.VolumeSpec, 0, len(entries))
	for name, entry := range entries {
		volumes = append(volumes, config.VolumeSpec{
			Name: name, Path: entry.disk.Path, DirectIO: entry.disk.DirectIO,
			Writable: entry.writable, Tenants: entry.tenants,
		})
	}
	return volumes
}

func (c *volumeCatalog) adding(name string, spec types.VolumeRegistration) (*catalogWrite, error) {
	if c.generation == math.MaxUint64 {
		return nil, fmt.Errorf("%w: registration generations exhausted", ErrVolumeRegistrationConflict)
	}
	entries := maps.Clone(c.entries)
	entries[name] = &catalogVolume{
		disk:    engine.VolumeSpec{Name: name, Path: spec.Path, DirectIO: spec.DirectIO},
		tenants: spec.Tenants, writable: spec.Writable, runtime: true, generation: c.generation + 1,
	}
	if err := config.ValidateVolumeCatalog(catalogDefinitions(entries)); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrVolumeRegistrationConflict, err)
	}
	return &catalogWrite{entries: entries, generation: c.generation + 1}, nil
}

func normalizeRegistration(name string, spec types.VolumeRegistration) (types.VolumeRegistration, error) {
	if spec.DirectIO == "" {
		spec.DirectIO = types.DirectIOOff
	}
	if err := config.ValidateVolumeCatalog([]config.VolumeSpec{{
		Name: name, Path: spec.Path, DirectIO: spec.DirectIO, Writable: spec.Writable, Tenants: spec.Tenants,
	}}); err != nil {
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
	if _, err := os.Lstat(filepath.Join(m.dataDir, runtimeVolumesFile)); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect runtime volume catalog: %w", err)
	}
	if err := validateRuntimeCatalogPath(m.dataDir); err != nil {
		return err
	}
	root, err := os.OpenRoot(m.dataDir)
	if err != nil {
		return fmt.Errorf("open runtime catalog directory: %w", err)
	}
	raw, readErr := root.ReadFile(runtimeVolumesFile)
	if err = errors.Join(readErr, root.Close()); err != nil {
		return fmt.Errorf("read runtime volume catalog: %w", err)
	}
	var record runtimeCatalog
	if err = json.Unmarshal(raw, &record, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("parse runtime volume catalog: %w", err)
	}
	if record.Version != 1 || record.Volumes == nil {
		return errors.New("invalid runtime volume catalog version or volumes")
	}
	entries := maps.Clone(m.catalog.entries)
	generation := m.catalog.generation
	for name, spec := range record.Volumes {
		normalized, normalizeErr := normalizeRegistration(name, spec)
		if normalizeErr != nil {
			return fmt.Errorf("runtime volume catalog: %w", normalizeErr)
		}
		if m.catalog.entries[name] != nil {
			return fmt.Errorf("runtime volume catalog duplicates static name %q", name)
		}
		if generation == math.MaxUint64 {
			return errors.New("runtime volume catalog generations exhausted")
		}
		generation++
		entries[name] = &catalogVolume{
			disk:    engine.VolumeSpec{Name: name, Path: normalized.Path, DirectIO: normalized.DirectIO},
			tenants: normalized.Tenants, writable: normalized.Writable, runtime: true, generation: generation,
		}
	}
	if err := config.ValidateVolumeCatalog(catalogDefinitions(entries)); err != nil {
		return fmt.Errorf("runtime volume catalog: %w", err)
	}
	m.catalog.entries, m.catalog.generation = entries, generation
	return nil
}

// RegisterVolume durably binds a root-managed name to an existing backing file.
func (m *Manager) RegisterVolume(ctx context.Context, name string, spec types.VolumeRegistration) error {
	if os.Geteuid() != 0 {
		return ErrVolumeCatalogReadOnly
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	normalized, err := normalizeRegistration(name, spec)
	if err != nil {
		return err
	}
	proposal, err := m.registrationProposal(name, normalized)
	if err != nil || proposal == nil {
		return err
	}
	if st, statErr := m.statVolume(normalized.Path); statErr != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("%w: backing for volume %q must be an available regular file", ErrBadVolume, name)
	}
	return m.writeCatalog(ctx, name, proposal)
}

func (m *Manager) registrationProposal(name string, spec types.VolumeRegistration) (*catalogWrite, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.catalog.admissionError(); err != nil {
		return nil, err
	}
	if entry := m.catalog.entries[name]; entry != nil {
		if entry.runtime && sameRegistration(entry.registration(), spec) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: volume %q already has different semantics or is static", ErrVolumeRegistrationConflict, name)
	}
	if m.volumeReferenced(name) {
		return nil, fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	return m.catalog.adding(name, spec)
}

// UnregisterVolume retires an unreferenced runtime binding without touching its backing.
func (m *Manager) UnregisterVolume(ctx context.Context, name string) error {
	if os.Geteuid() != 0 {
		return ErrVolumeCatalogReadOnly
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	if !types.ValidVolumeName(name) {
		return fmt.Errorf("%w: invalid volume name", ErrBadVolume)
	}
	proposal, err := m.removalProposal(name)
	if err != nil || proposal == nil {
		return err
	}
	return m.writeCatalog(ctx, name, proposal)
}

func (m *Manager) removalProposal(name string) (*catalogWrite, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.catalog.admissionError(); err != nil {
		return nil, err
	}
	entry := m.catalog.entries[name]
	if (entry != nil && !entry.runtime) || m.volumeReferenced(name) {
		return nil, fmt.Errorf("%w: volume %q is static or referenced", ErrVolumeRegistrationConflict, name)
	}
	if entry == nil {
		return nil, nil
	}
	entries := maps.Clone(m.catalog.entries)
	delete(entries, name)
	return &catalogWrite{entries: entries, generation: m.catalog.generation}, nil
}

func (m *Manager) volumeReferenced(name string) bool {
	if holders := m.catalog.holders[name]; holders.readers > 0 || holders.writers > 0 {
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

func (m *Manager) writeCatalog(ctx context.Context, name string, proposal *catalogWrite) error {
	if err := validateRuntimeCatalogPath(m.dataDir); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogInvalid, err)
	}
	record := runtimeCatalog{Version: 1, Volumes: map[string]types.VolumeRegistration{}}
	for key, entry := range proposal.entries {
		if entry.runtime {
			record.Volumes[key] = entry.registration()
		}
	}
	raw, err := json.Marshal(record, json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	proposal.raw = raw
	if err := m.acceptCatalog(ctx, name, proposal); err != nil {
		return err
	}
	return m.persistCatalog(proposal)
}

func (m *Manager) acceptCatalog(ctx context.Context, name string, proposal *catalogWrite) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.catalog.admissionError(); err != nil {
		return err
	}
	if m.volumeReferenced(name) {
		return fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	m.catalog.pending = proposal
	return nil
}

func (m *Manager) commitPendingCatalog(ctx context.Context) error {
	m.mu.Lock()
	proposal, unknown := m.catalog.pending, m.catalog.unknown > 0
	m.mu.Unlock()
	if proposal == nil {
		return nil
	}
	if unknown {
		return ErrVolumeCatalogUnavailable
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	if err := validateRuntimeCatalogPath(m.dataDir); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	return m.persistCatalog(proposal)
}

func (m *Manager) persistCatalog(proposal *catalogWrite) error {
	replace := m.catalog.replace
	if replace == nil {
		replace = func(path string, raw []byte) error { return utils.WriteFileSync(path, raw, 0o600) }
	}
	if err := replace(filepath.Join(m.dataDir, runtimeVolumesFile), proposal.raw); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	m.mu.Lock()
	m.catalog.entries, m.catalog.generation = proposal.entries, proposal.generation
	m.catalog.pending = nil
	m.mu.Unlock()
	return nil
}

func validateRuntimeCatalogPath(dir string) error {
	if err := protectedCatalogPath(dir); err != nil {
		return err
	}
	st, err := os.Lstat(filepath.Join(dir, runtimeVolumesFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0o600 || st.Sys().(*syscall.Stat_t).Uid != 0 {
		return errors.New("runtime volume catalog must be a root-owned regular file with mode 0600")
	}
	return nil
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
	if !m.catalog.writeMu.TryLock() {
		return
	}
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		log.WithFunc("pool.retryVolumeCatalog").Error(ctx, err, "persist runtime volume catalog")
	}
}
