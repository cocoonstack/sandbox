package pool

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

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

type catalogWrite struct {
	entries map[string]*catalogVolume
	raw     []byte
}

type volumeCatalog struct {
	entries map[string]*catalogVolume
	pending *catalogWrite
	holders map[string]volumeHolders
	unknown int
	writeMu sync.Mutex
}

func (c *volumeCatalog) admissionError() error {
	if c.pending != nil || c.unknown > 0 {
		return ErrVolumeCatalogUnavailable
	}
	return nil
}

// RegisterVolume durably binds a root-managed name to a backing path.
func (m *Manager) RegisterVolume(ctx context.Context, name string, spec types.VolumeRegistration) error {
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	normalized, err := normalizeRegistration(name, spec)
	if err != nil {
		return err
	}
	m.mu.Lock()
	entries := m.catalog.entries
	err = m.catalog.admissionError()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if entry := entries[name]; entry != nil {
		if entry.runtime && sameRegistration(entry.registration(), normalized) {
			return nil
		}
		return fmt.Errorf("%w: volume %q already has different semantics or is static", ErrVolumeRegistrationConflict, name)
	}
	entries = maps.Clone(entries)
	entries[name] = &catalogVolume{
		runtime:  true,
		disk:     engine.VolumeSpec{Name: name, Path: normalized.Path, DirectIO: normalized.DirectIO},
		tenants:  normalized.Tenants,
		writable: normalized.Writable,
	}
	if err := config.ValidateVolumeCatalog(catalogDefinitions(entries)); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeRegistrationConflict, err)
	}
	return m.writeCatalog(ctx, name, entries)
}

// UnregisterVolume retires an unreferenced runtime binding without touching its backing.
func (m *Manager) UnregisterVolume(ctx context.Context, name string) error {
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	if !types.ValidVolumeName(name) {
		return fmt.Errorf("%w: invalid volume name", ErrBadVolume)
	}
	m.mu.Lock()
	entries := m.catalog.entries
	entry := entries[name]
	err := m.catalog.admissionError()
	if err == nil && ((entry != nil && !entry.runtime) || (entry == nil && m.volumeReferenced(name))) {
		err = fmt.Errorf("%w: volume %q is static or referenced", ErrVolumeRegistrationConflict, name)
	}
	m.mu.Unlock()
	if err != nil || entry == nil {
		return err
	}
	entries = maps.Clone(entries)
	delete(entries, name)
	return m.writeCatalog(ctx, name, entries)
}

func (m *Manager) loadRuntimeVolumes() error {
	raw, err := os.ReadFile(filepath.Join(m.dataDir, runtimeVolumesFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
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
	for name, spec := range record.Volumes {
		normalized, normalizeErr := normalizeRegistration(name, spec)
		if normalizeErr != nil {
			return fmt.Errorf("runtime volume catalog: %w", normalizeErr)
		}
		if m.catalog.entries[name] != nil {
			return fmt.Errorf("runtime volume catalog duplicates static name %q", name)
		}
		entries[name] = &catalogVolume{
			runtime:  true,
			disk:     engine.VolumeSpec{Name: name, Path: normalized.Path, DirectIO: normalized.DirectIO},
			tenants:  normalized.Tenants,
			writable: normalized.Writable,
		}
	}
	if err := config.ValidateVolumeCatalog(catalogDefinitions(entries)); err != nil {
		return fmt.Errorf("runtime volume catalog: %w", err)
	}
	m.catalog.entries = entries
	return nil
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
		if volumeIndex(sb.Volumes, name) >= 0 || volumeIndex(sb.VolumesWant, name) >= 0 {
			return true
		}
	}
	return m.store.referencesVolume(name)
}

func (m *Manager) writeCatalog(ctx context.Context, name string, entries map[string]*catalogVolume) error {
	record := runtimeCatalog{Version: 1, Volumes: map[string]types.VolumeRegistration{}}
	for key, entry := range entries {
		if entry.runtime {
			record.Volumes[key] = entry.registration()
		}
	}
	raw, err := json.Marshal(record, json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	proposal := &catalogWrite{entries: entries, raw: raw}
	m.mu.Lock()
	err = ctx.Err()
	if err == nil {
		err = m.catalog.admissionError()
	}
	if err == nil && m.volumeReferenced(name) {
		err = fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	if err == nil {
		m.catalog.pending = proposal
	}
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return m.persistCatalog(proposal)
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
	return m.persistCatalog(proposal)
}

func (m *Manager) persistCatalog(proposal *catalogWrite) error {
	if err := utils.WriteFileSync(filepath.Join(m.dataDir, runtimeVolumesFile), proposal.raw, 0o600); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	m.mu.Lock()
	m.catalog.entries = proposal.entries
	m.catalog.pending = nil
	m.mu.Unlock()
	return nil
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

func catalogDefinitions(entries map[string]*catalogVolume) []config.VolumeSpec {
	volumes := make([]config.VolumeSpec, 0, len(entries))
	for name, entry := range entries {
		var tenants []string
		if entry.runtime {
			tenants = entry.tenants
		}
		volumes = append(volumes, config.VolumeSpec{
			Name: name, Path: entry.disk.Path, DirectIO: entry.disk.DirectIO,
			Writable: entry.writable, Tenants: tenants,
		})
	}
	return volumes
}

func normalizeRegistration(name string, spec types.VolumeRegistration) (types.VolumeRegistration, error) {
	spec.DirectIO = cmp.Or(spec.DirectIO, types.DirectIOOff)
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
