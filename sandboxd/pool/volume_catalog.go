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
	"strings"
	"sync"
	"unicode/utf8"

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
	name    string
	entries map[string]*catalogVolume
	raw     []byte
}

type volumeCatalog struct {
	entries map[string]*catalogVolume
	roots   []string
	pending *catalogWrite
	holders map[string]volumeHolders
	unknown int
	writeMu sync.Mutex
}

func (c *volumeCatalog) admissionError(name string) error {
	if c.unknown > 0 || c.pending != nil && c.pending.name == name {
		return ErrVolumeCatalogUnavailable
	}
	return nil
}

// RegisterVolume durably binds a root-managed name to a backing path.
func (m *Manager) RegisterVolume(ctx context.Context, name string, spec types.VolumeRegistration) error {
	volume, err := normalizeRegistration(name, spec, m.catalog.roots)
	if err != nil {
		return err
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err = m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	entries := m.catalog.entries
	err = m.catalog.admissionError(name)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if entry := entries[name]; entry != nil {
		if entry.runtime && entry.disk == volume.disk && entry.writable == volume.writable && slices.Equal(entry.tenants, volume.tenants) {
			return nil
		}
		return fmt.Errorf("%w: volume %q already has different semantics or is static", ErrVolumeRegistrationConflict, name)
	}
	if err := catalogPathConflict(entries, volume.disk.Path); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeRegistrationConflict, err)
	}
	entries = maps.Clone(entries)
	entries[name] = volume
	return m.writeCatalog(ctx, name, entries)
}

// UnregisterVolume retires an unreferenced runtime binding without touching its backing.
func (m *Manager) UnregisterVolume(ctx context.Context, name string) error {
	if !types.ValidVolumeName(name) {
		return fmt.Errorf("%w: invalid volume name", ErrBadVolume)
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := m.commitPendingCatalog(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	entries := m.catalog.entries
	entry := entries[name]
	err := m.catalog.admissionError(name)
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
		volume, normalizeErr := normalizeRegistration(name, spec, m.catalog.roots)
		if normalizeErr != nil {
			return fmt.Errorf("runtime volume catalog: %w", normalizeErr)
		}
		if m.catalog.entries[name] != nil {
			return fmt.Errorf("runtime volume catalog duplicates static name %q", name)
		}
		if err := catalogPathConflict(entries, volume.disk.Path); err != nil {
			return fmt.Errorf("runtime volume catalog: %w", err)
		}
		entries[name] = volume
	}
	m.catalog.entries = entries
	return nil
}

func (m *Manager) volumeReferenced(name string) bool {
	if m.catalog.holders[name] != (volumeHolders{}) {
		return true
	}
	// an adopted claim is published with no holders before its claims.json write counts it
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
			record.Volumes[key] = types.VolumeRegistration{Path: entry.disk.Path, DirectIO: entry.disk.DirectIO, Writable: entry.writable, Tenants: entry.tenants}
		}
	}
	raw, _ := json.Marshal(record, json.Deterministic(true))
	proposal := &catalogWrite{name: name, entries: entries, raw: raw}
	m.mu.Lock()
	err := ctx.Err()
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
	proposal := m.catalog.pending
	m.mu.Unlock()
	if proposal == nil {
		return nil
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

func catalogPathConflict(entries map[string]*catalogVolume, path string) error {
	for name, entry := range entries {
		if filepath.Clean(entry.disk.Path) == path {
			return fmt.Errorf("backing path already registered as %q", name)
		}
	}
	return nil
}

func normalizeRegistration(name string, spec types.VolumeRegistration, roots []string) (*catalogVolume, error) {
	spec.DirectIO = cmp.Or(spec.DirectIO, types.DirectIOOff)
	if !utf8.ValidString(spec.Path) {
		return nil, fmt.Errorf("%w: volume %q path must be valid UTF-8", ErrBadVolume, name)
	}
	if err := config.ValidateVolumeCatalog([]config.VolumeSpec{{
		Name: name, Path: spec.Path, DirectIO: spec.DirectIO, Writable: spec.Writable, Tenants: spec.Tenants,
	}}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	path := filepath.Clean(spec.Path)
	if len(roots) > 0 && !slices.ContainsFunc(roots, func(root string) bool {
		return path == root || strings.HasPrefix(path, strings.TrimSuffix(root, string(filepath.Separator))+string(filepath.Separator))
	}) {
		return nil, fmt.Errorf("%w: volume %q path is outside volume_roots", ErrBadVolume, name)
	}
	return &catalogVolume{
		runtime:  true,
		disk:     engine.VolumeSpec{Name: name, Path: path, DirectIO: spec.DirectIO},
		tenants:  slices.Compact(slices.Sorted(slices.Values(spec.Tenants))),
		writable: spec.Writable,
	}, nil
}
