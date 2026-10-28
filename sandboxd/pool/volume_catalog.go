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

type volumeCatalog struct {
	entries map[string]*catalogVolume
	roots   []string
	holders map[string]volumeHolders
	writeMu sync.Mutex
}

func (c *volumeCatalog) withinRoots(path string) bool {
	return len(c.roots) == 0 || slices.ContainsFunc(c.roots, func(root string) bool {
		return path == root || strings.HasPrefix(path, strings.TrimSuffix(root, string(filepath.Separator))+string(filepath.Separator))
	})
}

// RegisterVolume durably binds a root-managed name to a backing path.
func (m *Manager) RegisterVolume(ctx context.Context, name string, spec types.VolumeRegistration) error {
	volume, err := normalizeRegistration(name, spec)
	if err != nil {
		return err
	}
	if !m.catalog.withinRoots(volume.disk.Path) {
		return fmt.Errorf("%w: volume %q path is outside volume_roots", ErrBadVolume, name)
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	entries := m.catalog.entries
	if entry := entries[name]; entry != nil {
		m.mu.Unlock()
		if entry.runtime && entry.disk == volume.disk && entry.writable == volume.writable && slices.Equal(entry.tenants, volume.tenants) {
			return nil
		}
		return fmt.Errorf("%w: volume %q already has different semantics or is static", ErrVolumeRegistrationConflict, name)
	}
	if m.volumeReferenced(name) {
		m.mu.Unlock()
		return fmt.Errorf("%w: volume %q is referenced", ErrVolumeRegistrationConflict, name)
	}
	m.mu.Unlock()
	if err := catalogPathConflict(entries, volume.disk.Path); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeRegistrationConflict, err)
	}
	entries = maps.Clone(entries)
	entries[name] = volume
	if err := m.persistRuntimeCatalog(entries); err != nil {
		return err
	}
	m.mu.Lock()
	m.catalog.entries = entries
	m.mu.Unlock()
	return nil
}

// UnregisterVolume retires an unreferenced runtime binding without touching its backing.
func (m *Manager) UnregisterVolume(ctx context.Context, name string) error {
	if !types.ValidVolumeName(name) {
		return fmt.Errorf("%w: invalid volume name", ErrBadVolume)
	}
	m.catalog.writeMu.Lock()
	defer m.catalog.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	entry := m.catalog.entries[name]
	if (entry != nil && !entry.runtime) || m.volumeReferenced(name) {
		m.mu.Unlock()
		return fmt.Errorf("%w: volume %q is static or referenced", ErrVolumeRegistrationConflict, name)
	}
	entries := maps.Clone(m.catalog.entries)
	delete(entries, name)
	m.catalog.entries = entries
	m.mu.Unlock()
	return m.persistRuntimeCatalog(entries)
}

func (m *Manager) loadRuntimeVolumes(ctx context.Context) error {
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
		volume, normalizeErr := normalizeRegistration(name, spec)
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
	// skip only once every entry validated, so other bad metadata still refuses startup
	for name := range record.Volumes {
		if path := entries[name].disk.Path; !m.catalog.withinRoots(path) {
			log.WithFunc("pool.loadRuntimeVolumes").Warnf(ctx, "runtime volume %q: skipping image %s outside volume_roots", name, path)
			delete(entries, name)
		}
	}
	m.catalog.entries = entries
	return nil
}

func (m *Manager) volumeReferenced(name string) bool {
	if m.catalog.holders[name] != (volumeHolders{}) {
		return true
	}
	for _, sb := range m.claimed {
		if volumeIndex(sb.Volumes, name) >= 0 || volumeIndex(sb.VolumesWant, name) >= 0 {
			return true
		}
	}
	return false
}

func (m *Manager) persistRuntimeCatalog(entries map[string]*catalogVolume) error {
	record := runtimeCatalog{Version: 1, Volumes: map[string]types.VolumeRegistration{}}
	for name, entry := range entries {
		if entry.runtime {
			record.Volumes[name] = types.VolumeRegistration{Path: entry.disk.Path, DirectIO: entry.disk.DirectIO, Writable: entry.writable, Tenants: entry.tenants}
		}
	}
	raw, _ := json.Marshal(record, json.Deterministic(true))
	if err := utils.WriteFileSync(filepath.Join(m.dataDir, runtimeVolumesFile), raw, 0o600); err != nil {
		return fmt.Errorf("%w: %w", ErrVolumeCatalogUnavailable, err)
	}
	return nil
}

func catalogPathConflict(entries map[string]*catalogVolume, path string) error {
	for name, entry := range entries {
		if filepath.Clean(entry.disk.Path) == path {
			return fmt.Errorf("backing path already registered as %q", name)
		}
	}
	return nil
}

func normalizeRegistration(name string, spec types.VolumeRegistration) (*catalogVolume, error) {
	spec.DirectIO = cmp.Or(spec.DirectIO, types.DirectIOOff)
	for _, tenant := range spec.Tenants {
		if !types.NameRe.MatchString(tenant) {
			return nil, fmt.Errorf("%w: volume %q tenant %q must match %s", ErrBadVolume, name, tenant, types.NameRe)
		}
	}
	if !utf8.ValidString(spec.Path) {
		return nil, fmt.Errorf("%w: volume %q path must be valid UTF-8", ErrBadVolume, name)
	}
	if err := config.ValidateVolumeCatalog([]config.VolumeSpec{{
		Name: name, Path: spec.Path, DirectIO: spec.DirectIO, Writable: spec.Writable, Tenants: spec.Tenants,
	}}); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	return &catalogVolume{
		runtime:  true,
		disk:     engine.VolumeSpec{Name: name, Path: filepath.Clean(spec.Path), DirectIO: spec.DirectIO},
		tenants:  slices.Compact(slices.Sorted(slices.Values(spec.Tenants))),
		writable: spec.Writable,
	}, nil
}
