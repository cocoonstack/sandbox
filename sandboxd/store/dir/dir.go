// Package dir is the directory record backend: plain files under a
// root whose filesystem is the operator's choice — local disk keeps
// records node-local, a shared FUSE mount makes them cluster-wide.
package dir

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

const (
	oldSuffix = ".old" // pre-generation crash artifacts, reclaimed at Delete

	// generationGrace bounds the shared-mount race where another node is
	// still cloning a generation it resolved just before a re-publish.
	generationGrace = time.Hour
)

var _ store.Store = (*Store)(nil)

// Store keeps records as <root>/<id>/{meta.json,export-<gen>/...}: the
// generation dir is immutable and the meta rename is the atomic commit
// pointer, so readers and writers need no cross-process locks. idRe names
// the instance's id namespace — two instances (checkpoints, templates)
// share a root without seeing each other's records.
type Store struct {
	root string
	idRe *regexp.Regexp
}

func New(root string, idRe *regexp.Regexp) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &Store{root: root, idRe: idRe}, nil
}

func (d *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(d.root, id+"-*.tmp")
}

// Publish makes the generation fresh before its install, refreshes the
// outgoing generation at supersession, and commits by renaming meta.json.
func (d *Store) Publish(_ context.Context, staging, id string) error {
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	final := filepath.Join(d.root, id)
	if err := os.MkdirAll(final, 0o750); err != nil {
		return err
	}
	genDir := filepath.Join(final, store.ExportGen(metaRaw))
	now := time.Now()
	genInfo, statErr := os.Stat(genDir) //nolint:gosec // G703: id pinned by the instance idRe before any call
	switch {
	case errors.Is(statErr, fs.ErrNotExist):
		stagedExport := filepath.Join(staging, store.ExportDir)
		// Make the generation fresh before a peer can observe it without committed meta.
		if err := os.Chtimes(stagedExport, now, now); err != nil { //nolint:gosec // our own staging dir
			return err
		}
		if err := os.Rename(stagedExport, genDir); err != nil { //nolint:gosec // G703: our own staging dir
			return err
		}
	case statErr != nil:
		return statErr
	default:
		currentMeta, readErr := os.ReadFile(filepath.Join(final, store.MetaFile)) //nolint:gosec // fixed path under our root
		if readErr == nil && bytes.Equal(currentMeta, metaRaw) {
			_ = d.sweepGenerations(id)
			return os.RemoveAll(staging)
		}
		if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
			return readErr
		}
		// A sweep may already have selected an expired path for removal.
		if time.Since(genInfo.ModTime()) >= generationGrace {
			return fmt.Errorf("generation %s expired before commit; retry after sweep", filepath.Base(genDir))
		}
	}
	// Age the previous current generation from supersession, not publication.
	if err := touchCurrentGeneration(final, now); err != nil {
		return err
	}
	tmp := filepath.Join(final, store.MetaFile+".tmp")
	if err := os.WriteFile(tmp, metaRaw, 0o600); err != nil { //nolint:gosec // fixed path under our root
		return err
	}
	if err := os.Rename(tmp, filepath.Join(final, store.MetaFile)); err != nil {
		return err
	}
	_ = d.sweepGenerations(id) // later publishes and startups retry a failed sweep
	return os.RemoveAll(staging)
}

// Fetch resolves the committed meta and returns its immutable generation
// directory; release is a no-op. Records published before per-generation
// dirs fall back to the flat export layout.
func (d *Store) Fetch(ctx context.Context, id string) (string, []byte, func(), error) {
	meta, err := d.ReadMeta(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}
	dir := filepath.Join(d.root, id, store.ExportGen(meta))
	if _, statErr := os.Stat(dir); errors.Is(statErr, fs.ErrNotExist) {
		dir = filepath.Join(d.root, id, store.ExportDir)
		switch _, legacyErr := os.Stat(dir); {
		case errors.Is(legacyErr, fs.ErrNotExist):
			return "", nil, nil, store.ErrNotFound
		case legacyErr != nil:
			return "", nil, nil, legacyErr
		}
	} else if statErr != nil {
		return "", nil, nil, statErr
	}
	return dir, meta, func() {}, nil
}

func (d *Store) ReadMeta(_ context.Context, id string) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(d.root, id, store.MetaFile)) //nolint:gosec // id pinned by the instance idRe before any call
	if os.IsNotExist(err) {
		return nil, store.ErrNotFound
	}
	return raw, err
}

func (d *Store) Metas(ctx context.Context) ([][]byte, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var metas [][]byte
	for _, e := range entries {
		// Only well-formed record dirs in this instance's namespace — a
		// planted foo/meta.json is not a record.
		if !e.IsDir() || !d.idRe.MatchString(e.Name()) {
			continue
		}
		raw, err := d.ReadMeta(ctx, e.Name())
		switch {
		case err == nil:
			metas = append(metas, raw)
		case errors.Is(err, store.ErrNotFound): // absence mid-list is a race
		default:
			return nil, err // a corrupt or unreadable meta must not vanish silently
		}
	}
	return metas, nil
}

// Delete removes export generations first and the meta last, so a partially
// failed delete stays discoverable and a retry converges.
func (d *Store) Delete(_ context.Context, id string) error {
	final := filepath.Join(d.root, id)
	entries, err := os.ReadDir(final)
	if errors.Is(err, fs.ErrNotExist) {
		return os.RemoveAll(final + oldSuffix)
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == store.MetaFile {
			continue
		}
		if err := os.RemoveAll(filepath.Join(final, e.Name())); err != nil {
			return err
		}
	}
	return errors.Join(os.RemoveAll(final), os.RemoveAll(final+oldSuffix))
}

// SweepStaging clears crashed staging and delegates generation retention.
func (d *Store) SweepStaging() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return err
	}
	// ReadDir + suffix, not Glob: the root path may hold glob
	// metacharacters.
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			if err := os.RemoveAll(filepath.Join(d.root, e.Name())); err != nil {
				return err
			}
		}
	}
	return d.SweepGenerations()
}

// SweepGenerations reclaims expired record generations without touching staging.
func (d *Store) SweepGenerations() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || !d.idRe.MatchString(e.Name()) {
			continue
		}
		if err := d.sweepGenerations(e.Name()); err != nil {
			return err
		}
	}
	return nil
}

// sweepGenerations reclaims only entries whose own install or supersession
// time is outside the grace. Without committed meta it never removes the
// record directory, so a peer publishing into the same path remains safe.
func (d *Store) sweepGenerations(id string) (err error) {
	final := filepath.Join(d.root, id)
	metaFile, err := os.Open(filepath.Join(final, store.MetaFile)) //nolint:gosec // id pinned by the instance idRe
	if errors.Is(err, fs.ErrNotExist) {
		entries, readErr := os.ReadDir(final)
		if errors.Is(readErr, fs.ErrNotExist) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		for _, e := range entries {
			if removeErr := removeAgedEntry(final, e); removeErr != nil {
				return removeErr
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, metaFile.Close()) }()
	// The open file pins one inode across a concurrent meta rename.
	meta, err := io.ReadAll(metaFile)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(final)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	current := store.ExportGen(meta)
	hasCurrent := slices.ContainsFunc(entries, func(e fs.DirEntry) bool { return e.Name() == current })
	for _, e := range entries {
		name := e.Name()
		if name == store.MetaFile || name == current {
			continue
		}
		if name == store.ExportDir && !hasCurrent {
			continue // the legacy flat layout still backs the current meta
		}
		if removeErr := removeAgedEntry(final, e); removeErr != nil {
			return removeErr
		}
	}
	return nil
}

func touchCurrentGeneration(final string, now time.Time) error {
	meta, err := os.ReadFile(filepath.Join(final, store.MetaFile)) //nolint:gosec // fixed path under our root
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	current := filepath.Join(final, store.ExportGen(meta))
	err = os.Chtimes(current, now, now) //nolint:gosec // current is a hash-derived name under our root
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	err = os.Chtimes(filepath.Join(final, store.ExportDir), now, now)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func removeAgedEntry(final string, entry fs.DirEntry) error {
	info, err := entry.Info()
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// Publish refreshes a generation before it becomes visible or superseded.
	if time.Since(info.ModTime()) < generationGrace {
		return nil
	}
	return os.RemoveAll(filepath.Join(final, entry.Name()))
}
