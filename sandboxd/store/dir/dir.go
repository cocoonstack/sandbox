// Package dir is the directory record backend: plain files under a
// root whose filesystem is the operator's choice — local disk keeps
// records node-local, a shared FUSE mount makes them cluster-wide.
package dir

import (
	"context"
	"errors"
	"fmt"
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
	// still cloning a generation it resolved just before a re-publish:
	// nothing superseded more recently than this is reclaimed, and "startup"
	// is per-node, never a cluster-wide quiesce point.
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

// Publish installs the staged export as an immutable generation named by the
// meta hash, then commits by renaming the meta into place. The previous
// generation stays on disk for in-flight readers; the pre-install sweep
// reclaims any superseded past the grace, so re-promotes never accumulate.
func (d *Store) Publish(_ context.Context, staging, id string) error {
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	final := filepath.Join(d.root, id)
	// Best-effort, and strictly before MkdirAll and the install: it may drop
	// crash residue wholesale, and must never see the new generation.
	_ = d.sweepGenerations(id)
	if err := os.MkdirAll(final, 0o750); err != nil {
		return err
	}
	genDir := filepath.Join(final, store.ExportGen(metaRaw))
	// An existing generation dir is a retried publish of identical meta
	// bytes — the install already happened, only the commit is left.
	switch _, statErr := os.Stat(genDir); { //nolint:gosec // G703: id pinned by the instance idRe before any call
	case errors.Is(statErr, fs.ErrNotExist):
		if err := os.Rename(filepath.Join(staging, store.ExportDir), genDir); err != nil { //nolint:gosec // G703: our own staging dir
			return err
		}
	case statErr != nil:
		return statErr
	}
	tmp := filepath.Join(final, store.MetaFile+".tmp")
	if err := os.WriteFile(tmp, metaRaw, 0o600); err != nil { //nolint:gosec // fixed path under our root
		return err
	}
	if err := os.Rename(tmp, filepath.Join(final, store.MetaFile)); err != nil {
		return err
	}
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

// SweepStaging clears crashed staging and every record's long-superseded
// generations.
func (d *Store) SweepStaging() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return err
	}
	// ReadDir + suffix, not Glob: the root path may hold glob
	// metacharacters.
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			if err := os.RemoveAll(filepath.Join(d.root, name)); err != nil {
				return err
			}
		case e.IsDir() && d.idRe.MatchString(name):
			if err := d.sweepGenerations(name); err != nil {
				return err
			}
		}
	}
	return nil
}

// sweepGenerations reclaims a record's superseded generations. The current
// meta's mtime is the supersession time of every non-current generation, so
// nothing goes until the last publish is at least generationGrace old — a
// reader that resolved older metadata has had the whole grace to finish its
// clone. An uncommitted record (no meta) past the grace is crash residue.
func (d *Store) sweepGenerations(id string) error {
	final := filepath.Join(d.root, id)
	fi, err := os.Stat(filepath.Join(final, store.MetaFile))
	if errors.Is(err, fs.ErrNotExist) {
		if dirFi, dirErr := os.Stat(final); dirErr == nil && time.Since(dirFi.ModTime()) >= generationGrace {
			return os.RemoveAll(final)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if time.Since(fi.ModTime()) < generationGrace {
		return nil
	}
	meta, err := os.ReadFile(filepath.Join(final, store.MetaFile)) //nolint:gosec // id pinned by the instance idRe
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(final)
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
		if err := os.RemoveAll(filepath.Join(final, name)); err != nil {
			return err
		}
	}
	return nil
}
