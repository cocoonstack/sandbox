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
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

const oldSuffix = ".old"

var _ store.Store = (*Store)(nil)

// Store keeps records as <root>/<id>/{export,meta.json}; staging dirs are
// <root>/<id>-*.tmp siblings so publish is one rename. idRe names the
// instance's id namespace — two instances (checkpoints, templates) share a
// root without seeing each other's records.
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

func (d *Store) Publish(_ context.Context, staging, id string) error {
	final := filepath.Join(d.root, id)
	old := final + oldSuffix
	// Re-publish (re-promote) replaces by swap, not delete-then-rename: the
	// old generation survives as <id>.old until the new one is in place, so a
	// crash in between loses nothing (SweepStaging restores it). An absent
	// final with a live <id>.old is an unswept crash artifact — install first,
	// clear it only after success.
	switch _, statErr := os.Stat(final); {
	case errors.Is(statErr, fs.ErrNotExist):
		if err := os.Rename(staging, final); err != nil {
			return err
		}
		_ = os.RemoveAll(old)
		return nil
	case statErr != nil:
		return statErr
	}
	if err := os.RemoveAll(old); err != nil {
		return err
	}
	if err := os.Rename(final, old); err != nil {
		return err
	}
	if err := os.Rename(staging, final); err != nil {
		_ = os.Rename(old, final)
		return err
	}
	_ = os.RemoveAll(old) // leftovers are reclaimed by SweepStaging
	return nil
}

func (d *Store) Fetch(ctx context.Context, id string) (string, []byte, func(), error) {
	// Meta is the commit marker: a half-published record stays invisible.
	meta, err := d.ReadMeta(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}
	dir := filepath.Join(d.root, id, store.ExportDir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return "", nil, nil, store.ErrNotFound
	} else if err != nil {
		return "", nil, nil, err
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

// Delete also clears an unswept <id>.old so a sweep cannot resurrect the record.
func (d *Store) Delete(_ context.Context, id string) error {
	final := filepath.Join(d.root, id)
	return errors.Join(os.RemoveAll(final), os.RemoveAll(final+oldSuffix))
}

func (d *Store) SweepStaging() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return err
	}
	// ReadDir + suffix, not Glob: the root path may hold glob
	// metacharacters.
	for _, e := range entries {
		switch name := e.Name(); {
		case strings.HasSuffix(name, ".tmp"):
			if err := os.RemoveAll(filepath.Join(d.root, name)); err != nil {
				return err
			}
		case strings.HasSuffix(name, oldSuffix):
			// A crash mid-Publish: restore the moved-aside generation when
			// the swap never completed, drop it once the new one is live.
			final := filepath.Join(d.root, strings.TrimSuffix(name, oldSuffix))
			if _, err := os.Stat(final); errors.Is(err, fs.ErrNotExist) {
				if err := os.Rename(filepath.Join(d.root, name), final); err != nil {
					return err
				}
				continue
			}
			if err := os.RemoveAll(filepath.Join(d.root, name)); err != nil {
				return err
			}
		}
	}
	return nil
}
