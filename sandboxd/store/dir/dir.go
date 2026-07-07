// Package dir is the directory record backend: plain files under a
// root whose filesystem is the operator's choice — local disk keeps
// records node-local, a shared FUSE mount makes them cluster-wide.
package dir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

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
	// Re-publish (re-promote) replaces: drop the old generation first, or
	// the rename fails against the existing dir.
	if err := os.RemoveAll(final); err != nil {
		return err
	}
	return os.Rename(staging, final)
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
		if raw, err := d.ReadMeta(ctx, e.Name()); err == nil {
			metas = append(metas, raw)
		}
	}
	return metas, nil
}

func (d *Store) Delete(_ context.Context, id string) error {
	return os.RemoveAll(filepath.Join(d.root, id))
}

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
	return nil
}
