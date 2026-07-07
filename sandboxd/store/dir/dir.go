// Package dir is the directory checkpoint backend: plain files under a
// root whose filesystem is the operator's choice — local disk keeps
// checkpoints node-local, a shared FUSE mount makes them cluster-wide.
package dir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

var _ store.Store = (*Store)(nil)

// Store keeps checkpoints as <root>/<id>/{export,meta.json}; staging dirs
// are <root>/<id>-*.tmp siblings so publish is one rename.
type Store struct {
	root string
}

func New(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &Store{root: root}, nil
}

func (d *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(d.root, id+"-*.tmp")
}

func (d *Store) Publish(_ context.Context, staging, id string) error {
	return os.Rename(staging, filepath.Join(d.root, id))
}

func (d *Store) Fetch(_ context.Context, id string) (string, func(), error) {
	return filepath.Join(d.root, id, store.ExportDir), func() {}, nil
}

func (d *Store) ReadMeta(_ context.Context, id string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.root, id, store.MetaFile)) //nolint:gosec // id pinned by store.IDRe before any call
}

func (d *Store) Metas(ctx context.Context) ([][]byte, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var metas [][]byte
	for _, e := range entries {
		// Only well-formed checkpoint dirs — a planted foo/meta.json is
		// not a checkpoint.
		if !e.IsDir() || !store.IDRe.MatchString(e.Name()) {
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
