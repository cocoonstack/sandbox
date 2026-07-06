package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var _ CheckpointStore = (*dirCheckpointStore)(nil)

// CheckpointStore persists captured sandbox states. The manager needs four
// things from a backend: a staging area in the same publish domain, an
// atomic publish, a local directory cocoon can `clone --from-dir`, and
// listing/removal. A plain directory satisfies all of it — and because the
// directory's filesystem is the operator's choice, so does any FUSE mount
// (JuiceFS over S3, NFS): point checkpoint_dir at the mount and every node
// sharing it resolves every checkpoint. A native object-store backend that
// materializes exports on demand implements the same interface later.
type CheckpointStore interface {
	// Stage returns a writable staging directory whose Publish is atomic.
	Stage(id string) (string, error)
	// Publish atomically turns a staged directory into the checkpoint.
	Publish(staging, id string) error
	// ExportDir is the local path of a checkpoint's snapshot export.
	ExportDir(id string) string
	// ReadMeta returns a checkpoint's metadata record, or an error when the
	// checkpoint does not exist.
	ReadMeta(id string) ([]byte, error)
	// Metas lists every checkpoint's metadata.
	Metas() ([][]byte, error)
	// Delete removes a checkpoint.
	Delete(id string) error
	// SweepStaging removes abandoned staging left by a crash mid-publish.
	SweepStaging() error
}

// dirCheckpointStore keeps checkpoints as <root>/<id>/{export,meta.json};
// staging dirs are <root>/<id>-*.tmp siblings so publish is one rename.
type dirCheckpointStore struct {
	root string
}

func newDirCheckpointStore(root string) (*dirCheckpointStore, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &dirCheckpointStore{root: root}, nil
}

func (d *dirCheckpointStore) Stage(id string) (string, error) {
	return os.MkdirTemp(d.root, id+"-*.tmp")
}

func (d *dirCheckpointStore) Publish(staging, id string) error {
	return os.Rename(staging, filepath.Join(d.root, id))
}

func (d *dirCheckpointStore) ExportDir(id string) string {
	return filepath.Join(d.root, id, checkpointExport)
}

func (d *dirCheckpointStore) ReadMeta(id string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.root, id, "meta.json")) //nolint:gosec // id pinned by checkpointIDRe before any call
}

func (d *dirCheckpointStore) Metas() ([][]byte, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var metas [][]byte
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if raw, err := d.ReadMeta(e.Name()); err == nil {
			metas = append(metas, raw)
		}
	}
	return metas, nil
}

func (d *dirCheckpointStore) Delete(id string) error {
	return os.RemoveAll(filepath.Join(d.root, id))
}

func (d *dirCheckpointStore) SweepStaging() error {
	stale, err := filepath.Glob(filepath.Join(d.root, "*.tmp"))
	if err != nil {
		return err
	}
	for _, dir := range stale {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}
