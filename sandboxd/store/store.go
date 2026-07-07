// Package store persists captured sandbox states (checkpoints). The
// interface is what the pool manager needs from any backend: a staging
// area in the same publish domain, an atomic publish, a local directory
// cocoon can `clone --from-dir`, and listing/removal. The dir backend
// covers local disk and any FUSE mount (JuiceFS over object storage,
// NFS — every node sharing the mount resolves every checkpoint); the s3
// backend talks to object storage natively.
package store

import (
	"context"
	"regexp"
)

const (
	// ExportDir is the snapshot export under a checkpoint: <id>/export.
	ExportDir = "export"
	// MetaFile is the checkpoint's metadata record: <id>/meta.json. It is
	// written/uploaded last, so a lister never sees a half-published
	// checkpoint.
	MetaFile = "meta.json"
)

// IDRe pins the checkpoint id shape wherever an id reaches a backend, so
// a crafted id can never escape the store's namespace.
var IDRe = regexp.MustCompile(`^ck_[0-9a-f]{16}$`)

// Store is one checkpoint backend.
type Store interface {
	// Stage returns a writable staging directory whose Publish is atomic.
	Stage(id string) (string, error)
	// Publish atomically turns a staged directory into the checkpoint.
	// Callers pass an uncancelable ctx (WithoutCancel) — a publish must
	// finish once started.
	Publish(ctx context.Context, staging, id string) error
	// Fetch materializes a checkpoint's snapshot export as a local
	// directory cocoon can clone from, returning it with a release to call
	// when the clone is done. The dir backend returns its path with a
	// no-op release; the s3 backend downloads to a temp dir and releases
	// by removing it.
	Fetch(ctx context.Context, id string) (dir string, release func(), err error)
	// ReadMeta returns a checkpoint's metadata record, or an error when
	// the checkpoint does not exist.
	ReadMeta(ctx context.Context, id string) ([]byte, error)
	// Metas lists every checkpoint's metadata.
	Metas(ctx context.Context) ([][]byte, error)
	// Delete removes a checkpoint.
	Delete(ctx context.Context, id string) error
	// SweepStaging removes abandoned staging left by a crash mid-publish.
	SweepStaging() error
}
