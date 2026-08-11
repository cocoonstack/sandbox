// Package store persists captured sandbox states — checkpoints and promoted
// templates — behind one interface: staging, an atomic publish, a local
// directory cocoon can `clone --from-dir`, and listing/removal. The dir
// backend covers local disk and any shared FUSE mount (every node sharing it
// resolves every record); the s3 backend talks to object storage natively.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

const (
	// ExportDir is the snapshot export under a record: <id>/export.
	ExportDir = "export"
	// MetaFile is the record's metadata: <id>/meta.json. It is
	// written/uploaded last, so a lister never sees a half-published
	// record.
	MetaFile = "meta.json"
)

var (
	// ErrNotFound is the one absence signal: backends normalize their native
	// missing-record errors to it, so call sites can tell "gone" from a
	// backend failure (which must never silently degrade to a cold boot).
	ErrNotFound = errors.New("record not found")

	// The two id namespaces sharing one store root: backends filter listings
	// by their instance's regexp, so checkpoints and templates coexist in the
	// same directory/bucket without seeing each other. The pins also keep a
	// crafted id from escaping the store's namespace.
	CheckpointIDRe = regexp.MustCompile(`^ck_[0-9a-f]{16}$`)
	TemplateIDRe   = regexp.MustCompile(`^tp_[0-9a-f]{32}$`)
)

// Store is one record backend.
type Store interface {
	// Stage returns a writable staging directory whose Publish is atomic.
	Stage(id string) (string, error)
	// Publish turns a staged directory into the record, replacing any
	// previous generation atomically for listers and never disturbing one a
	// concurrent Fetch pinned (generations are immutable and retained until
	// Delete or a graced sweep). The caller serializes same-id operations
	// in-process; request-path callers pass an uncancelable ctx so a started
	// publish finishes.
	Publish(ctx context.Context, staging, id string) error
	// Fetch materializes a record's snapshot export as a local directory
	// cocoon can clone from, plus the meta it resolved on the way, and a
	// release pinning that exact generation — hold it until the clone is done.
	Fetch(ctx context.Context, id string) (dir string, meta []byte, release func(), err error)
	// ReadMeta returns a record's metadata, or an error when the record
	// does not exist.
	ReadMeta(ctx context.Context, id string) ([]byte, error)
	// Metas lists the metadata of every record in this instance's id
	// namespace.
	Metas(ctx context.Context) ([][]byte, error)
	// Delete removes a record.
	Delete(ctx context.Context, id string) error
	// SweepStaging removes abandoned staging left by a crash mid-publish.
	SweepStaging() error
	// SweepGenerations runs backend-specific generation retention without
	// disturbing staging or active fetch caches.
	SweepGenerations() error
}

// CheckpointID, TemplateID, and TemplateHash are the one home for the
// ck_/tp_ id scheme the regexps above pin.
func CheckpointID(suffix string) string { return "ck_" + suffix }

func TemplateID(hash string) string { return "tp_" + hash }

func TemplateHash(id string) string { return strings.TrimPrefix(id, "tp_") }

// ExportGen names a record generation's export dir/prefix from its meta
// bytes — meta is unique per publish (checkpoint ids are fresh, template
// records carry created_at), so a re-publish never collides with a
// generation a concurrent Fetch is reading.
func ExportGen(meta []byte) string { return ExportDir + "-" + ExportGenHash(meta) }

func ExportGenHash(meta []byte) string {
	sum := sha256.Sum256(meta)
	return hex.EncodeToString(sum[:8])
}
