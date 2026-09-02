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
	// MetaFile is the record's metadata, written last so a lister never sees a half-published record.
	MetaFile = "meta.json"
)

var (
	// ErrNotFound is the absence signal every backend normalizes its missing-record error to.
	ErrNotFound = errors.New("record not found")

	// CheckpointIDRe and TemplateIDRe let both id namespaces share one store root.
	CheckpointIDRe = regexp.MustCompile(`^ck_[0-9a-f]{16}$`)
	TemplateIDRe   = regexp.MustCompile(`^tp_[0-9a-f]{32}$`)
)

// Store is one record backend.
type Store interface {
	// Stage returns a writable staging directory whose Publish is atomic.
	Stage(id string) (string, error)
	// Publish atomically replaces the record's previous generation with the staged directory.
	Publish(ctx context.Context, staging, id string) error
	// PublishDigested applies Publish semantics and returns the export digest.
	PublishDigested(ctx context.Context, staging, id string) (string, error)
	// Fetch materializes a record's export locally and returns a release to hold until the clone ends.
	Fetch(ctx context.Context, id string) (dir string, meta []byte, digest string, release func(), err error)
	// ReadMeta returns a record's metadata, or an error when the record does not exist.
	ReadMeta(ctx context.Context, id string) ([]byte, error)
	// Metas lists the metadata of every record in this instance's id namespace.
	Metas(ctx context.Context) ([][]byte, error)
	// Delete removes a record.
	Delete(ctx context.Context, id string) error
	// SweepStaging removes abandoned staging left by a crash mid-publish.
	SweepStaging() error
	// SweepGenerations runs generation retention without disturbing staging or fetch caches.
	SweepGenerations() error
}

// CheckpointID, TemplateID, and TemplateHash are the one home for the ck_/tp_ id scheme.
func CheckpointID(suffix string) string { return "ck_" + suffix }

func TemplateID(hash string) string { return "tp_" + hash }

func TemplateHash(id string) string { return strings.TrimPrefix(id, "tp_") }

// ExportGen names a record generation's export dir from its meta bytes, unique per publish.
func ExportGen(meta []byte) string { return ExportDir + "-" + ExportGenHash(meta) }

func ExportGenHash(meta []byte) string {
	sum := sha256.Sum256(meta)
	return hex.EncodeToString(sum[:8])
}
