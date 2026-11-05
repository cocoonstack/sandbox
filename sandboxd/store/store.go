// Package store defines the record backend interface and the digest, chunk and id helpers its dir and s3 backends share.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	// ExportDir is the snapshot export inside a staging dir; publish renames it to its ExportGen.
	ExportDir = "export"
	// MetaFile is the record's metadata, written last so a lister never sees a half-published record.
	MetaFile = "meta.json"
	// LabelsFile holds a record's labels beside its meta, which names the export generation and so cannot change.
	LabelsFile = "labels.json"

	// GenerationGrace bounds the race where a node still clones a generation a re-publish superseded.
	GenerationGrace = time.Hour
)

var (
	// ErrNotFound is the absence signal every backend normalizes its missing-record error to.
	ErrNotFound = errors.New("record not found")

	// CheckpointIDRe and TemplateIDRe let both id namespaces share one store root.
	CheckpointIDRe = regexp.MustCompile(`^ck_[0-9a-f]{16}$`)
	TemplateIDRe   = regexp.MustCompile(`^tp_[0-9a-f]{32}$`)
)

// Record is one published record's metadata, its export digest (empty when published undigested) and its labels, nil when none are set.
type Record struct {
	Meta   []byte
	Digest string
	Labels []byte
}

// S3Config selects the bucket of the s3 backend; credentials come from the standard AWS chain, never this config.
type S3Config struct {
	Bucket                 string `json:"bucket"`
	Prefix                 string `json:"prefix,omitempty"`
	Endpoint               string `json:"endpoint,omitempty"`
	Region                 string `json:"region,omitempty"`
	ForcePathStyle         bool   `json:"force_path_style,omitzero"`
	UnsignedAcceptEncoding bool   `json:"unsigned_accept_encoding,omitzero"`
}

// CommitFunc makes a prepared generation the record's current one.
type CommitFunc func(ctx context.Context) error

// Store is one record backend.
type Store interface {
	// Stage returns a writable staging directory whose Publish is atomic.
	Stage(id string) (string, error)
	// Publish atomically replaces the record's previous generation with the staged directory; lineage scopes chunk dedup in a content-addressed backend and "" means the record id.
	Publish(ctx context.Context, staging, id, lineage string) error
	// PrepareDigested runs a Publish up to the step that makes the generation current, and returns the export digest and that step.
	PrepareDigested(ctx context.Context, staging, id, lineage string) (string, CommitFunc, error)
	// Fetch materializes a record's export locally; the caller holds the record lock until the clone ends.
	Fetch(ctx context.Context, id string) (dir string, meta []byte, digest string, err error)
	// FetchFile writes one file of a record's export to dst, without fetching the rest of the record; dst is the caller's to delete.
	FetchFile(ctx context.Context, id, rel, dst string) error
	// ReadMeta returns a record's metadata, or an error when the record does not exist.
	ReadMeta(ctx context.Context, id string) ([]byte, error)
	// ReadDigest returns a record's export digest, empty when it was published undigested.
	ReadDigest(ctx context.Context, id string) (string, error)
	// Metas lists the metadata and digest of every record in this instance's id namespace.
	Metas(ctx context.Context) ([]Record, error)
	// SetLabels replaces a record's labels; nil removes them. The caller holds the record lock.
	SetLabels(ctx context.Context, id string, labels []byte) error
	Delete(ctx context.Context, id string) error
	// SweepStaging clears abandoned staging, and a backend's local fetch cache, at startup.
	SweepStaging() error
	// SweepGenerations runs generation retention, sparing staging and fetch caches used within GenerationGrace.
	SweepGenerations(ctx context.Context) error
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

// WalkExport calls fn for every file under a staged export root with its slash-separated path relative to root; any non-regular entry fails the walk.
func WalkExport(root string, fn func(rel, path string, info fs.FileInfo) error) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			if !entry.IsDir() {
				return errors.New("export root is not a directory")
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("export entry %s is not a regular file (%s)", rel, info.Mode().Type())
		}
		return fn(rel, path, info)
	})
}
