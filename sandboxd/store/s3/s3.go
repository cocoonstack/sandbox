// Package s3 is the object-store record backend for nodes without a
// shared POSIX namespace: <prefix><id>/{export-<gen>/...,meta.json}
// objects, meta.json uploaded last as the commit marker (S3 has no atomic
// multi-object rename). The aws dependency is scoped to this package.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// Config selects the bucket and, for MinIO/R2-style endpoints, the
// addressing mode. Credentials come from the standard AWS chain (env,
// IAM role, web identity) — never from sandboxd's config file.
type Config struct {
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix,omitempty"`
	Endpoint       string `json:"endpoint,omitempty"`
	Region         string `json:"region,omitempty"`
	ForcePathStyle bool   `json:"force_path_style,omitempty"`
}

var _ store.Store = (*Store)(nil)

// Store stages locally under stagingRoot and publishes to the bucket;
// idRe names the instance's id namespace within the shared prefix.
type Store struct {
	client  *awss3.Client
	tm      *transfermanager.Client
	bucket  string
	prefix  string
	staging string
	idRe    *regexp.Regexp
	fetches singleflight.Group
}

// New builds the backend; ctx bounds the credential-chain resolution.
func New(ctx context.Context, cfg Config, stagingRoot string, idRe *regexp.Regexp) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 checkpoint store needs a bucket")
	}
	// A prefix without its trailing slash would glue onto the id and make
	// every record invisible to the delimiter listing.
	if cfg.Prefix != "" && !strings.HasSuffix(cfg.Prefix, "/") {
		cfg.Prefix += "/"
	}
	if err := os.MkdirAll(stagingRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	// Snapshot exports are hundreds of MB: multipart + concurrency keep a
	// publish/fetch bandwidth-bound instead of latency-bound.
	tm := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = 16 << 20
		o.Concurrency = 8
	})
	return &Store{client: client, tm: tm, bucket: cfg.Bucket, prefix: cfg.Prefix, staging: stagingRoot, idRe: idRe}, nil
}

func (s *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(s.staging, id+"-*.tmp")
}

// Publish uploads every staged file, meta.json last: a lister only sees
// the record once its commit marker exists. Export objects live under a
// per-generation prefix derived from the meta bytes, so a re-publish never
// overwrites keys a concurrent Fetch of the old generation is reading —
// mixed-generation downloads become impossible, not just unlikely.
func (s *Store) Publish(ctx context.Context, staging, id string) error {
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	gen := exportGen(metaRaw)
	fresh := map[string]struct{}{s.key(id, store.MetaFile): {}}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4) // files in parallel; each already multiparts internally
	err = filepath.WalkDir(staging, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(staging, path)
		if err != nil {
			return err
		}
		if rel == store.MetaFile {
			return nil
		}
		key := s.key(id, gen+strings.TrimPrefix(rel, store.ExportDir))
		fresh[key] = struct{}{}
		g.Go(func() error { return s.upload(gctx, key, path) })
		return nil
	})
	if err != nil {
		return err
	}
	if err = g.Wait(); err != nil {
		return err
	}
	if err = s.uploadReader(ctx, s.key(id, store.MetaFile), bytes.NewReader(metaRaw)); err != nil {
		return err
	}
	// A re-publish (re-promote) may ship a different export file set:
	// after the new meta commits, sweep keys the new generation did not
	// write, or Fetch would download the union of generations.
	keys, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	var stale []string
	for _, key := range keys {
		if _, ok := fresh[key]; !ok {
			stale = append(stale, key)
		}
	}
	if err := s.deleteKeys(ctx, stale); err != nil {
		return err
	}
	return os.RemoveAll(staging)
}

// Fetch materializes the export into a local cache generation keyed by the
// record's meta hash: an unchanged record's repeat fetch is one meta GET, and
// a new generation never disturbs a directory an in-flight clone is reading
// (old generations are reaped at Delete and startup). Concurrent misses share
// one download; release is a no-op; a missing id is ErrNotFound.
func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, func(), error) {
	meta, err := s.ReadMeta(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}
	gen := filepath.Join(s.staging, "cache", id, exportGenHash(meta))
	export := filepath.Join(gen, store.ExportDir)
	if _, statErr := os.Stat(export); statErr == nil {
		return export, meta, func() {}, nil
	}
	_, err, _ = s.fetches.Do(gen, func() (any, error) {
		return nil, s.populate(ctx, id, meta, gen)
	})
	if err != nil {
		return "", nil, nil, err
	}
	return export, meta, func() {}, nil
}

// populate downloads one cache generation and installs it atomically.
func (s *Store) populate(ctx context.Context, id string, meta []byte, gen string) error {
	if _, err := os.Stat(gen); err == nil {
		return nil // another flight installed it between stat and Do
	}
	local, err := os.MkdirTemp(s.staging, id+"-fetch-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(local) }()
	exportPrefix := s.key(id, exportGen(meta)) + "/"
	keys, err := s.list(ctx, exportPrefix)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		// Records published before per-generation prefixes.
		exportPrefix = s.key(id, store.ExportDir) + "/"
		if keys, err = s.list(ctx, exportPrefix); err != nil {
			return err
		}
	}
	if len(keys) == 0 {
		return fmt.Errorf("record %s has no export", id)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, key := range keys {
		g.Go(func() error {
			return s.download(gctx, key, filepath.Join(local, store.ExportDir, strings.TrimPrefix(key, exportPrefix)))
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(local, store.MetaFile), meta, 0o600); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(gen), 0o750); err != nil {
		return err
	}
	return os.Rename(local, gen)
}

func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &s.bucket, Key: aws.String(s.key(id, store.MetaFile)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("record %s: %w", id, err)
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(out.Body)
}

func (s *Store) Metas(ctx context.Context) ([][]byte, error) {
	// Delimiter listing yields one CommonPrefix per record instead of
	// walking every export object of both namespaces under the prefix.
	var ids []string
	p := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: &s.bucket, Prefix: &s.prefix, Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", s.prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			id := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, s.prefix), "/")
			if s.idRe.MatchString(id) {
				ids = append(ids, id)
			}
		}
	}
	metas := make([][]byte, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, id := range ids {
		g.Go(func() error {
			raw, err := s.ReadMeta(gctx, id)
			if err == nil {
				metas[i] = raw
			} else if !errors.Is(err, store.ErrNotFound) {
				return err // absence mid-list is a race, not a failure
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(metas, func(m []byte) bool { return m == nil }), nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_ = os.RemoveAll(filepath.Join(s.staging, "cache", id)) //nolint:gosec // id pinned by idRe
	// Uncommit first: dropping meta.json makes the record invisible to
	// loads before any export object disappears under a concurrent fetch.
	if err := s.deleteKeys(ctx, []string{s.key(id, store.MetaFile)}); err != nil {
		return err
	}
	keys, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	return s.deleteKeys(ctx, keys)
}

// SweepStaging clears local staging residue AND stale cache generations —
// it runs at startup, when no clone can be mid-flight. A crash between
// upload and meta.json leaves orphan objects invisible to Metas; an S3
// lifecycle rule on the bucket reclaims those (documented in deploy).
func (s *Store) SweepStaging() error {
	entries, err := os.ReadDir(s.staging)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.staging, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) key(id, rest string) string {
	if rest == "" {
		return s.prefix + id
	}
	return s.prefix + id + "/" + filepath.ToSlash(rest)
}

func (s *Store) upload(ctx context.Context, key, path string) error {
	f, err := os.Open(path) //nolint:gosec // path walked from our own staging dir
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return s.uploadReader(ctx, key, f)
}

func (s *Store) uploadReader(ctx context.Context, key string, body io.Reader) error {
	if _, err := s.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: &s.bucket, Key: &key, Body: body}); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}

func (s *Store) download(ctx context.Context, key, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	f, err := os.Create(path) //nolint:gosec // path derives from our own temp dir
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	// DownloadObject, not GetObject: the WriterAt form downloads parts in
	// parallel; GetObject's io.Reader is a single sequential stream.
	if _, err := s.tm.DownloadObject(ctx, &transfermanager.DownloadObjectInput{Bucket: &s.bucket, Key: &key, WriterAt: f}); err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	return nil
}

func (s *Store) deleteKeys(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if _, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}

func (s *Store) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

// exportGen names a record generation's export prefix from its meta bytes —
// meta is unique per publish (checkpoint ids are fresh, template records
// carry created_at), and Fetch keys its cache generation off the same hash.
func exportGen(meta []byte) string {
	return store.ExportDir + "-" + exportGenHash(meta)
}

func exportGenHash(meta []byte) string {
	sum := sha256.Sum256(meta)
	return hex.EncodeToString(sum[:8])
}
