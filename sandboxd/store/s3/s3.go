// Package s3 is the object-store checkpoint backend for nodes without a
// shared POSIX namespace: <prefix><id>/{export/...,meta.json} objects,
// meta.json uploaded last as the commit marker (S3 has no atomic
// multi-object rename). The aws dependency is scoped to this package.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

var _ store.Store = (*Store)(nil)

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

// Store stages locally under stagingRoot and publishes to the bucket;
// idRe names the instance's id namespace within the shared prefix.
type Store struct {
	client  *awss3.Client
	tm      *transfermanager.Client
	bucket  string
	prefix  string
	staging string
	idRe    *regexp.Regexp
}

// New builds the backend; ctx bounds the credential-chain resolution.
func New(ctx context.Context, cfg Config, stagingRoot string, idRe *regexp.Regexp) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 checkpoint store needs a bucket")
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
// the checkpoint once its commit marker exists.
func (s *Store) Publish(ctx context.Context, staging, id string) error {
	var meta string
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4) // files in parallel; each already multiparts internally
	err := filepath.Walk(staging, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(staging, path)
		if err != nil {
			return err
		}
		if rel == store.MetaFile {
			meta = path
			return nil
		}
		g.Go(func() error { return s.upload(gctx, s.key(id, rel), path) })
		return nil
	})
	if err != nil {
		return err
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if meta == "" {
		return fmt.Errorf("staging has no %s", store.MetaFile)
	}
	if err := s.upload(ctx, s.key(id, store.MetaFile), meta); err != nil {
		return err
	}
	return os.RemoveAll(staging)
}

// Fetch materializes the export locally, keeping one cached copy per id:
// records are immutable except re-publish (re-promote), so a cache whose
// stored meta still matches the bucket's is current — repeat claims pay
// one small meta GET instead of the full download. release is a no-op;
// Delete and the cache swap clean up.
func (s *Store) Fetch(ctx context.Context, id string) (string, func(), error) {
	meta, err := s.ReadMeta(ctx, id)
	if err != nil {
		return "", nil, err
	}
	cached := filepath.Join(s.staging, "cache", id)
	if have, readErr := os.ReadFile(filepath.Join(cached, store.MetaFile)); readErr == nil && bytes.Equal(have, meta) { //nolint:gosec // id pinned by idRe
		return filepath.Join(cached, store.ExportDir), func() {}, nil
	}

	local, err := os.MkdirTemp(s.staging, id+"-fetch-*")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(local) }
	exportPrefix := s.key(id, store.ExportDir) + "/"
	keys, err := s.list(ctx, exportPrefix)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if len(keys) == 0 {
		cleanup()
		return "", nil, fmt.Errorf("record %s has no export", id)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, key := range keys {
		g.Go(func() error {
			return s.download(gctx, key, filepath.Join(local, store.ExportDir, strings.TrimPrefix(key, exportPrefix)))
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return "", nil, err
	}
	if err := os.WriteFile(filepath.Join(local, store.MetaFile), meta, 0o600); err != nil {
		cleanup()
		return "", nil, err
	}
	// Swap into the cache slot; a concurrent Fetch that lost the race just
	// re-downloads next time.
	if err := os.MkdirAll(filepath.Dir(cached), 0o750); err != nil {
		cleanup()
		return "", nil, err
	}
	_ = os.RemoveAll(cached)
	if err := os.Rename(local, cached); err != nil {
		cleanup()
		return "", nil, err
	}
	return filepath.Join(cached, store.ExportDir), func() {}, nil
}

func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &s.bucket, Key: aws.String(s.key(id, store.MetaFile)),
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint %s: %w", id, err)
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(out.Body)
}

func (s *Store) Metas(ctx context.Context) ([][]byte, error) {
	keys, err := s.list(ctx, s.prefix)
	if err != nil {
		return nil, err
	}
	var metas [][]byte
	for _, key := range keys {
		rest, ok := strings.CutSuffix(key, "/"+store.MetaFile)
		if !ok {
			continue
		}
		id := rest[strings.LastIndex(rest, "/")+1:]
		if !s.idRe.MatchString(id) {
			continue
		}
		if raw, err := s.ReadMeta(ctx, id); err == nil {
			metas = append(metas, raw)
		}
	}
	return metas, nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_ = os.RemoveAll(filepath.Join(s.staging, "cache", id)) //nolint:gosec // id pinned by idRe
	keys, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}

// SweepStaging clears local staging residue; a crash between upload and
// meta.json leaves orphan objects invisible to Metas — an S3 lifecycle
// rule on the bucket reclaims those (documented in deploy).
func (s *Store) SweepStaging() error {
	entries, err := os.ReadDir(s.staging)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == "cache" {
			continue // warm fetch cache, not staging residue
		}
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
	if _, err := s.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: &s.bucket, Key: &key, Body: f}); err != nil {
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
