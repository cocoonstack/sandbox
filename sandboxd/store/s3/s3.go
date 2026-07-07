// Package s3 is the object-store checkpoint backend for nodes without a
// shared POSIX namespace: <prefix><id>/{export/...,meta.json} objects,
// meta.json uploaded last as the commit marker (S3 has no atomic
// multi-object rename). The aws dependency is scoped to this package.
package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

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

// Store stages locally under stagingRoot and publishes to the bucket.
type Store struct {
	client  *awss3.Client
	bucket  string
	prefix  string
	staging string
}

// New builds the backend; ctx bounds the credential-chain resolution.
func New(ctx context.Context, cfg Config, stagingRoot string) (*Store, error) {
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
	return &Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, staging: stagingRoot}, nil
}

func (s *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(s.staging, id+"-*.tmp")
}

// Publish uploads every staged file, meta.json last: a lister only sees
// the checkpoint once its commit marker exists.
func (s *Store) Publish(ctx context.Context, staging, id string) error {
	var meta string
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
		return s.upload(ctx, s.key(id, rel), path)
	})
	if err != nil {
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

// Fetch downloads the export into a temp dir; release removes it.
func (s *Store) Fetch(ctx context.Context, id string) (string, func(), error) {
	local, err := os.MkdirTemp(s.staging, id+"-fetch-*")
	if err != nil {
		return "", nil, err
	}
	release := func() { _ = os.RemoveAll(local) }
	exportPrefix := s.key(id, store.ExportDir) + "/"
	keys, err := s.list(ctx, exportPrefix)
	if err != nil {
		release()
		return "", nil, err
	}
	if len(keys) == 0 {
		release()
		return "", nil, fmt.Errorf("checkpoint %s has no export", id)
	}
	dir := filepath.Join(local, store.ExportDir)
	for _, key := range keys {
		if err := s.download(ctx, key, filepath.Join(dir, strings.TrimPrefix(key, exportPrefix))); err != nil {
			release()
			return "", nil, err
		}
	}
	return dir, release, nil
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
		if !store.IDRe.MatchString(id) {
			continue
		}
		if raw, err := s.ReadMeta(ctx, id); err == nil {
			metas = append(metas, raw)
		}
	}
	return metas, nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
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
	if _, err := s.client.PutObject(ctx, &awss3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: f}); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return nil
}

func (s *Store) download(ctx context.Context, key, path string) error {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	if err = os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	f, err := os.Create(path) //nolint:gosec // path derives from our own temp dir
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, out.Body)
	return err
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
