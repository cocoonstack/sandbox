// Package s3 is the object-store record backend for nodes without a
// shared POSIX namespace: <prefix><id>/{export-<gen>/...,meta.json}
// objects, meta.json uploaded last as the commit marker (S3 has no atomic
// multi-object rename). The aws dependency is scoped to this package.
package s3

import (
	"bytes"
	"context"
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
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	digestMetadataKey   = "content-digest"
	uploadPartSizeBytes = 16 << 20
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

type publishFile struct {
	path       string
	key        string
	digestPath string
}

type transferManager interface {
	UploadObject(context.Context, *transfermanager.UploadObjectInput, ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
	DownloadObject(context.Context, *transfermanager.DownloadObjectInput, ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error)
}

var _ store.Store = (*Store)(nil)

// Store stages locally under stagingRoot and publishes to the bucket;
// idRe names the instance's id namespace within the shared prefix.
type Store struct {
	client  *awss3.Client
	tm      transferManager
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
		o.PartSizeBytes = uploadPartSizeBytes
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
	_, err := s.publish(ctx, staging, id, false)
	return err
}

func (s *Store) PublishDigested(ctx context.Context, staging, id string) (string, error) {
	return s.publish(ctx, staging, id, true)
}

// Fetch materializes the export into a local cache generation keyed by the
// record's meta hash: an unchanged record's repeat fetch is one meta GET, and
// a new generation never disturbs a directory an in-flight clone is reading
// (old generations are reaped at Delete and startup). Concurrent misses share
// one download; release is a no-op; a missing id is ErrNotFound.
func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, string, func(), error) {
	meta, digest, err := s.readMeta(ctx, id)
	if err != nil {
		return "", nil, "", nil, err
	}
	gen := filepath.Join(s.staging, "cache", id, store.ExportGenHash(meta))
	export := filepath.Join(gen, store.ExportDir)
	if _, statErr := os.Stat(export); statErr == nil {
		return export, meta, digest, func() {}, nil
	}
	_, err, _ = s.fetches.Do(gen, func() (any, error) {
		return nil, s.populate(ctx, id, meta, gen)
	})
	if err != nil {
		return "", nil, "", nil, err
	}
	return export, meta, digest, func() {}, nil
}

func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	meta, _, err := s.readMeta(ctx, id)
	return meta, err
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
	metaKey := s.key(id, store.MetaFile)
	keys, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	// Keep the commit marker so a failed export cleanup remains discoverable on retry.
	keys = slices.DeleteFunc(keys, func(key string) bool { return key == metaKey })
	if err := s.deleteKeys(ctx, keys); err != nil {
		return err
	}
	return s.deleteKeys(ctx, []string{metaKey})
}

// SweepStaging clears local staging residue AND stale cache generations —
// it runs at startup, when no clone can be mid-flight. A crash between
// upload and meta.json leaves orphan objects invisible to Metas; an S3
// lifecycle rule on the bucket reclaims those (documented in deploy).
func (s *Store) SweepStaging() error { return utils.RemoveDirEntries(s.staging, nil) }

// SweepGenerations is a no-op: Delete reclaims committed S3 generations, and
// bucket lifecycle policy handles invisible upload orphans.
func (s *Store) SweepGenerations() error { return nil }

func (s *Store) readMeta(ctx context.Context, id string) ([]byte, string, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &s.bucket, Key: aws.String(s.key(id, store.MetaFile)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") { //nolint:goconst // AWS API error codes, compared as literals
			return nil, "", store.ErrNotFound
		}
		return nil, "", fmt.Errorf("record %s: %w", id, err)
	}
	defer func() { _ = out.Body.Close() }()
	meta, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	return meta, out.Metadata[digestMetadataKey], nil
}

func (s *Store) publish(ctx context.Context, staging, id string, digested bool) (string, error) {
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return "", fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	files, err := s.publishFiles(staging, id, store.ExportGen(metaRaw), digested)
	if err != nil {
		return "", err
	}
	digestFiles := make([]store.DigestFile, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4) // files in parallel; each already multiparts internally
	for i, file := range files {
		g.Go(func() error {
			if !digested {
				return s.upload(gctx, file.key, file.path)
			}
			fileDigest, digestErr := s.uploadDigested(gctx, file.key, file.path, file.digestPath)
			if digestErr == nil {
				digestFiles[i] = fileDigest
			}
			return digestErr
		})
	}
	if err = g.Wait(); err != nil {
		return "", err
	}
	digest := ""
	var metadata map[string]string
	if digested {
		digest, err = store.AssembleDigest(digestFiles)
		if err != nil {
			return "", err
		}
		metadata = map[string]string{digestMetadataKey: digest}
	}
	if err = s.uploadReader(ctx, s.key(id, store.MetaFile), bytes.NewReader(metaRaw), int64(len(metaRaw)), metadata); err != nil {
		return "", err
	}
	// Keep old generations because another node may have selected the previous meta; Delete reclaims them.
	if err := os.RemoveAll(staging); err != nil {
		return "", err
	}
	return digest, nil
}

func (s *Store) publishFiles(staging, id, gen string, digested bool) ([]publishFile, error) {
	root := staging
	if digested {
		root = filepath.Join(staging, store.ExportDir)
	}
	var files []publishFile
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !digested {
			if rel == store.MetaFile {
				return nil
			}
			files = append(files, publishFile{
				path: path,
				key:  s.key(id, gen+strings.TrimPrefix(rel, store.ExportDir)),
			})
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !info.Mode().IsRegular() {
			return fmt.Errorf("digest export %s is not a regular file", rel)
		}
		files = append(files, publishFile{
			path:       path,
			key:        s.key(id, gen+"/"+rel),
			digestPath: rel,
		})
		return nil
	})
	return files, err
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
	exportPrefix := s.key(id, store.ExportGen(meta)) + "/"
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
	info, err := f.Stat()
	if err != nil {
		return err
	}
	return s.uploadReader(ctx, key, f, info.Size(), nil)
}

func (s *Store) uploadDigested(ctx context.Context, key, path, digestPath string) (store.DigestFile, error) {
	f, err := os.Open(path) //nolint:gosec // path walked from our own staging dir
	if err != nil {
		return store.DigestFile{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return store.DigestFile{}, err
	}
	if !info.Mode().IsRegular() {
		return store.DigestFile{}, fmt.Errorf("digest export %s is not a regular file", digestPath)
	}
	reader := newDigestReader(f, digestPath, info.Size())
	if err := s.uploadReader(ctx, key, reader, info.Size(), nil); err != nil {
		return store.DigestFile{}, err
	}
	return reader.Digest()
}

func (s *Store) uploadReader(ctx context.Context, key string, body io.Reader, size int64, metadata map[string]string) error {
	input := &transfermanager.UploadObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		Body:          body,
		ContentLength: aws.Int64(size),
		Metadata:      metadata,
	}
	if _, err := s.tm.UploadObject(ctx, input); err != nil {
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
	// DeleteObjects caps one batch at 1000 keys.
	for chunk := range slices.Chunk(keys, 1000) {
		objs := make([]s3types.ObjectIdentifier, len(chunk))
		for i := range chunk {
			objs[i] = s3types.ObjectIdentifier{Key: &chunk[i]}
		}
		out, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &s3types.Delete{Objects: objs, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete %d objects: %w", len(chunk), err)
		}
		for _, e := range out.Errors {
			// Strict backends report deleting an absent key per-entry where
			// AWS succeeds silently; tolerating it keeps Delete retries
			// convergent, matching single-object DeleteObject semantics.
			if code := aws.ToString(e.Code); code == "NoSuchKey" || code == "NoSuchVersion" {
				continue
			}
			return fmt.Errorf("delete %s: %s", aws.ToString(e.Key), aws.ToString(e.Message))
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
