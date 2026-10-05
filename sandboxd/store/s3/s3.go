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
	"time"

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
	uploadConcurrency   = 8 // multipart parts in flight per transfer
	metaReadConcurrency = 8
	publishConcurrency  = 4 // files in parallel; each already multiparts internally
	fetchConcurrency    = 4
	fetchBudget         = 30 * time.Minute
	deleteBatch         = 1000 // DeleteObjects caps one request at 1000 keys
)

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

// Store stages locally and publishes to the bucket; idRe names the instance's id namespace.
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
func New(ctx context.Context, cfg store.S3Config, stagingRoot string, idRe *regexp.Regexp) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 checkpoint store needs a bucket")
	}
	// a prefix without its trailing slash hides every record from the delimiter listing.
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
			o.BaseEndpoint = new(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	// snapshot exports are hundreds of MB: multipart concurrency keeps transfers bandwidth-bound.
	tm := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = uploadPartSizeBytes
		o.Concurrency = uploadConcurrency
	})
	return &Store{client: client, tm: tm, bucket: cfg.Bucket, prefix: cfg.Prefix, staging: stagingRoot, idRe: idRe}, nil
}

func (s *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(s.staging, id+"-*.tmp")
}

func (s *Store) Publish(ctx context.Context, staging, id string) error {
	_, err := s.publish(ctx, staging, id, false)
	return err
}

func (s *Store) PublishDigested(ctx context.Context, staging, id string) (string, error) {
	return s.publish(ctx, staging, id, true)
}

func (s *Store) Fetch(ctx context.Context, id string) (string, []byte, string, error) {
	meta, digest, err := s.readMeta(ctx, id)
	if err != nil {
		return "", nil, "", err
	}
	gen := filepath.Join(s.staging, "cache", id, store.ExportGenHash(meta))
	export := filepath.Join(gen, store.ExportDir)
	if _, statErr := os.Stat(export); statErr == nil {
		now := time.Now()
		_ = os.Chtimes(gen, now, now)
		return export, meta, digest, nil
	}
	flight := s.fetches.DoChan(gen, func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchBudget)
		defer cancel()
		return nil, s.populate(fctx, id, meta, gen)
	})
	select {
	case res := <-flight:
		if res.Err != nil {
			return "", nil, "", res.Err
		}
	case <-ctx.Done():
		return "", nil, "", ctx.Err()
	}
	return export, meta, digest, nil
}

func (s *Store) ReadMeta(ctx context.Context, id string) ([]byte, error) {
	meta, _, err := s.readMeta(ctx, id)
	return meta, err
}

func (s *Store) Metas(ctx context.Context) ([]store.Record, error) {
	// delimiter listing yields one CommonPrefix per record instead of every export object.
	var ids []string
	p := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: &s.bucket, Prefix: &s.prefix, Delimiter: new("/"),
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
	recs := make([]store.Record, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(metaReadConcurrency)
	for i, id := range ids {
		g.Go(func() error {
			raw, digest, err := s.readMeta(gctx, id)
			if errors.Is(err, store.ErrNotFound) {
				return nil // absence mid-list is a race, not a failure
			}
			if err != nil {
				return err
			}
			labels, _, err := s.getObject(gctx, s.key(id, store.LabelsFile))
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			recs[i] = store.Record{Meta: raw, Digest: digest, Labels: labels}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(recs, func(r store.Record) bool { return r.Meta == nil }), nil
}

func (s *Store) SetLabels(ctx context.Context, id string, labels []byte) error {
	key := s.key(id, store.LabelsFile)
	if labels == nil {
		return s.deleteKeys(ctx, []string{key})
	}
	return s.uploadReader(ctx, key, bytes.NewReader(labels), int64(len(labels)), nil)
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_ = os.RemoveAll(filepath.Join(s.staging, "cache", id))
	metaKey := s.key(id, store.MetaFile)
	keys, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	// keep the commit marker so a failed export cleanup remains discoverable on retry.
	keys = slices.DeleteFunc(keys, func(key string) bool { return key == metaKey })
	if err := s.deleteKeys(ctx, keys); err != nil {
		return err
	}
	return s.deleteKeys(ctx, []string{metaKey})
}

// SweepStaging clears local staging residue at startup.
func (s *Store) SweepStaging() error {
	return utils.RemoveDirEntries(s.staging, nil) // objects orphaned before meta.json fall to the bucket's lifecycle rule
}

func (s *Store) SweepGenerations() error { return nil }

func (s *Store) readMeta(ctx context.Context, id string) ([]byte, string, error) {
	meta, metadata, err := s.getObject(ctx, s.key(id, store.MetaFile))
	if err != nil {
		return nil, "", err
	}
	return meta, metadata[digestMetadataKey], nil
}

func (s *Store) getObject(ctx context.Context, key string) ([]byte, map[string]string, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		apiErr, ok := errors.AsType[smithy.APIError](err)
		if ok && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, nil, store.ErrNotFound
		}
		return nil, nil, fmt.Errorf("object %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, err
	}
	return body, out.Metadata, nil
}

func (s *Store) publish(ctx context.Context, staging, id string, digested bool) (string, error) {
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return "", fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	files, err := s.publishFiles(staging, id, store.ExportGen(metaRaw))
	if err != nil {
		return "", err
	}
	digestFiles := make([]store.DigestFile, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(publishConcurrency)
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
	// keep old generations: another node may have selected the previous meta; Delete reclaims them.
	if err := os.RemoveAll(staging); err != nil {
		return "", err
	}
	return digest, nil
}

func (s *Store) publishFiles(staging, id, gen string) ([]publishFile, error) {
	root := filepath.Join(staging, store.ExportDir)
	var files []publishFile
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if err := store.RequireRegular(rel, info.Mode()); err != nil {
			return err
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
		return store.ErrNotFound
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fetchConcurrency)
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
	if err := os.Rename(local, gen); err != nil {
		return err
	}
	pruneIdleGenerations(filepath.Dir(gen), gen)
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
	if err := store.RequireRegular(digestPath, info.Mode()); err != nil {
		return store.DigestFile{}, err
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
		ContentLength: new(size),
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
	// DownloadObject, not GetObject: the WriterAt form downloads parts in parallel.
	if _, err := s.tm.DownloadObject(ctx, &transfermanager.DownloadObjectInput{Bucket: &s.bucket, Key: &key, WriterAt: f}); err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	return nil
}

func (s *Store) deleteKeys(ctx context.Context, keys []string) error {
	for chunk := range slices.Chunk(keys, deleteBatch) {
		objs := make([]s3types.ObjectIdentifier, len(chunk))
		for i := range chunk {
			objs[i] = s3types.ObjectIdentifier{Key: &chunk[i]}
		}
		out, err := s.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &s3types.Delete{Objects: objs, Quiet: new(true)},
		})
		if err != nil {
			return fmt.Errorf("delete %d objects: %w", len(chunk), err)
		}
		for _, e := range out.Errors {
			// strict backends error on deleting an absent key where AWS succeeds silently.
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

// pruneIdleGenerations drops cached generations of one record that no Fetch used within the grace; a hit refreshes the mtime.
func pruneIdleGenerations(dir, keep string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if info, err := entry.Info(); err == nil && path != keep && time.Since(info.ModTime()) >= store.GenerationGrace {
			_ = os.RemoveAll(path)
		}
	}
}
