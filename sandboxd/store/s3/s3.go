// Package s3 is the object-store record backend for nodes without a
// shared POSIX namespace: <prefix><id>/{export-<gen>/manifest.json,meta.json}
// plus the content-addressed chunks the manifests name, meta.json uploaded
// last as the commit marker (S3 has no atomic multi-object rename). Export
// files are stored as 16 MiB chunks keyed by SHA-256; holes and all-zero
// chunks are absent. The aws dependency is scoped to this package.
package s3

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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
	manifestName        = "manifest.json"
	chunksDir           = "chunks"
	lineagesDir         = "lineages"
	uploadConcurrency   = 8 // chunk PUTs in flight per publish
	metaReadConcurrency = 8
	publishConcurrency  = 4 // files scanned in parallel
	fetchConcurrency    = 8 // chunk GETs in flight per fetch
	fetchBudget         = 30 * time.Minute
	deleteBatch         = 1000 // DeleteObjects caps one request at 1000 keys
)

var chunkBufs = sync.Pool{New: func() any {
	buf := make([]byte, store.DigestChunkSize)
	return &buf
}}

type publishFile struct {
	path string
	rel  string
}

type manifest struct {
	Lineage string         `json:"lineage"`
	Files   []manifestFile `json:"files"`
}

type manifestFile struct {
	Path   string          `json:"path"`
	Size   int64           `json:"size"`
	Chunks []manifestChunk `json:"chunks"`
}

type manifestChunk struct {
	Index  int64  `json:"index"`
	SHA256 string `json:"sha256"`
}

type recordObjects struct {
	meta      *s3types.Object
	manifests map[string]s3types.Object // by generation
}

var _ store.Store = (*Store)(nil)

// Store stages locally and publishes to the bucket; idRe names the instance's id namespace.
type Store struct {
	client  *awss3.Client
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
	return &Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, staging: stagingRoot, idRe: idRe}, nil
}

func (s *Store) Stage(id string) (string, error) {
	return os.MkdirTemp(s.staging, id+"-*.tmp")
}

func (s *Store) Publish(ctx context.Context, staging, id, lineage string) error {
	_, err := s.publish(ctx, staging, id, lineage, false)
	return err
}

func (s *Store) PublishDigested(ctx context.Context, staging, id, lineage string) (string, error) {
	return s.publish(ctx, staging, id, lineage, true)
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
	ids, err := s.recordIDs(ctx)
	if err != nil {
		return nil, err
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
	return s.putObject(ctx, key, labels, nil)
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_ = os.RemoveAll(filepath.Join(s.staging, "cache", id))
	objs, err := s.list(ctx, s.key(id, "")+"/")
	if err != nil {
		return err
	}
	for _, key := range objectKeys(objs) {
		if strings.HasSuffix(key, "/"+manifestName) {
			if err = s.refreshObject(ctx, key); err != nil {
				return err
			}
		}
	}
	if err = s.deleteKeys(ctx, []string{s.key(id, store.LabelsFile)}); err != nil {
		return err
	}
	return s.deleteKeys(ctx, []string{s.key(id, store.MetaFile)})
}

// SweepStaging clears local staging residue at startup.
func (s *Store) SweepStaging() error {
	return utils.RemoveDirEntries(s.staging, nil)
}

func (s *Store) SweepGenerations(ctx context.Context) error {
	objs, err := s.list(ctx, s.prefix)
	if err != nil {
		return err
	}
	inv := newInventory(s.prefix, objs)
	now := time.Now()
	refs := &chunkRefs{live: map[string]map[string]struct{}{}}
	var doomed []string
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(metaReadConcurrency)
	for _, rec := range inv.records {
		g.Go(func() error {
			dead, sweepErr := s.sweepRecord(gctx, rec, now, refs)
			mu.Lock()
			doomed = append(doomed, dead...)
			mu.Unlock()
			return sweepErr
		})
	}
	if err = g.Wait(); err != nil {
		return err
	}
	if err = s.deleteKeys(ctx, doomed); err != nil {
		return err
	}
	dead := make(map[string]struct{}, len(doomed))
	for _, key := range doomed {
		dead[key] = struct{}{}
	}
	var stale []string
	for _, marker := range inv.markers {
		if idle(marker.LastModified, now) && !inv.alive(path.Base(*marker.Key), dead) {
			stale = append(stale, *marker.Key)
		}
	}
	for lineage, chunks := range inv.chunks {
		for _, obj := range chunks {
			if !refs.has(lineage, path.Base(*obj.Key)) && idle(obj.LastModified, now) {
				stale = append(stale, *obj.Key)
			}
		}
	}
	return s.deleteKeys(ctx, stale)
}

func (s *Store) readMeta(ctx context.Context, id string) ([]byte, string, error) {
	meta, out, err := s.getObject(ctx, s.key(id, store.MetaFile))
	if err != nil {
		return nil, "", err
	}
	return meta, out.Metadata[digestMetadataKey], nil
}

func (s *Store) currentManifest(ctx context.Context, id string) (manifest, error) {
	meta, _, err := s.getObject(ctx, s.key(id, store.MetaFile))
	if err != nil {
		return manifest{}, err
	}
	return s.readManifest(ctx, id, store.ExportGen(meta))
}

func (s *Store) readManifest(ctx context.Context, id, gen string) (manifest, error) {
	raw, _, err := s.getObject(ctx, s.key(id, gen+"/"+manifestName))
	if err != nil {
		return manifest{}, err
	}
	var man manifest
	if err := json.Unmarshal(raw, &man); err != nil {
		return manifest{}, fmt.Errorf("decode manifest of %s/%s: %w", id, gen, err)
	}
	return man, nil
}

func (s *Store) getObject(ctx context.Context, key string) ([]byte, *awss3.GetObjectOutput, error) {
	out, err := s.get(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, err
	}
	return body, out, nil
}

func (s *Store) get(ctx context.Context, key string) (*awss3.GetObjectOutput, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		apiErr, ok := errors.AsType[smithy.APIError](err)
		if ok && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("object %s: %w", key, err)
	}
	return out, nil
}

func (s *Store) publish(ctx context.Context, staging, id, lineage string, digested bool) (string, error) {
	lineage = cmp.Or(lineage, id)
	metaRaw, err := os.ReadFile(filepath.Join(staging, store.MetaFile)) //nolint:gosec // our own staging dir
	if err != nil {
		return "", fmt.Errorf("staging has no %s: %w", store.MetaFile, err)
	}
	gen := store.ExportGen(metaRaw)
	files, err := publishFiles(staging)
	if err != nil {
		return "", err
	}
	have, err := s.referencedChunks(ctx, id, lineage)
	if err != nil {
		return "", err
	}
	man := manifest{Lineage: lineage, Files: make([]manifestFile, len(files))}
	digestFiles := make([]store.DigestFile, len(files))
	chunkGroup, chunkCtx := errgroup.WithContext(ctx)
	chunkGroup.SetLimit(uploadConcurrency)
	up := &chunkUploader{store: s, group: chunkGroup, ctx: chunkCtx, lineage: lineage, have: have}
	fileGroup, fileCtx := errgroup.WithContext(chunkCtx)
	fileGroup.SetLimit(publishConcurrency)
	for i, file := range files {
		fileGroup.Go(func() (err error) {
			digestFiles[i], man.Files[i], err = up.scan(fileCtx, file)
			return err
		})
	}
	fileErr := fileGroup.Wait()
	if err = chunkGroup.Wait(); err == nil {
		err = fileErr
	}
	if err != nil {
		return "", err
	}
	manifestRaw, err := json.Marshal(man)
	if err != nil {
		return "", fmt.Errorf("encode manifest: %w", err)
	}
	if err = s.putObject(ctx, s.key(id, gen+"/"+manifestName), manifestRaw, nil); err != nil {
		return "", err
	}
	if lineage != id {
		if err = s.putObject(ctx, s.lineageKey(lineage, id), nil, nil); err != nil {
			return "", err
		}
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
	if err = s.putObject(ctx, s.key(id, store.MetaFile), metaRaw, metadata); err != nil {
		return "", err
	}
	// keep old generations: another node may have selected the previous meta; the sweep reclaims them.
	if err := os.RemoveAll(staging); err != nil {
		return "", err
	}
	return digest, nil
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
	exportGen := store.ExportGen(meta)
	man, err := s.readManifest(ctx, id, exportGen)
	if err != nil {
		return err
	}
	if err := s.materialize(ctx, man, filepath.Join(local, store.ExportDir)); err != nil {
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

// materialize sizes each file to its logical length and writes only its present chunks, so holes stay holes.
func (s *Store) materialize(ctx context.Context, man manifest, root string) (err error) {
	handles := make([]*os.File, len(man.Files))
	defer func() {
		for _, f := range handles {
			if f != nil {
				err = errors.Join(err, f.Close())
			}
		}
	}()
	for i, file := range man.Files {
		target := filepath.Join(root, filepath.FromSlash(file.Path))
		if err = os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		if handles[i], err = os.Create(target); err != nil { //nolint:gosec // path derives from our own temp dir
			return err
		}
		if err = handles[i].Truncate(file.Size); err != nil {
			return err
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fetchConcurrency)
	for i, file := range man.Files {
		for _, chunk := range file.Chunks {
			g.Go(func() error {
				return s.fetchChunk(gctx, s.chunkKey(man.Lineage, chunk.SHA256), handles[i], file.Size, chunk)
			})
		}
	}
	return g.Wait()
}

func (s *Store) fetchChunk(ctx context.Context, key string, f *os.File, size int64, chunk manifestChunk) error {
	out, err := s.get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("chunk %s is missing", key) // not ErrNotFound: the record exists but is damaged
	}
	if err != nil {
		return err
	}
	defer func() { _ = out.Body.Close() }()
	if err := store.WriteChunk(f, size, chunk.Index, chunk.SHA256, out.Body); err != nil {
		return fmt.Errorf("chunk %s: %w", key, err)
	}
	return nil
}

func (s *Store) key(id, rest string) string {
	if rest == "" {
		return s.prefix + id
	}
	return s.prefix + id + "/" + filepath.ToSlash(rest)
}

func (s *Store) lineageKey(lineage, id string) string {
	return s.prefix + lineagesDir + "/" + lineage + "/" + id
}

func (s *Store) chunkKey(lineage, sha string) string {
	return s.prefix + chunksDir + "/" + lineage + "/" + sha
}

func (s *Store) recordIDs(ctx context.Context) ([]string, error) {
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
	return ids, nil
}

func (s *Store) referencedChunks(ctx context.Context, id, lineage string) (map[string]struct{}, error) {
	ids := []string{id}
	if lineage != id {
		markers, err := s.list(ctx, s.lineageKey(lineage, ""))
		if err != nil {
			return nil, err
		}
		ids = ids[:0]
		for _, marker := range markers {
			ids = append(ids, path.Base(*marker.Key))
		}
	}
	refs := &chunkRefs{live: map[string]map[string]struct{}{}}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(metaReadConcurrency)
	for _, recordID := range ids {
		g.Go(func() error {
			man, err := s.currentManifest(gctx, recordID)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			refs.add(man)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	have := refs.live[lineage]
	if have == nil {
		have = map[string]struct{}{}
	}
	return have, nil
}

func (s *Store) sweepRecord(ctx context.Context, rec *recordObjects, now time.Time, refs *chunkRefs) ([]string, error) {
	current := ""
	var metaModified *time.Time
	vanished := false
	if rec.meta != nil {
		meta, out, err := s.getObject(ctx, *rec.meta.Key)
		vanished = errors.Is(err, store.ErrNotFound)
		if err != nil && !vanished {
			return nil, err
		}
		if !vanished {
			current, metaModified = store.ExportGen(meta), out.LastModified
		}
	}
	var doomed []string
	for gen, obj := range rec.manifests {
		if !vanished && gen != current && idle(obj.LastModified, now) && (rec.meta == nil || idle(metaModified, now)) {
			doomed = append(doomed, *obj.Key)
			continue
		}
		raw, _, err := s.getObject(ctx, *obj.Key)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var man manifest
		if err = json.Unmarshal(raw, &man); err != nil {
			return nil, fmt.Errorf("decode manifest %s: %w", *obj.Key, err)
		}
		refs.add(man)
	}
	return doomed, nil
}

func (s *Store) refreshObject(ctx context.Context, key string) error {
	body, _, err := s.getObject(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.putObject(ctx, key, body, nil)
}

func (s *Store) putObject(ctx context.Context, key string, body []byte, metadata map[string]string) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		Body:          bytes.NewReader(body),
		ContentLength: new(int64(len(body))),
		Metadata:      metadata,
	})
	if err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
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

func (s *Store) list(ctx context.Context, prefix string) ([]s3types.Object, error) {
	var objs []s3types.Object
	p := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		objs = append(objs, page.Contents...)
	}
	return objs, nil
}

type inventory struct {
	records map[string]*recordObjects
	chunks  map[string][]s3types.Object // by lineage
	markers []s3types.Object
}

func (inv inventory) alive(id string, dead map[string]struct{}) bool {
	rec := inv.records[id]
	if rec == nil {
		return false
	}
	if rec.meta != nil {
		return true
	}
	for _, obj := range rec.manifests {
		if _, gone := dead[*obj.Key]; !gone {
			return true
		}
	}
	return false
}

type chunkUploader struct {
	store *Store
	group *errgroup.Group
	ctx   context.Context // the group's, so a finished scan cannot cancel uploads in flight

	lineage string

	mu   sync.Mutex
	have map[string]struct{}
}

func (u *chunkUploader) scan(ctx context.Context, file publishFile) (store.DigestFile, manifestFile, error) {
	f, err := os.Open(file.path)
	if err != nil {
		return store.DigestFile{}, manifestFile{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return store.DigestFile{}, manifestFile{}, err
	}
	if err = store.RequireRegular(file.rel, info.Mode()); err != nil {
		return store.DigestFile{}, manifestFile{}, err
	}
	entry := manifestFile{Path: file.rel, Size: info.Size()}
	digest, err := store.ScanFile(f, file.rel, info.Size(), getChunkBuf, func(c store.Chunk) error {
		entry.Chunks = append(entry.Chunks, manifestChunk{Index: c.Index, SHA256: c.SHA256})
		return u.upload(ctx, c)
	})
	return digest, entry, err
}

func (u *chunkUploader) upload(ctx context.Context, c store.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !u.claim(c.SHA256) {
		putChunkBuf(c.Data)
		return nil
	}
	u.group.Go(func() error {
		defer putChunkBuf(c.Data)
		return u.store.putObject(u.ctx, u.store.chunkKey(u.lineage, c.SHA256), c.Data, nil)
	})
	return nil
}

func (u *chunkUploader) claim(sha string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.have[sha]; ok {
		return false
	}
	u.have[sha] = struct{}{}
	return true
}

type chunkRefs struct {
	mu   sync.Mutex
	live map[string]map[string]struct{} // lineage -> chunk sha
}

func (r *chunkRefs) add(man manifest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.live[man.Lineage]
	if set == nil {
		set = map[string]struct{}{}
		r.live[man.Lineage] = set
	}
	for _, file := range man.Files {
		for _, chunk := range file.Chunks {
			set[chunk.SHA256] = struct{}{}
		}
	}
}

func (r *chunkRefs) has(lineage, sha string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.live[lineage][sha]
	return ok
}

func publishFiles(staging string) ([]publishFile, error) {
	var files []publishFile
	err := store.WalkExport(filepath.Join(staging, store.ExportDir), func(rel, abs string, _ fs.FileInfo) error {
		files = append(files, publishFile{path: abs, rel: rel})
		return nil
	})
	return files, err
}

func objectKeys(objs []s3types.Object) []string {
	keys := make([]string, len(objs))
	for i, obj := range objs {
		keys[i] = *obj.Key
	}
	return keys
}

func idle(modified *time.Time, now time.Time) bool {
	return now.Sub(aws.ToTime(modified)) >= store.GenerationGrace
}

// pruneIdleGenerations drops cached generations of one record that no Fetch used within the grace; a hit refreshes the mtime.
func pruneIdleGenerations(dir, keep string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		if info, err := entry.Info(); err == nil && entryPath != keep && time.Since(info.ModTime()) >= store.GenerationGrace {
			_ = os.RemoveAll(entryPath)
		}
	}
}

func newInventory(prefix string, objs []s3types.Object) inventory {
	inv := inventory{records: map[string]*recordObjects{}, chunks: map[string][]s3types.Object{}}
	for _, obj := range objs {
		first, rest, _ := strings.Cut(strings.TrimPrefix(*obj.Key, prefix), "/")
		if first == lineagesDir {
			inv.markers = append(inv.markers, obj)
			continue
		}
		if first == chunksDir {
			if lineage, _, ok := strings.Cut(rest, "/"); ok {
				inv.chunks[lineage] = append(inv.chunks[lineage], obj)
			}
			continue
		}
		if !store.CheckpointIDRe.MatchString(first) && !store.TemplateIDRe.MatchString(first) {
			continue
		}
		rec := inv.records[first]
		if rec == nil {
			rec = &recordObjects{manifests: map[string]s3types.Object{}}
			inv.records[first] = rec
		}
		if rest == store.MetaFile {
			rec.meta = &obj
		} else if gen, name, ok := strings.Cut(rest, "/"); ok && name == manifestName {
			rec.manifests[gen] = obj
		}
	}
	return inv
}

func getChunkBuf() []byte { return *chunkBufs.Get().(*[]byte) }

func putChunkBuf(buf []byte) {
	buf = buf[:cap(buf)]
	chunkBufs.Put(&buf)
}
