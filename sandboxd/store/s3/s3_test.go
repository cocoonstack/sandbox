package s3

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestS3BackendContract(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	storetest.RunContract(t, st)
}

// TestDeleteRetryConverges: a Delete retried after a partial failure re-deletes
// keys already gone; strict backends answer NoSuchKey per entry and the retry
// must still converge instead of failing forever.
func TestDeleteRetryConverges(t *testing.T) {
	const id = "ck_00000000000000bb"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                []byte(`{"id":"` + id + `"}`),
		"ck/" + id + "/" + store.ExportDir + "/disk.img": []byte("bytes"),
	}}
	st := newTestStore(t, fake)
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete retry after keys already gone: %v", err)
	}
	if len(fake.objects) != 0 {
		t.Errorf("objects left behind: %v", fake.objects)
	}
}

func TestDeleteRetainsMetaUntilExportsAreGone(t *testing.T) {
	const id = "ck_00000000000000bb"
	metaKey := "ck/" + id + "/" + store.MetaFile
	exportKeys := []string{
		"ck/" + id + "/export-first/disk.img",
		"ck/" + id + "/export-second/disk.img",
	}
	for _, tc := range []struct {
		name       string
		failList   bool
		failDelete bool
	}{
		{name: "list failure", failList: true},
		{name: "delete failure", failDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeS3{
				objects: map[string][]byte{
					metaKey:       []byte(`{"id":"` + id + `"}`),
					exportKeys[0]: []byte("first"),
					exportKeys[1]: []byte("second"),
				},
				failList:   tc.failList,
				failDelete: tc.failDelete,
			}
			st := newTestStore(t, fake)

			if err := st.Delete(t.Context(), id); err == nil {
				t.Fatal("first Delete succeeded with an injected failure")
			}
			fake.mu.Lock()
			if _, ok := fake.objects[metaKey]; !ok {
				fake.mu.Unlock()
				t.Fatal("first Delete removed meta before export cleanup succeeded")
			}
			for _, batch := range fake.deleteBatches {
				if slices.Contains(batch, metaKey) {
					fake.mu.Unlock()
					t.Fatalf("first Delete included meta in export batch %v", batch)
				}
			}
			fake.failList = false
			fake.failDelete = false
			fake.deleteBatches = nil
			fake.mu.Unlock()

			if err := st.Delete(t.Context(), id); err != nil {
				t.Fatalf("second Delete: %v", err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.objects) != 0 {
				t.Errorf("objects left after retry: %v", fake.objects)
			}
			if len(fake.deleteBatches) < 2 {
				t.Fatalf("delete batches = %v, want exports then meta", fake.deleteBatches)
			}
			last := len(fake.deleteBatches) - 1
			for i, batch := range fake.deleteBatches {
				hasMeta := slices.Contains(batch, metaKey)
				if (i == last) != hasMeta {
					t.Errorf("delete batch %d = %v, meta must appear only in final batch", i, batch)
				}
			}
			if batch := fake.deleteBatches[last]; len(batch) != 1 || batch[0] != metaKey {
				t.Errorf("final delete batch = %v, want only %s", batch, metaKey)
			}
		})
	}
}

// TestFetchLegacyExportLayout: records published before per-generation
// export prefixes keep flat export/ keys; Fetch must fall back to them.
func TestFetchLegacyExportLayout(t *testing.T) {
	const id = "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                []byte(`{"id":"` + id + `"}`),
		"ck/" + id + "/" + store.ExportDir + "/disk.img": []byte("legacy-bytes"),
	}}
	st := newTestStore(t, fake)
	dir, _, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	got, err := os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil || string(got) != "legacy-bytes" {
		t.Fatalf("fetched legacy export: %q, %v", got, err)
	}
}

func TestDigestReaderIgnoresReadBoundaries(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize))
	data = append(data, 'y')
	source := &patternReader{data: data, limits: []int{1, 31, 64 << 10, 7, 79 << 10}}
	reader := newDigestReader(source, "boundary.bin", int64(len(data)))
	if _, ok := any(reader).(io.Seeker); ok {
		t.Fatal("digest reader exposes io.Seeker")
	}
	if _, err := io.CopyBuffer(io.Discard, reader, make([]byte, 113<<10)); err != nil {
		t.Fatalf("read digest stream: %v", err)
	}
	file, err := reader.Digest()
	if err != nil {
		t.Fatalf("finish digest stream: %v", err)
	}
	digest, err := store.AssembleDigest([]store.DigestFile{file})
	if err != nil {
		t.Fatalf("assemble digest: %v", err)
	}
	const want = "sha256:da8dd289e7d66408bd7d202642f180f2c22bc508cf0fe1d6e9f0e0b7967f6a5e"
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
}

func TestUploadDigestedSetsContentLength(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize))
	data = append(data, 'y')
	path := filepath.Join(t.TempDir(), "boundary.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	tm := &recordingTransferManager{}
	st := &Store{tm: tm}
	file, err := st.uploadDigested(t.Context(), "key", path, "boundary.bin")
	if err != nil {
		t.Fatalf("upload digested: %v", err)
	}
	if tm.contentLength == nil || *tm.contentLength != int64(len(data)) {
		t.Fatalf("ContentLength = %v, want %d", tm.contentLength, len(data))
	}
	if tm.read != int64(len(data)) {
		t.Fatalf("uploaded bytes = %d, want %d", tm.read, len(data))
	}
	digest, err := store.AssembleDigest([]store.DigestFile{file})
	if err != nil {
		t.Fatalf("assemble digest: %v", err)
	}
	const want = "sha256:da8dd289e7d66408bd7d202642f180f2c22bc508cf0fe1d6e9f0e0b7967f6a5e"
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
}

func TestPublishDigestedRetryAndMetaRequestAccounting(t *testing.T) {
	const id = "ck_00000000000000dd"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	meta := []byte(`{"id":"` + id + `","gen":1}`)
	content := "retry-body"
	exportKey := "ck/" + id + "/" + store.ExportGen(meta) + "/disk.img"
	metaKey := "ck/" + id + "/" + store.MetaFile
	fake.setPutFailures(exportKey, 1)

	staging := stageRecord(t, st, id, meta, content)
	digest, err := st.PublishDigested(t.Context(), staging, id)
	if err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	chunk := sha256.Sum256([]byte(content))
	want, err := store.AssembleDigest([]store.DigestFile{{
		Path: "disk.img", Size: int64(len(content)), Chunks: [][sha256.Size]byte{chunk},
	}})
	if err != nil {
		t.Fatalf("assemble expected digest: %v", err)
	}
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
	if got := fake.requestCount(http.MethodPut, exportKey); got != 2 {
		t.Errorf("export PUT requests = %d, want 2", got)
	}
	if got := fake.contentLengths(exportKey); !slices.Equal(got, []int64{int64(len(content)), int64(len(content))}) {
		t.Errorf("export content lengths = %v, want two %d-byte requests", got, len(content))
	}
	if got := fake.objectMetadata(metaKey)[digestMetadataKey]; got != want {
		t.Errorf("marker digest metadata = %q, want %q", got, want)
	}

	fake.resetRequests()
	_, _, fetchedDigest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch miss: %v", err)
	}
	release()
	if fetchedDigest != want {
		t.Errorf("Fetch miss digest = %q, want %q", fetchedDigest, want)
	}
	assertSingleMetaGet(t, fake, metaKey)

	fake.resetRequests()
	_, _, fetchedDigest, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch hit: %v", err)
	}
	release()
	if fetchedDigest != want {
		t.Errorf("Fetch hit digest = %q, want %q", fetchedDigest, want)
	}
	assertSingleMetaGet(t, fake, metaKey)

	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":2}`), "plain")
	_, _, fetchedDigest, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch plain replacement: %v", err)
	}
	release()
	if fetchedDigest != "" {
		t.Errorf("plain replacement digest = %q, want empty", fetchedDigest)
	}
	if got := fake.objectMetadata(metaKey)[digestMetadataKey]; got != "" {
		t.Errorf("plain marker retained digest metadata %q", got)
	}
}

func TestPublishDigestedFailurePreservesCommittedGeneration(t *testing.T) {
	const id = "ck_00000000000000ee"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	oldMeta := []byte(`{"id":"` + id + `","gen":1}`)
	oldStaging := stageRecord(t, st, id, oldMeta, "old")
	oldDigest, err := st.PublishDigested(t.Context(), oldStaging, id)
	if err != nil {
		t.Fatalf("publish old generation: %v", err)
	}

	newMeta := []byte(`{"id":"` + id + `","gen":2}`)
	newStaging := stageRecord(t, st, id, newMeta, "replacement")
	newExportKey := "ck/" + id + "/" + store.ExportGen(newMeta) + "/disk.img"
	metaKey := "ck/" + id + "/" + store.MetaFile
	markerPuts := fake.requestCount(http.MethodPut, metaKey)
	fake.setPutFailures(newExportKey, 100)
	digest, err := st.PublishDigested(t.Context(), newStaging, id)
	if err == nil {
		t.Fatal("PublishDigested succeeded with injected export failure")
	}
	if digest != "" {
		t.Errorf("failed PublishDigested returned digest %q", digest)
	}
	if got := fake.requestCount(http.MethodPut, metaKey); got != markerPuts {
		t.Errorf("marker PUT requests after export failure = %d, want %d", got, markerPuts)
	}

	dir, meta, fetchedDigest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch old generation: %v", err)
	}
	defer release()
	content, err := os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read old generation: %v", err)
	}
	if string(meta) != string(oldMeta) || string(content) != "old" || fetchedDigest != oldDigest {
		t.Errorf("old generation = meta %q, content %q, digest %q", meta, content, fetchedDigest)
	}

	fake.setPutFailures(newExportKey, 0)
	newDigest, err := st.PublishDigested(t.Context(), newStaging, id)
	if err != nil {
		t.Fatalf("retry PublishDigested: %v", err)
	}
	dir, meta, fetchedDigest, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch replacement: %v", err)
	}
	defer release()
	content, err = os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if string(meta) != string(newMeta) || string(content) != "replacement" || fetchedDigest != newDigest {
		t.Errorf("replacement = meta %q, content %q, digest %q", meta, content, fetchedDigest)
	}
}

func TestRepublishRetainsGenerationSelectedByAnotherStore(t *testing.T) {
	const id = "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{}}
	reader := newTestStore(t, fake)
	writer := newTestStore(t, fake)
	publishRecord(t, writer, id, []byte(`{"id":"`+id+`","gen":1}`), "first")
	selected, err := reader.ReadMeta(t.Context(), id)
	if err != nil {
		t.Fatalf("select first generation: %v", err)
	}
	publishRecord(t, writer, id, []byte(`{"id":"`+id+`","gen":2}`), "second")

	gen := filepath.Join(reader.staging, "selected-first")
	if err = reader.populate(t.Context(), id, selected, gen); err != nil {
		t.Fatalf("fetch selected first generation after re-publish: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(gen, store.ExportDir, "disk.img")) //nolint:gosec // test path
	if err != nil || string(got) != "first" {
		t.Fatalf("selected generation bytes: %q, %v, want first", got, err)
	}
	if err = writer.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete all generations: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.objects) != 0 {
		t.Errorf("objects left after Delete: %v", fake.objects)
	}
}

// TestS3BackendContractRealEndpoint runs the same contract against a real
// S3 implementation (MinIO on a testbed) when SANDBOX_S3_E2E names its
// endpoint — real list pagination, checksums, and path-style behavior the
// in-process fake cannot vouch for.
func TestS3BackendContractRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("SANDBOX_S3_E2E")
	if endpoint == "" {
		t.Skip("SANDBOX_S3_E2E not set (export it to a MinIO endpoint to run)")
	}
	st, err := New(t.Context(), Config{
		Bucket:         cmp.Or(os.Getenv("SANDBOX_S3_E2E_BUCKET"), "sbx-checkpoints"),
		Prefix:         "contract/",
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}

func publishRecord(t *testing.T, st *Store, id string, meta []byte, content string) {
	t.Helper()
	staging := stageRecord(t, st, id, meta, content)
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish record: %v", err)
	}
}

func stageRecord(t *testing.T, st *Store, id string, meta []byte, content string) string {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage record: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if err := os.MkdirAll(export, 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(export, "disk.img"), []byte(content), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return staging
}

func newTestStore(t *testing.T, fake *fakeS3) *Store {
	t.Helper()
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	// Keep PutObject bodies plain instead of adding aws-chunked trailers the fake would need to decode.
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
	st, err := New(t.Context(), Config{
		Bucket: "testbucket", Prefix: "ck/", Endpoint: ts.URL,
		Region: "us-east-1", ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return st
}

func assertSingleMetaGet(t *testing.T, fake *fakeS3, metaKey string) {
	t.Helper()
	if got := fake.requestCount(http.MethodGet, metaKey); got != 1 {
		t.Errorf("meta GET requests = %d, want 1", got)
	}
	if got := fake.requestCount(http.MethodHead, metaKey); got != 0 {
		t.Errorf("meta HEAD requests = %d, want 0", got)
	}
}

type patternReader struct {
	data   []byte
	limits []int
	offset int
	read   int
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	limit := r.limits[r.read%len(r.limits)]
	r.read++
	n := min(len(p), limit, len(r.data)-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

type recordingTransferManager struct {
	contentLength *int64
	read          int64
}

func (m *recordingTransferManager) UploadObject(
	_ context.Context,
	input *transfermanager.UploadObjectInput,
	_ ...func(*transfermanager.Options),
) (*transfermanager.UploadObjectOutput, error) {
	if input.ContentLength != nil {
		length := *input.ContentLength
		m.contentLength = &length
	}
	read, err := io.CopyBuffer(io.Discard, input.Body, make([]byte, 113<<10))
	m.read = read
	return &transfermanager.UploadObjectOutput{}, err
}

func (*recordingTransferManager) DownloadObject(
	context.Context,
	*transfermanager.DownloadObjectInput,
	...func(*transfermanager.Options),
) (*transfermanager.DownloadObjectOutput, error) {
	return nil, fmt.Errorf("unexpected download")
}

type fakeRequest struct {
	method string
	key    string
}

type fakeS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte
	metadata      map[string]map[string]string
	failPuts      map[string]int
	requests      map[fakeRequest]int
	putLengths    map[string][]int64
	deleteBatches [][]string
	failList      bool
	failDelete    bool
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
	if f.requests == nil {
		f.requests = map[fakeRequest]int{}
	}
	f.requests[fakeRequest{method: r.Method, key: key}]++
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && query.Get("list-type") == "2":
		f.handleListObjects(w, r)
	case r.Method == http.MethodPost && query.Has("delete"):
		f.handleDeleteObjects(w, r)
	case r.Method == http.MethodPut:
		f.handlePutObject(w, r, key)
	case r.Method == http.MethodHead:
		f.handleHeadObject(w, key)
	case r.Method == http.MethodGet:
		f.handleGetObject(w, r, key)
	case r.Method == http.MethodDelete:
		f.handleDeleteObject(w, key)
	default:
		http.Error(w, "unsupported", http.StatusNotImplemented)
	}
}

func (f *fakeS3) setPutFailures(key string, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPuts == nil {
		f.failPuts = map[string]int{}
	}
	f.failPuts[key] = count
}

func (f *fakeS3) requestCount(method, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[fakeRequest{method: method, key: key}]
}

func (f *fakeS3) contentLengths(key string) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.putLengths[key])
}

func (f *fakeS3) objectMetadata(key string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.metadata[key])
}

func (f *fakeS3) resetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.requests)
}

func (f *fakeS3) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if f.putLengths == nil {
		f.putLengths = map[string][]int64{}
	}
	f.putLengths[key] = append(f.putLengths[key], r.ContentLength)
	if f.failPuts[key] > 0 {
		f.failPuts[key]--
		writeFakeError(w, http.StatusInternalServerError, "InternalError", "injected put failure")
		return
	}
	if f.objects == nil {
		f.objects = map[string][]byte{}
	}
	if f.metadata == nil {
		f.metadata = map[string]map[string]string{}
	}
	f.objects[key] = bytes.Clone(body)
	metadata := requestMetadata(r.Header)
	if len(metadata) == 0 {
		delete(f.metadata, key)
	} else {
		f.metadata[key] = metadata
	}
	w.Header().Set("ETag", `"put"`)
}

func (f *fakeS3) handleListObjects(w http.ResponseWriter, r *http.Request) {
	if f.failList {
		http.Error(w, "injected list failure", http.StatusBadRequest)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delim := r.URL.Query().Get("delimiter")
	type object struct {
		Key  string `xml:"Key"`
		Size int    `xml:"Size"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	var result struct {
		XMLName        xml.Name `xml:"ListBucketResult"`
		IsTruncated    bool     `xml:"IsTruncated"`
		Contents       []object
		CommonPrefixes []commonPrefix
	}
	seen := map[string]bool{}
	for key, body := range f.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(key[len(prefix):], delim); i >= 0 {
				common := key[:len(prefix)+i+1]
				if !seen[common] {
					seen[common] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: common})
				}
				continue
			}
		}
		result.Contents = append(result.Contents, object{Key: key, Size: len(body)})
	}
	slices.SortFunc(result.Contents, func(a, b object) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(result.CommonPrefixes, func(a, b commonPrefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) handleHeadObject(w http.ResponseWriter, key string) {
	body, ok := f.objects[key]
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	writeMetadata(w.Header(), f.metadata[key])
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
}

func (f *fakeS3) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	body, ok := f.objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		return
	}
	writeMetadata(w.Header(), f.metadata[key])
	if rng := r.Header.Get("Range"); rng != "" {
		var start, end int
		if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil && start < len(body) {
			end = min(end, len(body)-1)
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[start : end+1])
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

func (f *fakeS3) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil || xml.Unmarshal(body, &req) != nil {
		http.Error(w, "bad delete xml", http.StatusBadRequest)
		return
	}
	batch := make([]string, len(req.Objects))
	for i := range req.Objects {
		batch[i] = req.Objects[i].Key
	}
	f.deleteBatches = append(f.deleteBatches, batch)
	type deleteError struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message,omitempty"`
	}
	var result struct {
		XMLName xml.Name      `xml:"DeleteResult"`
		Errors  []deleteError `xml:"Error"`
	}
	if f.failDelete {
		result.Errors = append(result.Errors, deleteError{
			Key: req.Objects[0].Key, Code: "AccessDenied", Message: "injected delete failure",
		})
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
		return
	}
	for _, object := range req.Objects {
		if _, ok := f.objects[object.Key]; !ok {
			result.Errors = append(result.Errors, deleteError{Key: object.Key, Code: "NoSuchKey"})
			continue
		}
		delete(f.objects, object.Key)
		delete(f.metadata, object.Key)
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) handleDeleteObject(w http.ResponseWriter, key string) {
	delete(f.objects, key)
	delete(f.metadata, key)
	w.WriteHeader(http.StatusNoContent)
}

func requestMetadata(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		key = strings.ToLower(key)
		if name, ok := strings.CutPrefix(key, "x-amz-meta-"); ok && len(values) > 0 {
			metadata[name] = values[0]
		}
	}
	return metadata
}

func writeMetadata(header http.Header, metadata map[string]string) {
	for key, value := range metadata {
		header.Set("x-amz-meta-"+key, value)
	}
}

func writeFakeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}
