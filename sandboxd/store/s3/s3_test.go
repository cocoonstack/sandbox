package s3

import (
	"cmp"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestS3BackendContract(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	// Seekable bodies + when_required keep PutObject payloads plain (no
	// aws-chunked checksum trailers the fake would have to decode).
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

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
			ts := httptest.NewServer(fake)
			t.Cleanup(ts.Close)
			st, err := New(t.Context(), Config{
				Bucket: "testbucket", Prefix: "ck/", Endpoint: ts.URL,
				Region: "us-east-1", ForcePathStyle: true,
			}, t.TempDir(), store.CheckpointIDRe)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if err = st.Delete(t.Context(), id); err == nil {
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

			if err = st.Delete(t.Context(), id); err != nil {
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
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	got, err := os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil || string(got) != "legacy-bytes" {
		t.Fatalf("fetched legacy export: %q, %v", got, err)
	}
}

func TestRepublishRetainsGenerationSelectedByAnotherStore(t *testing.T) {
	const id = "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
	cfg := Config{
		Bucket: "testbucket", Prefix: "ck/", Endpoint: ts.URL,
		Region: "us-east-1", ForcePathStyle: true,
	}
	reader, err := New(t.Context(), cfg, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	writer, err := New(t.Context(), cfg, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
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
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish record: %v", err)
	}
}

// fakeS3 implements just enough of the S3 REST surface (path-style) for
// the backend: PutObject, GetObject, DeleteObjects, ListObjectsV2.
type fakeS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte // key -> body
	failList      bool
	failDelete    bool
	deleteBatches [][]string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
	switch {
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[key] = body
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
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
		for k, v := range f.objects {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			if delim != "" {
				if i := strings.Index(k[len(prefix):], delim); i >= 0 {
					cp := k[:len(prefix)+i+1]
					if !seen[cp] {
						seen[cp] = true
						result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: cp})
					}
					continue
				}
			}
			result.Contents = append(result.Contents, object{Key: k, Size: len(v)})
		}
		slices.SortFunc(result.Contents, func(a, b object) int { return cmp.Compare(a.Key, b.Key) })
		slices.SortFunc(result.CommonPrefixes, func(a, b commonPrefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodHead:
		body, ok := f.objects[key]
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	case r.Method == http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		// transfermanager sizes ranged downloads via HeadObject then GETs
		// with a Range header; serve it so multi-part gets stay correct.
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil && start < len(body) {
				if end >= len(body) {
					end = len(body) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(body[start : end+1])
				return
			}
		}
		_, _ = w.Write(body)
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		var req struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad delete xml", http.StatusBadRequest)
			return
		}
		batch := make([]string, len(req.Objects))
		for i := range req.Objects {
			batch[i] = req.Objects[i].Key
		}
		f.deleteBatches = append(f.deleteBatches, batch)
		// Strict-backend emulation: absent keys answer a per-entry NoSuchKey
		// (AWS succeeds silently) so the client's tolerance stays exercised.
		type delErr struct {
			Key     string `xml:"Key"`
			Code    string `xml:"Code"`
			Message string `xml:"Message,omitempty"`
		}
		var result struct {
			XMLName xml.Name `xml:"DeleteResult"`
			Errors  []delErr `xml:"Error"`
		}
		if f.failDelete {
			result.Errors = append(result.Errors, delErr{
				Key: req.Objects[0].Key, Code: "AccessDenied", Message: "injected delete failure",
			})
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
			return
		}
		for _, o := range req.Objects {
			if _, ok := f.objects[o.Key]; !ok {
				result.Errors = append(result.Errors, delErr{Key: o.Key, Code: "NoSuchKey"})
				continue
			}
			delete(f.objects, o.Key)
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unsupported", http.StatusNotImplemented)
	}
}
