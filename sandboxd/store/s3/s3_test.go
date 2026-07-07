package s3

import (
	"cmp"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

// fakeS3 implements just enough of the S3 REST surface (path-style) for
// the backend: PutObject, GetObject, DeleteObject, ListObjectsV2.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte // key -> body
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
		prefix := r.URL.Query().Get("prefix")
		type object struct {
			Key  string `xml:"Key"`
			Size int    `xml:"Size"`
		}
		var result struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			IsTruncated bool     `xml:"IsTruncated"`
			Contents    []object
		}
		for k, v := range f.objects {
			if strings.HasPrefix(k, prefix) {
				result.Contents = append(result.Contents, object{Key: k, Size: len(v)})
			}
		}
		sort.Slice(result.Contents, func(i, j int) bool { return result.Contents[i].Key < result.Contents[j].Key })
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unsupported", http.StatusNotImplemented)
	}
}

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
