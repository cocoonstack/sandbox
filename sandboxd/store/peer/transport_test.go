package peer

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

func TestTarUntarRoundTrip(t *testing.T) {
	src := t.TempDir()
	files := map[string]string{
		"meta.json":                 `{"id":"ck_0000000000000001"}`,
		"export/memory-range-0":     "guest-memory-page-contents",
		"export/disk.qcow2":         "disk-bytes",
		"export/nested/deeper/file": "deep",
	}
	for name, content := range files {
		p := filepath.Join(src, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	var buf bytes.Buffer
	if err := tarDir(src, &buf); err != nil {
		t.Fatalf("tarDir: %v", err)
	}
	dst := t.TempDir()
	if err := Untar(&buf, dst); err != nil {
		t.Fatalf("Untar: %v", err)
	}

	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil {
			t.Errorf("read back %s: %v", name, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestUntarRejectsPathTraversal(t *testing.T) {
	for _, name := range []string{
		"../escape",
		"../../etc/passwd",
		"export/../../escape",
		"/absolute/path",
		`..\windows-style`,
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			body := []byte("pwned")
			if err := tw.WriteHeader(&tar.Header{
				Name: name, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg,
			}); err != nil {
				t.Fatalf("WriteHeader: %v", err)
			}
			if _, err := tw.Write(body); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := tw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			dst := t.TempDir()
			err := Untar(&buf, dst)
			if err == nil {
				t.Fatalf("Untar accepted %q; it must refuse to write outside the destination", name)
			}
			if !strings.Contains(err.Error(), "untar") {
				t.Errorf("error = %v, want an untar rejection", err)
			}
		})
	}
}

func TestUntarSkipsNonDataEntries(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: "evil-link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd", Mode: 0o777,
	}); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: "meta.json", Mode: 0o600, Size: 2, Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if _, err := tw.Write([]byte("{}")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: recordTrailer, Mode: 0o600, Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("WriteHeader trailer: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dst := t.TempDir()
	if err := Untar(&buf, dst); err != nil {
		t.Fatalf("Untar: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "evil-link")); err == nil {
		t.Error("symlink entry was materialized; non-data entries must be skipped")
	}

	if _, err := os.Stat(filepath.Join(dst, "meta.json")); err != nil {
		t.Errorf("meta.json missing after skipping the symlink: %v", err)
	}
}

func TestTarSkipsSymlinksAtSource(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "real"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(src, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	var buf bytes.Buffer
	if err := tarDir(src, &buf); err != nil {
		t.Fatalf("tarDir: %v", err)
	}
	tr := tar.NewReader(&buf)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Typeflag == tar.TypeSymlink {
			t.Errorf("Tar emitted a symlink entry %q", hdr.Name)
		}
	}
}

func TestHTTPPullerNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	p := &HTTPPuller{Client: srv.Client()}
	err := p.Pull(t.Context(), srv.URL, "ck_0000000000000001", t.TempDir())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Pull error = %v, want ErrNotFound so the next owner is tried", err)
	}
}

func TestHTTPPullerStopsReadingAfterAFailedUntar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 4096))
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && r.Context().Err() == nil {
			if _, err := w.Write(make([]byte, 64<<10)); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	t.Cleanup(srv.Close)
	start := time.Now()
	err := (&HTTPPuller{Client: srv.Client()}).Pull(t.Context(), srv.URL, "ck_0000000000000001", t.TempDir())
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("pull of a non-tar body: %v after %s, want a prompt untar error", err, time.Since(start))
	}
}

func TestHTTPPullerPresentsToken(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		_ = tw.WriteHeader(&tar.Header{Name: "meta.json", Mode: 0o600, Size: 2, Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte("{}"))
		_ = tw.WriteHeader(&tar.Header{Name: recordTrailer, Mode: 0o600, Typeflag: tar.TypeReg})
		_ = tw.Close()
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	p := &HTTPPuller{Client: srv.Client(), Token: "fleet-token"}
	dst := t.TempDir()
	if err := p.Pull(t.Context(), srv.URL, "ck_0000000000000001", dst); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if gotAuth != "Bearer fleet-token" {
		t.Errorf("Authorization = %q, want the fleet token", gotAuth)
	}
	if want := "/v1/checkpoints/ck_0000000000000001/blob"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if _, err := os.Stat(filepath.Join(dst, "meta.json")); err != nil {
		t.Errorf("pulled record not materialized: %v", err)
	}
}

func TestTarRecordRoundTripsAWholeRecord(t *testing.T) {
	exportDir := t.TempDir()
	for name, content := range map[string]string{
		"memory-ranges": "guest-pages",
		"cow.raw":       "disk",
		"snapshot.json": "{}",
	} {
		if err := os.WriteFile(filepath.Join(exportDir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	meta := []byte(`{"id":"ck_0000000000000001","key":{"template":"t"}}`)

	var buf bytes.Buffer
	if err := TarRecord(exportDir, meta, &buf); err != nil {
		t.Fatalf("TarRecord: %v", err)
	}
	dst := t.TempDir()
	if err := Untar(&buf, dst); err != nil {
		t.Fatalf("Untar: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dst, "meta.json"))
	if err != nil {
		t.Fatalf("meta.json missing from the shipped record: %v", err)
	}
	if string(got) != string(meta) {
		t.Errorf("meta.json = %q, want %q", got, meta)
	}

	for name, want := range map[string]string{
		"memory-ranges": "guest-pages",
		"cow.raw":       "disk",
	} {
		got, err := os.ReadFile(filepath.Join(dst, "export", name))
		if err != nil {
			t.Errorf("export/%s missing: %v", name, err)
			continue
		}
		if string(got) != want {
			t.Errorf("export/%s = %q, want %q", name, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, "memory-ranges")); err == nil {
		t.Error("export contents landed at the record root; they must be under export/")
	}
}

func TestUntarRejectsRecordWithoutCompletionMarker(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	_ = tw.WriteHeader(&tar.Header{Name: "meta.json", Mode: 0o600, Size: 4, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte(`{"a"`))
	_ = tw.WriteHeader(&tar.Header{Name: "export/mem", Mode: 0o600, Size: 5, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("bytes"))
	_ = tw.Close()

	if err := Untar(&buf, t.TempDir()); err == nil {
		t.Error("Untar accepted a record with no completion marker (an incomplete transfer)")
	}
}

func TestTarRecordUntarRoundTripComplete(t *testing.T) {
	exportDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(exportDir, "mem"), []byte("state"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var buf bytes.Buffer
	if err := TarRecord(exportDir, []byte(`{"id":"ck_1"}`), &buf); err != nil {
		t.Fatalf("TarRecord: %v", err)
	}
	dst := t.TempDir()
	if err := Untar(&buf, dst); err != nil {
		t.Fatalf("Untar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, recordTrailer)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("completion marker was written to disk (%v); it must be consumed, not stored", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "export", "mem")); err != nil {
		t.Errorf("export content missing after round trip: %v", err)
	}
}

func TestTarRecordKeepsSparseFilesSparse(t *testing.T) {
	size := 10*store.DigestChunkSize + 4096
	exportDir := t.TempDir()
	src := filepath.Join(exportDir, "cow.raw")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err = f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	for off, data := range map[int64]string{0: "sparse-head", 4*store.DigestChunkSize + 7: "sparse-mid", 8*store.DigestChunkSize + 5: "tail"} {
		if _, err = f.WriteAt([]byte(data), off); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err = os.WriteFile(filepath.Join(exportDir, "snapshot.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	want := sha256OfFile(t, src)

	var buf bytes.Buffer
	if err = TarRecord(exportDir, []byte(`{"id":"ck_0000000000000001"}`), &buf); err != nil {
		t.Fatalf("TarRecord: %v", err)
	}
	sent := int64(buf.Len())
	if limit := 3*store.DigestChunkSize + 1<<20; sent > limit {
		t.Errorf("stream carries %d bytes for a %d-byte file with 3 data chunks, want at most %d", sent, size, limit)
	}
	dst := t.TempDir()
	if err = Untar(&buf, dst); err != nil {
		t.Fatalf("Untar: %v", err)
	}
	got := filepath.Join(dst, "export", "cow.raw")
	info, err := os.Stat(got)
	if err != nil || info.Size() != size {
		t.Fatalf("pulled size = %v, %v, want %d", info, err, size)
	}
	if sha256OfFile(t, got) != want {
		t.Error("pulled content differs from the source")
	}
	if allocated := info.Sys().(*syscall.Stat_t).Blocks * 512; allocated > sent {
		t.Errorf("pulled file allocates %d bytes, more than the %d sent", allocated, sent)
	}
	if body, err := os.ReadFile(filepath.Join(dst, "export", "snapshot.json")); err != nil || string(body) != "{}" {
		t.Errorf("small file = %q, %v", body, err)
	}
}

func TestUntarRejectsDamagedChunk(t *testing.T) {
	exportDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(exportDir, "mem"), []byte("guest-pages-payload"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var buf bytes.Buffer
	if err := TarRecord(exportDir, []byte(`{"id":"ck_1"}`), &buf); err != nil {
		t.Fatalf("TarRecord: %v", err)
	}
	raw := buf.Bytes()
	raw[bytes.Index(raw, []byte("guest-pages-payload"))] ^= 0xff
	err := Untar(bytes.NewReader(raw), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("Untar of a corrupted chunk = %v, want a checksum error", err)
	}
}

func tarDir(src string, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() { _ = tw.Close() }()
	if err := tarInto(src, "", tw); err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: recordTrailer, Mode: 0o600, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	return tw.Close()
}

func sha256OfFile(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	return [sha256.Size]byte(h.Sum(nil))
}
