package peer

import (
	"archive/tar"
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTarUntarRoundTrip is the transport's core promise: a record that goes out
// as a tar comes back byte-identical on the other node, directory structure
// included. A checkpoint is a guest memory image plus its meta — a transport
// that silently drops a file would produce a clone of the wrong state.
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
	if err := Tar(src, &buf); err != nil {
		t.Fatalf("Tar: %v", err)
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

// TestUntarRejectsPathTraversal is the security invariant. A peer authenticates
// with the fleet token, but an authenticated peer is still not allowed to write
// outside the destination this node chose — a compromised or buggy one must not
// be able to overwrite the host's files.
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

// TestUntarSkipsNonDataEntries keeps symlinks and device nodes out of a record.
// A record is data; honoring a symlink entry would be an indirect way to make a
// later write land outside the destination.
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
	// The legitimate entry alongside it still lands.
	if _, err := os.Stat(filepath.Join(dst, "meta.json")); err != nil {
		t.Errorf("meta.json missing after skipping the symlink: %v", err)
	}
}

// TestTarSkipsSymlinksAtSource is the same rule on the sending side: a record
// that somehow contains a symlink must not put one on the wire.
func TestTarSkipsSymlinksAtSource(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "real"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(src, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	var buf bytes.Buffer
	if err := Tar(src, &buf); err != nil {
		t.Fatalf("Tar: %v", err)
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

// TestHTTPPullerNotFound maps a peer's 404 to ErrNotFound so the caller keeps
// trying the next owner instead of failing the whole heal. A gossiped view can
// be stale — one owner not having the record is normal, not an error.
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

// TestHTTPPullerPresentsToken proves the fleet token rides on the pull: the
// serving peer authorizes this exactly like any other control-plane read.
func TestHTTPPullerPresentsToken(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		_ = tw.WriteHeader(&tar.Header{Name: "meta.json", Mode: 0o600, Size: 2, Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte("{}"))
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

// TestTarRecordRoundTripsAWholeRecord reproduces a bug found in the cluster:
// the blob endpoint tarred only the export directory Fetch() returns, so a
// pulled record published cleanly and then failed every read as "unknown
// checkpoint" — the meta carrying the pool key and lineage was never shipped.
// A record on the wire must be exactly what the local store lays out.
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

	// meta.json at the record root — without it the record is unreadable.
	got, err := os.ReadFile(filepath.Join(dst, "meta.json"))
	if err != nil {
		t.Fatalf("meta.json missing from the shipped record: %v", err)
	}
	if string(got) != string(meta) {
		t.Errorf("meta.json = %q, want %q", got, meta)
	}
	// export/ contents under the export prefix, not at the root.
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
