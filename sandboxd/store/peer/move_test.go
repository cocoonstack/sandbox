package peer

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestSendRoundShipsTheListedFilesInOrderAndKeepsSparseFilesSparse(t *testing.T) {
	src := t.TempDir()
	size := 6*store.DigestChunkSize + 4096
	storetest.WriteSparse(t, filepath.Join(src, "cow.raw"), size, map[int64][]byte{
		0:                           []byte("head"),
		4*store.DigestChunkSize + 9: []byte("mid"),
	})
	dense := bytes.Repeat([]byte("guest-memory-"), 300000)
	for name, body := range map[string][]byte{"memory-ranges": dense, "state.json": []byte(`{"round":1}`), "unlisted": []byte("x")} {
		if err := os.WriteFile(filepath.Join(src, name), body, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	names := []string{"state.json", "memory-ranges", "cow.raw"}
	var method, path, auth string
	var body bytes.Buffer
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, auth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		_, _ = io.Copy(&body, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	wire, err := SendRound(t.Context(), srv.URL, "fleet-token", "/v1/moves/m1/rounds/1", src, names)
	if err != nil {
		t.Fatalf("SendRound: %v", err)
	}
	if wire != int64(body.Len()) || wire >= size {
		t.Errorf("SendRound counted %d bytes, want the %d on the wire, far below the sparse file's %d", wire, body.Len(), size)
	}
	if method != http.MethodPut || path != "/v1/moves/m1/rounds/1" || auth != "Bearer fleet-token" {
		t.Errorf("request = %s %s auth %q, want PUT /v1/moves/m1/rounds/1 with the fleet token", method, path, auth)
	}
	if got, want := entryNames(t, body.Bytes()), append(slices.Clone(names), recordTrailer); !slices.Equal(got, want) {
		t.Errorf("entries on the wire = %q, want %q", got, want)
	}
	sent := int64(body.Len())
	dst := t.TempDir()
	if err = ReceiveRound(&body, dst); err != nil {
		t.Fatalf("ReceiveRound: %v", err)
	}
	for _, name := range names {
		if sha256OfFile(t, filepath.Join(dst, name)) != sha256OfFile(t, filepath.Join(src, name)) {
			t.Errorf("%s differs from the source", name)
		}
	}
	info, err := os.Stat(filepath.Join(dst, "cow.raw"))
	if err != nil || info.Size() != size {
		t.Fatalf("received cow.raw = %v, %v, want %d bytes", info, err, size)
	}
	if allocated := info.Sys().(*syscall.Stat_t).Blocks * 512; allocated > sent {
		t.Errorf("received cow.raw allocates %d bytes, more than the %d sent", allocated, sent)
	}
	entries, _ := os.ReadDir(dst)
	if len(entries) != len(names) {
		t.Errorf("dst holds %d entries, want exactly the %d listed files", len(entries), len(names))
	}
}

func TestReceiveRoundReplacesFilesOfTheSameName(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	size := 3 * store.DigestChunkSize
	storetest.WriteSparse(t, filepath.Join(src, "cow.raw"), size, map[int64][]byte{store.DigestChunkSize: []byte("new")})
	if err := os.WriteFile(filepath.Join(src, "state.json"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "state.json"), []byte("an older and longer state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "cow.raw"), bytes.Repeat([]byte{0xee}, int(size)), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := ReceiveRound(bytes.NewReader(roundStream(t, src, "state.json", "cow.raw")), dst); err != nil {
		t.Fatalf("ReceiveRound: %v", err)
	}
	if body, err := os.ReadFile(filepath.Join(dst, "state.json")); err != nil || string(body) != "new" {
		t.Errorf("state.json = %q, %v, want the new round's bytes", body, err)
	}
	if sha256OfFile(t, filepath.Join(dst, "cow.raw")) != sha256OfFile(t, filepath.Join(src, "cow.raw")) {
		t.Error("cow.raw keeps bytes of the replaced file in its holes")
	}
}

func TestReceiveRoundOfACutStreamLeavesNoCompleteLookingFile(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "state.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	storetest.WriteSparse(t, filepath.Join(src, "memory-ranges"), 4*store.DigestChunkSize, map[int64][]byte{
		0:                         bytes.Repeat([]byte("a"), 4096),
		store.DigestChunkSize:     bytes.Repeat([]byte("b"), 4096),
		3 * store.DigestChunkSize: bytes.Repeat([]byte("c"), 4096),
	})
	full := roundStream(t, src, "state.json", "memory-ranges")
	var noTrailer bytes.Buffer
	tw := tar.NewWriter(&noTrailer)
	tr := tar.NewReader(bytes.NewReader(full))
	for {
		hdr, err := tr.Next()
		if err != nil || hdr.Name == recordTrailer {
			break
		}
		if err = tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err = io.Copy(tw, tr); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	for name, stream := range map[string][]byte{
		"no completion marker": noTrailer.Bytes(),
		"cut inside a chunk":   full[:len(full)-int(store.DigestChunkSize)/2],
	} {
		t.Run(name, func(t *testing.T) {
			dst := t.TempDir()
			if err := ReceiveRound(bytes.NewReader(stream), dst); err == nil {
				t.Fatal("ReceiveRound accepted a cut stream")
			}
			if _, err := os.Stat(filepath.Join(dst, "memory-ranges")); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("memory-ranges exists after a cut stream (%v); a partial file must not take its name", err)
			}
			if _, err := os.Stat(filepath.Join(dst, "memory-ranges"+partialSuffix)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("partial file left behind (%v)", err)
			}
			if body, err := os.ReadFile(filepath.Join(dst, "state.json")); err != nil || string(body) != "{}" {
				t.Errorf("state.json = %q, %v, want the file that arrived whole", body, err)
			}
		})
	}
}

func TestReceiveRoundRefusesNonLocalAndNonRegularEntries(t *testing.T) {
	sized := map[string]string{paxFileSize: "4"}
	for _, hdr := range []*tar.Header{
		{Name: "../escape", Mode: 0o600, Typeflag: tar.TypeReg, PAXRecords: sized},
		{Name: "/abs/escape", Mode: 0o600, Typeflag: tar.TypeReg, PAXRecords: sized},
		{Name: `..\escape`, Mode: 0o600, Typeflag: tar.TypeReg, PAXRecords: sized},
		{Name: "link", Linkname: "/etc/passwd", Mode: 0o777, Typeflag: tar.TypeSymlink},
		{Name: "sub/", Mode: 0o700, Typeflag: tar.TypeDir},
		{Name: "plain", Mode: 0o600, Typeflag: tar.TypeReg},
	} {
		t.Run(hdr.Name, func(t *testing.T) {
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			if err := tw.WriteHeader(hdr); err != nil {
				t.Fatal(err)
			}
			if err := tw.WriteHeader(&tar.Header{Name: recordTrailer, Mode: 0o600, Typeflag: tar.TypeReg}); err != nil {
				t.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			dst := filepath.Join(root, "dst")
			if err := os.Mkdir(dst, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := ReceiveRound(&buf, dst); err == nil {
				t.Fatalf("ReceiveRound accepted %q", hdr.Name)
			}
			if entries, _ := os.ReadDir(dst); len(entries) != 0 {
				t.Errorf("dst holds %d entries after a refused stream", len(entries))
			}
			if entries, _ := os.ReadDir(root); len(entries) != 1 {
				t.Errorf("dst's parent holds %d entries after a refused stream, want only dst", len(entries))
			}
		})
	}
}

func TestSendRoundFailsOnARefusedPutOrAMissingFile(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "state.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fleet-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := ReceiveRound(r.Body, t.TempDir()); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if _, err := SendRound(t.Context(), srv.URL, "wrong", "/r", src, []string{"state.json"}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("refused PUT = %v, want the 401", err)
	}
	if _, err := SendRound(t.Context(), srv.URL, "fleet-token", "/r", src, []string{"state.json", "missing"}); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Errorf("round with a missing file = %v, want the stat error", err)
	}
}

func TestSendRoundFailsWhenATargetStopsReading(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "memory-ranges"), bytes.Repeat([]byte("guest-memory-"), 4<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-stop }))
	defer srv.Close()
	defer close(stop)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := SendRound(ctx, srv.URL, "", "/r", src, []string{"memory-ranges"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("round to a target that stopped reading: %v, want the deadline", err)
	}
}

func roundStream(t *testing.T, dir string, names ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeRound(dir, names, &buf); err != nil {
		t.Fatalf("writeRound: %v", err)
	}
	return buf.Bytes()
}

func entryNames(t *testing.T, raw []byte) []string {
	t.Helper()
	var names []string
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		if err != nil {
			t.Fatalf("read entries: %v", err)
		}
		if len(names) == 0 || names[len(names)-1] != hdr.Name {
			names = append(names, hdr.Name)
		}
	}
}
