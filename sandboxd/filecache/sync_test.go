package filecache

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseListing(t *testing.T) {
	out := "f\tREADME.md\t1700000000.5\t12\t\n" +
		"l\tlink\t1700000001\t0\ttarget/path\n" +
		"f\t.filecache/journal/x\t1700000002\t3\t\n" + // excluded
		"f\tsrc/main.go\t1700000003.9\t20\t\n"
	m := parseListing(out)
	if len(m) != 3 {
		t.Fatalf("want 3 entries, got %d: %v", len(m), m)
	}
	if e := m["README.md"]; e.Kind != "f" || e.Size != 12 || e.MtimeS != 1700000000 {
		t.Errorf("README.md: %+v", e)
	}
	if e := m["link"]; e.Kind != "l" || e.Target != "target/path" || e.Size != 0 {
		t.Errorf("link: %+v", e)
	}
	if _, ok := m[".filecache/journal/x"]; ok {
		t.Error(".filecache entries must be excluded")
	}
}

func TestParseJournalName(t *testing.T) {
	cases := []struct {
		name   string
		writer string
		seq    uint64
		ok     bool
	}{
		{"sb_abc-0000000000000007.json", "sb_abc", 7, true},
		{"sb_with-dash-0000000000000012.json", "sb_with-dash", 12, true},
		{"seq", "", 0, false},
		{"no-suffix", "", 0, false},
		{"writer-notanumber.json", "", 0, false},
	}
	for _, c := range cases {
		w, s, ok := parseJournalName(c.name)
		if ok != c.ok || w != c.writer || s != c.seq {
			t.Errorf("%q: got (%q,%d,%v) want (%q,%d,%v)", c.name, w, s, ok, c.writer, c.seq, c.ok)
		}
	}
}

// TestJournalRoundTrip covers appendJournal → unseenJournal → maxSeqs and the
// per-writer applied high-water filter (the core of cross-writer convergence).
func TestJournalRoundTrip(t *testing.T) {
	ws := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(ws, fcDir, "journal"), 0o755))

	a := &syncer{ws: ws, writer: "wA", applied: map[string]uint64{}}
	b := &syncer{ws: ws, writer: "wB", applied: map[string]uint64{}}

	a.seq = 1
	must(t, a.appendJournal(journalEntry{Writer: "wA", Seq: 1, TsNs: 100, Puts: map[string]entMeta{"f1": {Kind: "f", Size: 3}}}))
	a.seq = 2
	must(t, a.appendJournal(journalEntry{Writer: "wA", Seq: 2, TsNs: 200, Dels: []string{"f1"}}))

	// B has seen nothing yet: both entries are unseen, ordered by ts.
	got, err := b.unseenJournal()
	must(t, err)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("unseen: %+v", got)
	}
	// A never pulls its own entries.
	if own, _ := a.unseenJournal(); len(own) != 0 {
		t.Errorf("a saw its own entries: %+v", own)
	}
	// After recording seq 1 applied, only seq 2 remains.
	b.applied["wA"] = 1
	got, _ = b.unseenJournal()
	if len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("after applied=1: %+v", got)
	}
	// maxSeqs recovers the high-water for resume.
	if ms := maxSeqs(filepath.Join(ws, fcDir, "journal")); ms["wA"] != 2 {
		t.Errorf("maxSeqs wA = %d, want 2", ms["wA"])
	}
	// The seq file is freshened for the puller fast-path.
	if _, err := os.Stat(filepath.Join(ws, fcDir, "seq")); err != nil {
		t.Errorf("seq file: %v", err)
	}
}

// TestTarRoundTrip covers scanNAS → writeTar → applyTarToNAS: the workspace
// tree survives a hydrate/publish round-trip byte-for-byte, and .filecache is
// never carried.
func TestTarRoundTrip(t *testing.T) {
	src := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(src, "src"), 0o755))
	must(t, os.MkdirAll(filepath.Join(src, fcDir, "journal"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "README.md"), []byte("seed"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "src", "main.go"), []byte("package main\n"), 0o600))
	must(t, os.Symlink("README.md", filepath.Join(src, "readme-link")))
	must(t, os.WriteFile(filepath.Join(src, fcDir, "journal", "wA-0000000000000001.json"), []byte("{}"), 0o644))

	ents, err := scanNAS(src)
	must(t, err)
	if len(ents) != 3 { // README, src/main.go, readme-link; .filecache excluded
		t.Fatalf("scanNAS: %d entries %v", len(ents), ents)
	}

	var buf bytes.Buffer
	must(t, writeTar(&buf, src, ents))

	dst := t.TempDir()
	must(t, applyTarToNAS(dst, &buf))

	if b, _ := os.ReadFile(filepath.Join(dst, "README.md")); string(b) != "seed" {
		t.Errorf("README.md content: %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "src", "main.go")); string(b) != "package main\n" {
		t.Errorf("main.go content: %q", b)
	}
	if tgt, err := os.Readlink(filepath.Join(dst, "readme-link")); err != nil || tgt != "README.md" {
		t.Errorf("symlink: %q %v", tgt, err)
	}
	fi, _ := os.Stat(filepath.Join(dst, "src", "main.go"))
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode not preserved: %v", fi.Mode())
	}
	if _, err := os.Stat(filepath.Join(dst, fcDir)); !os.IsNotExist(err) {
		t.Error(".filecache must not be carried in the tar")
	}
}

// TestApplyTarAtomicOverwrite verifies a file lands via rename with no
// leftover temp file and correct final content.
func TestApplyTarAtomicOverwrite(t *testing.T) {
	dst := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dst, "f"), []byte("old"), 0o644))

	src := t.TempDir()
	must(t, os.WriteFile(filepath.Join(src, "f"), []byte("new-content"), 0o644))
	ents, _ := scanNAS(src)
	var buf bytes.Buffer
	must(t, writeTar(&buf, src, ents))
	must(t, applyTarToNAS(dst, &buf))

	if b, _ := os.ReadFile(filepath.Join(dst, "f")); string(b) != "new-content" {
		t.Errorf("overwrite: %q", b)
	}
	matches, _ := filepath.Glob(filepath.Join(dst, "*.fc-tmp.*"))
	if len(matches) != 0 {
		t.Errorf("temp files left behind: %v", matches)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// stubGuest serves a fixed find listing and enough of Guest for one pushCycle.
type stubGuest struct {
	listing string
	tar     []byte // returned by ReadFile for the push tar
}

func (g *stubGuest) Run(_ context.Context, _ string, argv ...string) (string, error) {
	cmd := argv[len(argv)-1]
	if strings.Contains(cmd, "find") {
		return g.listing, nil
	}
	return "", nil
}
func (g *stubGuest) WriteFile(context.Context, string, string, uint32, []byte) error { return nil }
func (g *stubGuest) ReadFile(context.Context, string, string) ([]byte, error)        { return g.tar, nil }
func (g *stubGuest) PushTar(context.Context, string, string, io.Reader) error        { return nil }
func (g *stubGuest) Remove(context.Context, string, string, bool) error              { return nil }

// TestPushCycleSettleWindow: a file whose mtime is inside the settle window is
// held back by a periodic push (still being written) but published by the
// barrier's settle=0 push — losing a hot file at teardown would lose data.
func TestPushCycleSettleWindow(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, fcDir, "journal"), 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	listing := "f\thot.bin\t" + strconv.FormatInt(now, 10) + ".0\t3\t\n"

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{Name: "hot.bin", Mode: 0o644, Size: 3, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	g := &stubGuest{listing: listing, tar: tarBuf.Bytes()}
	s := newSyncer(g, "sock", "/workspace", ws, "w1")

	puts, _, err := s.pushCycle(t.Context(), settleWindow)
	if err != nil {
		t.Fatalf("settled push: %v", err)
	}
	if puts != 0 {
		t.Fatalf("hot file published by periodic push: puts=%d", puts)
	}
	if _, ok := s.manifest["hot.bin"]; ok {
		t.Fatal("hot file entered the manifest while held back")
	}

	puts, _, err = s.pushCycle(t.Context(), 0)
	if err != nil {
		t.Fatalf("barrier push: %v", err)
	}
	if puts != 1 {
		t.Fatalf("barrier push skipped the hot file: puts=%d", puts)
	}
	if b, err := os.ReadFile(filepath.Join(ws, "hot.bin")); err != nil || string(b) != "abc" {
		t.Fatalf("hot.bin on NAS = %q, %v", b, err)
	}
}

// recDisk records Disk calls so the pre-attach fast path is observable.
type recDisk struct{ calls []string }

func (d *recDisk) Attach(_ context.Context, vm, raw, name string) error {
	d.calls = append(d.calls, "attach")
	return nil
}

func (d *recDisk) Mount(_ context.Context, _, _, _ string) error {
	d.calls = append(d.calls, "mount")
	return nil
}

func (d *recDisk) Unmount(_ context.Context, _, _ string) error {
	d.calls = append(d.calls, "unmount")
	return nil
}

func (d *recDisk) Detach(_ context.Context, _, _ string) error {
	d.calls = append(d.calls, "detach")
	return nil
}

// TestAttachAndMountUsesPreAttachedImage: with the image already provisioned
// by refill, a claim pays only the mount; without one it does the full
// provision — the fallback that keeps adopted VMs working.
func TestAttachAndMountUsesPreAttachedImage(t *testing.T) {
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 not on PATH")
	}
	d := &recDisk{}
	p := &diskProvisioner{disk: d, root: t.TempDir(), sizeMB: 4}

	if err := p.preAttach(t.Context(), "vm-a"); err != nil {
		t.Fatalf("preAttach: %v", err)
	}
	if want := []string{"attach"}; !slices.Equal(d.calls, want) {
		t.Fatalf("preAttach calls = %v, want %v", d.calls, want)
	}
	if err := p.attachAndMount(t.Context(), "vm-a", "sock", "/workspace"); err != nil {
		t.Fatalf("attachAndMount: %v", err)
	}
	if want := []string{"attach", "mount"}; !slices.Equal(d.calls, want) {
		t.Fatalf("fast path re-provisioned: calls = %v, want %v", d.calls, want)
	}

	d.calls = nil
	if err := p.attachAndMount(t.Context(), "vm-b", "sock", "/workspace"); err != nil {
		t.Fatalf("fallback attachAndMount: %v", err)
	}
	if want := []string{"attach", "mount"}; !slices.Equal(d.calls, want) {
		t.Fatalf("fallback calls = %v, want %v", d.calls, want)
	}

	p.cleanupVM("vm-a")
	if _, err := os.Stat(p.rawPath("vm-a")); !os.IsNotExist(err) {
		t.Fatal("cleanupVM left the image behind")
	}
}
