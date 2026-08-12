package filecache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
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
