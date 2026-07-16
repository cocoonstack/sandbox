package pool

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJournalRotateFailureDegradesAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.Truncate(path, journalMaxBytes); err != nil {
		t.Fatalf("setup: %v", err)
	}
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	defer func() { _ = j.close() }()
	// A directory at the backup path makes the rotation rename fail.
	if err = os.Mkdir(path+".1", 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err = j.append(map[string]string{"ev": "a"}); err == nil {
		t.Fatal("append during a blocked rotation must surface the rename error")
	}
	if err = j.append(map[string]string{"ev": "b"}); err == nil {
		t.Fatal("append while rotation stays blocked must keep surfacing the rename error")
	}
	if got := tail(t, path); !strings.Contains(got, `"ev":"a"`) || !strings.Contains(got, `"ev":"b"`) {
		t.Errorf("degraded journal dropped events: %q", got)
	}
	if err = os.Remove(path + ".1"); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	if err = j.append(map[string]string{"ev": "c"}); err != nil {
		t.Fatalf("append after unblocked rotation: %v", err)
	}
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read live: %v", err)
	}
	if !strings.Contains(string(live), `"ev":"c"`) || strings.Contains(string(live), `"ev":"a"`) {
		t.Errorf("rotation did not swap the live file: %q", live)
	}
	if got := tail(t, path+".1"); !strings.Contains(got, `"ev":"a"`) || !strings.Contains(got, `"ev":"b"`) {
		t.Errorf("backup missing degraded events: %q", got)
	}
}

func tail(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	buf := make([]byte, 1024)
	n, err := f.ReadAt(buf, max(st.Size()-int64(len(buf)), 0))
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(buf[:n])
}
