package pool

import (
	"encoding/json/v2"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestJournalRotateFailureDegradesAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	j := fullJournal(t, path)
	err := os.Mkdir(path+".1", 0o750)
	if err != nil {
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

func TestJournalKeepsAnEventWithInvalidUTF8(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	if err = j.append(map[string]string{"dest": "a\xffb"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := tail(t, path); !strings.Contains(got, `"dest":"a`+"\uFFFD"+`b"`) {
		t.Errorf("journal %q, want the event with the invalid byte as U+FFFD", got)
	}
}

func TestJournalRotateReopenFailureKeepsWriting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.jsonl")
	j := fullJournal(t, path)
	j.path = filepath.Join(dir, "gone", "usage.jsonl")
	if err := j.append(map[string]string{"ev": "a"}); err == nil {
		t.Fatal("append across a failed reopen must surface the error")
	}
	if err := j.append(map[string]string{"ev": "b"}); err == nil {
		t.Fatal("append while the reopen stays blocked must keep surfacing the error")
	}
	if got := tail(t, path); !strings.Contains(got, `"ev":"a"`) || !strings.Contains(got, `"ev":"b"`) {
		t.Errorf("events lost to a closed descriptor: %q", got)
	}
}

func TestJournalKeepsTheFirstAppendAfterATornLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	if err := os.WriteFile(path, []byte(`{"ev":"claim","id":"sb_a"}`+"\n"+`{"t":"2026-10-30T00:00:00Z","ev":"rel`), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	if err = j.append(usage.Event{Event: "claim", ID: "sb_b"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	assertLastAppendOnItsOwnLine(t, j, path, "a torn line")
}

func TestJournalEndsALineAShortWriteTore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	if err = j.append(usage.Event{Event: "claim", ID: "sb_a"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	var limit syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatalf("getrlimit: %v", err)
	}
	short := limit
	short.Cur = uint64(j.size) + 5
	if err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &short); err != nil {
		t.Fatalf("setrlimit: %v", err)
	}
	tornErr := j.append(usage.Event{Event: "release", ID: "sb_a"})
	if err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatalf("restore rlimit: %v", err)
	}
	if tornErr == nil {
		t.Fatal("append past the file size limit succeeded, want a short write")
	}
	if err = j.append(usage.Event{Event: "claim", ID: "sb_b"}); err != nil {
		t.Fatalf("append after the short write: %v", err)
	}
	assertLastAppendOnItsOwnLine(t, j, path, "a short write")
}

func TestJournalLeavesAWholeLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	if err := os.WriteFile(path, []byte(`{"ev":"claim"}`+"\n"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	if j.size != 15 {
		t.Errorf("journal size %d after opening a whole file, want 15", j.size)
	}
}

func fullJournal(t *testing.T, path string) *journal {
	t.Helper()
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
	return j
}

func assertLastAppendOnItsOwnLine(t *testing.T, j *journal, path, after string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	var ev usage.Event
	if len(lines) != 3 || json.Unmarshal([]byte(lines[2]), &ev) != nil || ev.ID != "sb_b" {
		t.Errorf("journal after %s %q, want the append sb_b on a line of its own", after, raw)
	}
	if st, _ := os.Stat(path); j.size != st.Size() {
		t.Errorf("journal size %d, file %d", j.size, st.Size())
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
