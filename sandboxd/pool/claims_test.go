package pool

import (
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestStoreRoundTrip(t *testing.T) {
	s := newClaimStore(t.TempDir())
	claims := map[string]*types.Sandbox{
		"sb_a": {ID: "sb_a", VMName: "sbx-1", Key: testKey, Token: "t1", Deadline: time.Now().Add(time.Minute).UTC(), VsockSocket: "/v/1"},
		"sb_b": {ID: "sb_b", VMName: "sbx-2", Key: testKey, Token: "t2"},
	}

	if err := s.save(claims); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !maps.EqualFunc(got, claims, func(a, b *types.Sandbox) bool { return *a == *b }) {
		t.Errorf("got %+v, want %+v", got, claims)
	}
}

func TestStoreLoadMissingFile(t *testing.T) {
	got, err := newClaimStore(t.TempDir()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d entries, want 0", len(got))
	}
}

func TestStoreLoadCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "claims.json"), []byte("{oops"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if _, err := newClaimStore(dir).load(); err == nil {
		t.Error("load succeeded on corrupt file")
	}
}

// TestClaimStoreCommitCoalesces asserts an older snapshot never overwrites a
// newer one already on disk — the guard that lets the write leave the manager
// mutex without losing the latest state.
func TestClaimStoreCommitCoalesces(t *testing.T) {
	s := newClaimStore(t.TempDir())
	one := map[string]*types.Sandbox{"sb_a": {ID: "sb_a"}}
	two := map[string]*types.Sandbox{"sb_a": {ID: "sb_a"}, "sb_b": {ID: "sb_b"}}

	older := s.snapshot(one) // seq 1
	newer := s.snapshot(two) // seq 2
	if err := s.commit(newer); err != nil {
		t.Fatalf("commit newer: %v", err)
	}
	if err := s.commit(older); err != nil { // must be a no-op, not an overwrite
		t.Fatalf("commit older: %v", err)
	}
	if got, err := s.load(); err != nil || len(got) != 2 {
		t.Errorf("stale snapshot overwrote the newer state: %d claims (%v), want 2", len(got), err)
	}
}

// TestClaimStoreConcurrentCommits exercises the writer off the manager mutex:
// many snapshot+commit pairs race, and the file must stay parseable (-race).
func TestClaimStoreConcurrentCommits(t *testing.T) {
	s := newClaimStore(t.TempDir())
	claims := map[string]*types.Sandbox{"sb_a": {ID: "sb_a"}}
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { _ = s.commit(s.snapshot(claims)) })
	}
	wg.Wait()
	if got, err := s.load(); err != nil || len(got) != 1 {
		t.Fatalf("after concurrent commits: %+v, %v", got, err)
	}
}
