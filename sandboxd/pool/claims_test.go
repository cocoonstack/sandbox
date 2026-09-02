package pool

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestStoreRoundTrip(t *testing.T) {
	s := newClaimStore(t.TempDir())
	claims := map[string]*types.Sandbox{
		"sb_a": {ID: "sb_a", VMName: "sbx-1", Key: testKey, Token: "t1", Deadline: time.Now().Add(time.Minute).UTC(), ClaimRef: "ns/workload", Volumes: []types.Volume{{Name: "dataset-a", Mount: "/datasets/a"}}, VsockSocket: "/v/1"},
		"sb_b": {ID: "sb_b", VMName: "sbx-2", Key: testKey, Token: "t2"},
	}

	if err := s.save(claims); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got, claims) {
		t.Errorf("got %+v, want %+v", got, claims)
	}
}

func TestClaimStoreSnapshotDetachesVolumes(t *testing.T) {
	s := newClaimStore(t.TempDir())
	sb := &types.Sandbox{ID: "sb_a", Volumes: []types.Volume{{Name: "dataset-a", Mount: "/datasets/a"}}}
	snap := s.set(sb)
	sb.Volumes[0].Mount = "/mutated"
	if err := s.commit(snap); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	loaded := got[sb.ID]
	if loaded == nil {
		t.Fatal("detached snapshot omitted claim")
	}
	if !slices.Equal(loaded.Volumes, []types.Volume{{Name: "dataset-a", Mount: "/datasets/a"}}) {
		t.Errorf("volumes %v, want detached snapshot", loaded.Volumes)
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

func TestClaimStoreCommitCoalesces(t *testing.T) {
	s := newClaimStore(t.TempDir())
	older := s.set(&types.Sandbox{ID: "sb_a"})
	newer := s.set(&types.Sandbox{ID: "sb_b"})
	if err := s.commit(newer); err != nil {
		t.Fatalf("commit newer: %v", err)
	}
	if err := s.commit(older); err != nil {
		t.Fatalf("commit older: %v", err)
	}
	if got, err := s.load(); err != nil || len(got) != 2 {
		t.Errorf("stale snapshot overwrote the newer state: %d claims (%v), want 2", len(got), err)
	}
}

func TestClaimStoreIncrementalProjection(t *testing.T) {
	s := newClaimStore(t.TempDir())
	a := &types.Sandbox{ID: "sb_a", VMName: "sbx-1", Key: testKey}
	b := &types.Sandbox{ID: "sb_b", VMName: "sbx-2", Key: testKey}
	if err := s.commit(s.set(a, b)); err != nil {
		t.Fatalf("commit set: %v", err)
	}
	a.HibernateSnap = "snap-a"
	if err := s.commit(s.set(a)); err != nil {
		t.Fatalf("commit update: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 || got["sb_a"].HibernateSnap != "snap-a" || got["sb_b"].VMName != "sbx-2" {
		t.Fatalf("after incremental update: %+v", got)
	}
	if delErr := s.commit(s.del("sb_a")); delErr != nil {
		t.Fatalf("commit del: %v", delErr)
	}
	if got, err = s.load(); err != nil || len(got) != 1 || got["sb_b"] == nil {
		t.Fatalf("after del: %+v, %v", got, err)
	}
	if resetErr := s.commit(s.reset(map[string]*types.Sandbox{"sb_c": {ID: "sb_c", Key: testKey}})); resetErr != nil {
		t.Fatalf("commit reset: %v", resetErr)
	}
	if got, err = s.load(); err != nil || len(got) != 1 || got["sb_c"] == nil {
		t.Fatalf("after reset: %+v, %v", got, err)
	}
}

func TestClaimStoreMarkRepersists(t *testing.T) {
	s := newClaimStore(t.TempDir())
	s.set(&types.Sandbox{ID: "sb_a", Key: testKey})
	if s.synced() {
		t.Fatal("synced with an unwritten change")
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatalf("commit mark: %v", err)
	}
	if !s.synced() {
		t.Error("not synced after flushing the mark")
	}
	if got, err := s.load(); err != nil || len(got) != 1 {
		t.Errorf("after mark flush: %+v, %v", got, err)
	}
}

func TestClaimStoreConcurrentCommits(t *testing.T) {
	s := newClaimStore(t.TempDir())
	sb := &types.Sandbox{ID: "sb_a"}
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { _ = s.commit(s.set(sb)) })
	}
	wg.Wait()
	if got, err := s.load(); err != nil || len(got) != 1 {
		t.Fatalf("after concurrent commits: %+v, %v", got, err)
	}
}
