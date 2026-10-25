package pool

import (
	"encoding/json/v2"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClaimCheckpointReferencesTrackPossibleSnapshots(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ArchiveCk: "archive", ArchivePending: "archive-pending", ReplicaCk: "replica", ReplicaPending: "replica-pending", FromCheckpoint: "branch-origin"}
	snap := s.set(sb)
	if refs := s.checkpointRefs(sb.ID); len(refs) != 4 {
		t.Fatalf("current references: %+v", refs)
	}
	if err := s.commit(snap); err != nil {
		t.Fatal(err)
	}
	sb.ReplicaPending = "uncertain-replica"
	s.replaceFile = func(raw []byte) error {
		if err := s.replace(raw); err != nil {
			return err
		}
		return errors.New("injected lost replacement acknowledgement")
	}
	if err := s.commit(s.set(sb)); err == nil {
		t.Fatal("replacement unexpectedly acknowledged")
	}
	loaded, err := newClaimStore(filepath.Dir(s.path), false).load()
	if err != nil || loaded[sb.ID].ReplicaPending != "uncertain-replica" {
		t.Fatalf("lost acknowledgement fixture: %+v, %v", loaded, err)
	}
	sb.ArchiveCk, sb.ArchivePending, sb.ReplicaCk, sb.ReplicaPending = "", "", "", ""
	s.set(sb)
	s.set(&types.Sandbox{ID: "other", ReplicaPending: "other-replica"})
	refs := s.checkpointRefs(sb.ID)
	if len(refs) != 5 {
		t.Fatalf("lost snapshot references: %+v", refs)
	}
	delete(refs, "uncertain-replica")
	if _, retained := s.checkpointRefs(sb.ID)["uncertain-replica"]; !retained {
		t.Fatal("caller mutated the retained reference set")
	}
	if len(s.checkpointRefs("unknown")) != 0 || len(s.checkpointRefs("other")) != 1 {
		t.Fatal("checkpoint references crossed claim ownership")
	}
	s.replaceFile = nil
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if len(s.checkpointRefs(sb.ID)) != 0 || len(s.checkpointRefs()) != 1 {
		t.Fatal("acknowledged snapshot did not retire only the removed references")
	}
}

func TestClaimCapturedCheckpointReferenceSurvivesConcurrentClear(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ReplicaPending: "captured-replica"}
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	s.replaceFile = func(raw []byte) error {
		close(entered)
		<-resume
		var rows map[string]*types.Sandbox
		if err := json.Unmarshal(raw, &rows); err != nil {
			return err
		}
		if rows[sb.ID].ReplicaPending != "captured-replica" {
			return errors.New("captured write lost its original checkpoint")
		}
		return s.replace(raw)
	}
	snap := s.set(sb)
	committed := make(chan error, 1)
	go func() { committed <- s.commit(snap) }()
	<-entered
	sb.ReplicaPending = ""
	s.set(sb)
	if _, retained := s.checkpointRefs(sb.ID)["captured-replica"]; !retained {
		t.Error("captured replacement reference disappeared before acknowledgement")
	}
	release()
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	s.replaceFile = nil
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if len(s.checkpointRefs(sb.ID)) != 0 {
		t.Fatal("later acknowledgement retained an obsolete checkpoint")
	}
}

func TestClaimCheckpointReferencesSurviveFailedStartupRewrite(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ReplicaPending: "before-recovery"}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	s = newClaimStore(filepath.Dir(s.path), false)
	claims, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	claims[sb.ID].ReplicaPending = ""
	s.replaceFile = func([]byte) error { return errors.New("startup rewrite failed") }
	if err = s.save(claims); err == nil {
		t.Fatal("startup rewrite unexpectedly acknowledged")
	}
	if _, retained := s.checkpointRefs(sb.ID)["before-recovery"]; !retained {
		t.Fatal("failed startup rewrite retired the loaded checkpoint")
	}
	s.replaceFile = nil
	if err = s.save(claims); err != nil {
		t.Fatal(err)
	}
	if len(s.checkpointRefs(sb.ID)) != 0 {
		t.Fatal("successful startup rewrite retained the old checkpoint")
	}
}

func TestRecommitKeepsOneRetrierWhileWritesFail(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	dir := filepath.Join(t.TempDir(), "absent")
	m.store.path = filepath.Join(dir, "claims.json")

	before := runtime.NumGoroutine()
	for range 50 {
		m.mu.Lock()
		snap := m.store.set(sb)
		m.mu.Unlock()
		m.store.recommit(t.Context(), snap)
	}
	if grown := runtime.NumGoroutine() - before; grown > 2 {
		t.Fatalf("%d goroutines for 50 failed persists, want one retrier", grown)
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	waitFor(t, func() bool {
		_, err := os.Stat(m.store.path)
		return err == nil && !m.store.recommitting.Load()
	})
}

func TestRecommitDrainsAChangeItWasTooBusyToSee(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	m.store.path = filepath.Join(t.TempDir(), "claims.json")
	fifo := m.store.path + ".tmp"
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	change := func() claimSnapshot {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.store.set(sb)
	}

	m.store.recommit(t.Context(), change())
	waitFor(t, func() bool {
		if m.store.writeMu.TryLock() {
			m.store.writeMu.Unlock()
			return false
		}
		return true
	})
	m.store.recommit(t.Context(), change())

	r, err := os.Open(fifo)
	if err != nil {
		t.Fatalf("open fifo: %v", err)
	}
	_, _ = io.Copy(io.Discard, r)
	_ = r.Close()
	waitFor(t, func() bool { return !m.store.recommitting.Load() && !m.store.pending(m.store.mark()) })
}

func TestStoreRoundTrip(t *testing.T) {
	dataset := []types.Volume{{Name: "dataset-a", Mount: "/datasets/a"}}
	claims := map[string]*types.Sandbox{
		"sb_a": {ID: "sb_a", VMName: "sbx-1", Key: testKey, Token: "t1", Deadline: time.Now().Add(time.Minute).UTC(), ClaimedAt: time.Now().Add(-time.Minute).UTC(), ClaimRef: "ns/workload", Metadata: types.Metadata{"team": "a"}, OnExpire: types.ExpireArchive, Volumes: dataset, VolumesWant: dataset, VsockSocket: "/v/1"},
		"sb_b": {ID: "sb_b", VMName: "sbx-2", Key: testKey, Token: "t2"},
	}
	for _, fsync := range []bool{false, true} {
		s := newClaimStore(t.TempDir(), fsync)
		if err := s.save(claims); err != nil {
			t.Fatalf("save (sync=%v): %v", fsync, err)
		}
		got, err := s.load()
		if err != nil {
			t.Fatalf("load (sync=%v): %v", fsync, err)
		}
		if !reflect.DeepEqual(got, claims) {
			t.Errorf("sync=%v: got %+v, want %+v", fsync, got, claims)
		}
		if _, err := os.Stat(s.path + ".tmp"); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("sync=%v: temp file left behind: %v", fsync, err)
		}
	}
}

func TestClaimStoreSnapshotDetachesVolumes(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
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

func TestClaimRowOmitsUnsetFlags(t *testing.T) {
	raw, err := encodeRow(new(dtoOf(&types.Sandbox{ID: "sb_1", VMName: "vm"})))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, flag := range []string{"reader_safe", "no_egress"} {
		if strings.Contains(string(raw), flag) {
			t.Errorf("row %s carries the unset flag %s", raw, flag)
		}
	}
}

func TestStoreLoadMissingFile(t *testing.T) {
	got, err := newClaimStore(t.TempDir(), false).load()
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

	if _, err := newClaimStore(dir, false).load(); err == nil {
		t.Error("load succeeded on corrupt file")
	}
}

func TestClaimStoreCommitCoalesces(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
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
	s := newClaimStore(t.TempDir(), false)
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
	s := newClaimStore(t.TempDir(), false)
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
	s := newClaimStore(t.TempDir(), false)
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

func TestClaimLoadRestoresAttachOnlyBeforeRecoveryReferences(t *testing.T) {
	dir := t.TempDir()
	raw := `{"old":{"id":"old","volumes":[{"name":"raw","mode":"rw"},{"name":"mounted","mount":"/data"}],"volumes_want":[]},"desired":{"id":"desired","volumes":[],"volumes_want":[{"name":"raw"}]},"missing":{"id":"missing","volumes":[{"name":"raw"}]}}`
	if err := os.WriteFile(filepath.Join(dir, "claims.json"), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newClaimStore(dir, false)
	claims, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	if !claims["old"].Volumes[0].AttachOnly || claims["old"].Volumes[1].AttachOnly || claims["old"].VolumesWant == nil || len(claims["old"].VolumesWant) != 0 {
		t.Fatalf("old shapes changed: %+v", claims["old"])
	}
	if !claims["desired"].VolumesWant[0].AttachOnly || !slices.Equal(claims["missing"].Volumes, claims["missing"].VolumesWant) {
		t.Fatal("legacy desired or missing-desired fallback changed")
	}
	refs := s.recoveryVolumes()
	if !refs["old"][0].AttachOnly || !refs["desired"][0].AttachOnly {
		t.Fatalf("recovery references captured before canonicalization: %+v", refs)
	}
	if err = os.WriteFile(filepath.Join(dir, "claims.json"), []byte(`{"bad":{"id":"bad","volumes":[{"name":"data","mount":"/data","attach_only":true}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = newClaimStore(dir, false).load(); err == nil {
		t.Fatal("accepted contradictory persisted shape")
	}
}
