package pool

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestClaimCheckpointReferencesTrackPossibleSnapshots(t *testing.T) {
	if withoutDACOverride(t) {
		return
	}
	dir := t.TempDir()
	s := newClaimStore(dir, true)
	sb := &types.Sandbox{ID: "owner", ArchiveCk: "archive", ArchivePending: "archive-pending", ReplicaCk: "replica", ReplicaPending: "replica-pending", FromCheckpoint: "branch-origin"}
	snap := s.set(sb)
	if refs := s.checkpointReferences(); len(refs) != 4 {
		t.Fatalf("current references: %+v", refs)
	}
	if err := s.commit(snap); err != nil {
		t.Fatal(err)
	}
	sb.ReplicaPending = "uncertain-replica"
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	err := s.commit(s.set(sb))
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("expected post-rename permission failure: %v", err)
	}
	if err = os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	loaded, err := newClaimStore(dir, false).load()
	if err != nil || loaded[sb.ID].ReplicaPending != "uncertain-replica" {
		t.Fatalf("post-rename file: %+v, %v", loaded, err)
	}
	sb.ArchiveCk, sb.ArchivePending, sb.ReplicaCk, sb.ReplicaPending = "", "", "", ""
	s.set(sb)
	refs := s.checkpointReferences()
	if len(refs) != 5 {
		t.Fatalf("lost snapshot references: %+v", refs)
	}
	delete(refs, "uncertain-replica")
	if !s.referencesCheckpoint("uncertain-replica") {
		t.Fatal("caller mutated retained reference set")
	}
	s.set(&types.Sandbox{ID: "other", ReplicaPending: "other-replica"})
	if s.hasCheckpointReferences("unknown") || !s.hasCheckpointReferences("other") {
		t.Fatal("checkpoint references crossed ownership")
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.hasCheckpointReferences(sb.ID) || len(s.checkpointReferences()) != 1 {
		t.Fatal("acknowledgment did not retire only removed references")
	}
}

func TestClaimCapturedCheckpointReferenceSurvivesConcurrentClear(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ReplicaPending: "captured-replica"}
	release := captureClaimWrite(t, s, s.set(sb), sb.ID)
	sb.ReplicaPending = ""
	s.set(sb)
	if !s.referencesCheckpoint("captured-replica") {
		t.Fatal("captured reference disappeared before acknowledgment")
	}
	var rows map[string]*types.Sandbox
	if err := json.Unmarshal(release(), &rows); err != nil {
		t.Fatal(err)
	}
	if rows[sb.ID].ReplicaPending != "captured-replica" {
		t.Fatal("captured write lost its original checkpoint")
	}
	if !s.referencesCheckpoint("captured-replica") {
		t.Fatal("acknowledgment lost captured checkpoint")
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.hasCheckpointReferences(sb.ID) {
		t.Fatal("later acknowledgment retained obsolete checkpoint")
	}
}

func TestClaimCheckpointReferencesSurviveFailedStartupRewrite(t *testing.T) {
	for _, omit := range []bool{false, true} {
		t.Run(fmt.Sprint(omit), func(t *testing.T) {
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
			if omit {
				delete(claims, sb.ID)
			} else {
				claims[sb.ID].ReplicaPending = ""
			}
			restore := failClaimWrites(t, s)
			if err = s.save(claims); err == nil {
				t.Fatal("startup rewrite unexpectedly acknowledged")
			}
			if !s.referencesCheckpoint("before-recovery") {
				t.Fatal("failed startup rewrite retired loaded checkpoint")
			}
			restore()
			if err = s.save(claims); err != nil {
				t.Fatal(err)
			}
			if s.hasCheckpointReferences(sb.ID) || s.referencesCheckpoint("before-recovery") {
				t.Fatal("successful startup rewrite retained old checkpoint")
			}
		})
	}
}

func TestClaimSharedCheckpointReferencesRetirePerOwner(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	first := &types.Sandbox{ID: "first", ReplicaCk: "shared", ArchivePending: "shared"}
	second := &types.Sandbox{ID: "second", ArchiveCk: "shared"}
	if err := s.commit(s.set(first, second)); err != nil {
		t.Fatal(err)
	}
	s.del(first.ID)
	if !s.referencesCheckpoint("shared") {
		t.Fatal("memory deletion lost shared checkpoint")
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if !s.referencesCheckpoint("shared") || s.hasCheckpointReferences(first.ID) || !s.hasCheckpointReferences(second.ID) {
		t.Fatal("one owner's retirement changed another owner's reference")
	}
	if err := s.commit(s.del(second.ID)); err != nil {
		t.Fatal(err)
	}
	if s.referencesCheckpoint("shared") || len(s.checkpointReferences()) != 0 {
		t.Fatal("last owner left shared checkpoint pinned")
	}
}

func TestClaimReferenceChangesStayDirtyAfterFailedReturnToOriginal(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ReplicaCk: "original"}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	restore := failClaimWrites(t, s)
	sb.ReplicaCk = "attempted"
	if err := s.commit(s.set(sb)); err == nil {
		t.Fatal("expected write failure")
	}
	sb.ReplicaCk = "original"
	s.set(sb)
	if !s.referencesCheckpoint("original") || !s.referencesCheckpoint("attempted") {
		t.Fatal("failed return lost a possible snapshot")
	}
	restore()
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if !s.referencesCheckpoint("original") || s.referencesCheckpoint("attempted") {
		t.Fatal("successful return did not retire failed attempt")
	}
}

func TestClaimCapturedResetAndRecreatedOwner(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprint(reset), func(t *testing.T) {
			s := newClaimStore(t.TempDir(), false)
			sb := &types.Sandbox{ID: "owner", ReplicaCk: "captured"}
			release := captureClaimWrite(t, s, s.set(sb), sb.ID)
			if reset {
				s.reset(map[string]*types.Sandbox{})
			} else {
				s.del(sb.ID)
			}
			s.set(&types.Sandbox{ID: sb.ID, ReplicaCk: "recreated"})
			var captured map[string]*types.Sandbox
			if err := json.Unmarshal(release(), &captured); err != nil {
				t.Fatal(err)
			}
			if captured[sb.ID].ReplicaCk != "captured" {
				t.Fatal("in-flight snapshot changed")
			}
			if !s.referencesCheckpoint("captured") || !s.referencesCheckpoint("recreated") {
				t.Fatal("acknowledgment lost captured or newer reference")
			}
			if err := s.commit(s.mark()); err != nil {
				t.Fatal(err)
			}
			if s.referencesCheckpoint("captured") || !s.referencesCheckpoint("recreated") {
				t.Fatal("new snapshot did not retire old owner references")
			}
		})
	}
}

func TestClaimReferencesSurviveRenameFailure(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", ReplicaCk: "captured"}
	if err := os.Mkdir(s.path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(s.set(sb)); err == nil {
		t.Fatal("expected rename failure")
	}
	s.del(sb.ID)
	if !s.referencesCheckpoint("captured") {
		t.Fatal("failed rename released attempted checkpoint")
	}
	if err := os.Remove(s.path); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.referencesCheckpoint("captured") {
		t.Fatal("successful rewrite did not retire failed snapshot")
	}
}

func failClaimWrites(t *testing.T, s *claimStore) func() {
	t.Helper()
	tmp := s.path + ".tmp"
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	restore := sync.OnceFunc(func() {
		if err := os.Remove(tmp); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(restore)
	return restore
}

func captureClaimWrite(t *testing.T, s *claimStore, snap claimSnapshot, owner string) func() []byte {
	t.Helper()
	fifo := s.path + ".tmp"
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.commit(snap) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.retained[owner].attempted.empty()
	})
	var raw []byte
	release := sync.OnceFunc(func() {
		r, err := os.Open(fifo)
		if err != nil {
			t.Fatal(err)
		}
		raw, err = io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(s.path); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(release)
	return func() []byte { release(); return raw }
}
