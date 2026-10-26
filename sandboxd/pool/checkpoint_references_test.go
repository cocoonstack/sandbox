package pool

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	failWrite = iota
	captureWrite
	failAfterRename
)

func TestClaimCheckpointReferencesOutliveUnacknowledgedWrites(t *testing.T) {
	recreate := func(s *claimStore) { s.set(&types.Sandbox{ID: "owner", ReplicaCk: "recreated"}) }
	for _, tc := range []struct {
		name                            string
		fault                           int
		acked, next                     *types.Sandbox
		startup                         func(claims map[string]*types.Sandbox)
		mutate                          func(s *claimStore)
		onDisk, afterFault, afterCommit []string
	}{
		{
			name: "captured write then clear", fault: captureWrite,
			next:   &types.Sandbox{ID: "owner", ReplicaPending: "captured"},
			mutate: func(s *claimStore) { s.set(&types.Sandbox{ID: "owner"}) },
			onDisk: []string{"captured"}, afterFault: []string{"captured"},
		},
		{
			name: "captured write then delete and recreate", fault: captureWrite,
			next:   &types.Sandbox{ID: "owner", ReplicaCk: "captured"},
			mutate: func(s *claimStore) { s.del("owner"); recreate(s) },
			onDisk: []string{"captured"}, afterFault: []string{"captured", "recreated"}, afterCommit: []string{"recreated"},
		},
		{
			name: "captured write then reset and recreate", fault: captureWrite,
			next:   &types.Sandbox{ID: "owner", ReplicaCk: "captured"},
			mutate: func(s *claimStore) { s.reset(map[string]*types.Sandbox{}); recreate(s) },
			onDisk: []string{"captured"}, afterFault: []string{"captured", "recreated"}, afterCommit: []string{"recreated"},
		},
		{
			name: "failed startup rewrite clears the loaded reference", fault: failWrite,
			acked:   &types.Sandbox{ID: "owner", ReplicaPending: "loaded"},
			startup: func(claims map[string]*types.Sandbox) { claims["owner"].ReplicaPending = "" },
			onDisk:  []string{"loaded"}, afterFault: []string{"loaded"},
		},
		{
			name: "failed startup rewrite drops the loaded claim", fault: failWrite,
			acked:   &types.Sandbox{ID: "owner", ReplicaPending: "loaded"},
			startup: func(claims map[string]*types.Sandbox) { delete(claims, "owner") },
			onDisk:  []string{"loaded"}, afterFault: []string{"loaded"},
		},
		{
			name: "failed writes then a return to the original", fault: failWrite,
			acked: &types.Sandbox{ID: "owner", ReplicaCk: "original"},
			next:  &types.Sandbox{ID: "owner", ReplicaCk: "attempt-1"},
			mutate: func(s *claimStore) {
				for _, ck := range []string{"attempt-2", "attempt-3", "attempt-4"} {
					_ = s.commit(s.set(&types.Sandbox{ID: "owner", ReplicaCk: ck}))
				}
				s.set(&types.Sandbox{ID: "owner", ReplicaCk: "original"})
			},
			onDisk:      []string{"original"},
			afterFault:  []string{"attempt-1", "attempt-2", "attempt-3", "attempt-4", "original"},
			afterCommit: []string{"original"},
		},
		{
			name: "write fails after the rename", fault: failAfterRename,
			acked: &types.Sandbox{ID: "owner", ArchiveCk: "archive", ArchivePending: "archive-pending", ReplicaCk: "replica", ReplicaPending: "replica-pending", FromCheckpoint: "branch-origin"},
			next:  &types.Sandbox{ID: "owner", ReplicaPending: "uncertain"},
			mutate: func(s *claimStore) {
				s.set(&types.Sandbox{ID: "owner"}, &types.Sandbox{ID: "other", ReplicaPending: "other-replica"})
			},
			onDisk:      []string{"uncertain"},
			afterFault:  []string{"archive", "archive-pending", "other-replica", "replica", "replica-pending", "uncertain"},
			afterCommit: []string{"other-replica"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fault == failAfterRename && os.Geteuid() == 0 {
				t.Skip("requires non-root: root can read the write-only directory")
			}
			dir := t.TempDir()
			s := newClaimStore(dir, tc.fault == failAfterRename)
			if tc.acked != nil {
				if err := s.commit(s.set(tc.acked)); err != nil {
					t.Fatal(err)
				}
			}
			var snap claimSnapshot
			if tc.startup != nil {
				s = newClaimStore(dir, false)
				claims, err := s.load()
				if err != nil {
					t.Fatal(err)
				}
				tc.startup(claims)
				snap = s.reset(claims)
			} else {
				snap = s.set(tc.next)
			}
			var finish func()
			switch tc.fault {
			case failWrite:
				finish = failClaimWrites(t, s)
				if err := s.commit(snap); err == nil {
					t.Fatal("write succeeded under the fault")
				}
			case failAfterRename:
				if err := os.Chmod(dir, 0o300); err != nil {
					t.Fatal(err)
				}
				finish = func() { _ = os.Chmod(dir, 0o700) }
				t.Cleanup(finish)
				if err := s.commit(snap); !errors.Is(err, fs.ErrPermission) {
					t.Fatalf("want a failed directory sync after the rename: %v", err)
				}
			case captureWrite:
				release := captureClaimWrite(t, s, snap, "owner")
				finish = func() { release() }
			}
			if tc.mutate != nil {
				tc.mutate(s)
			}
			finish()
			loaded, err := newClaimStore(dir, false).load()
			if err != nil {
				t.Fatal(err)
			}
			var onDisk []string
			for _, sb := range loaded {
				onDisk = appendRefs(onDisk, checkpointRefs(sb))
			}
			slices.Sort(onDisk)
			if pinned := slices.Sorted(maps.Keys(s.checkpointReferences())); !slices.Equal(onDisk, tc.onDisk) || !slices.Equal(pinned, tc.afterFault) {
				t.Fatalf("after the fault: on disk %v, pinned %v; want %v, %v", onDisk, pinned, tc.onDisk, tc.afterFault)
			}
			if err = s.commit(s.mark()); err != nil {
				t.Fatal(err)
			}
			if pinned := slices.Sorted(maps.Keys(s.checkpointReferences())); !slices.Equal(pinned, tc.afterCommit) {
				t.Fatalf("after the next commit: pinned %v, want %v", pinned, tc.afterCommit)
			}
		})
	}
}

func TestClaimSharedCheckpointReferencesRetirePerOwner(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	if err := s.commit(s.set(&types.Sandbox{ID: "first", ReplicaCk: "shared", ArchivePending: "shared"}, &types.Sandbox{ID: "second", ArchiveCk: "shared"})); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(s.del("first")); err != nil || !s.referencesCheckpoint("shared") {
		t.Fatalf("one owner's retirement released another owner's checkpoint: %v", err)
	}
	if err := s.commit(s.del("second")); err != nil || len(s.checkpointReferences()) != 0 {
		t.Fatalf("last owner left the shared checkpoint pinned: %v", err)
	}
}

func failClaimWrites(t *testing.T, s *claimStore) func() {
	t.Helper()
	tmp := s.path + ".tmp"
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	s.recommitting.Store(true)
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
	if err := syscall.Mkfifo(s.path+".tmp", 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.commit(snap) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.retained[owner]) != 0 || len(s.retainedVolumes[owner]) != 0
	})
	return func() []byte {
		r, err := os.Open(s.path + ".tmp")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(r)
		_ = r.Close()
		if err = errors.Join(err, <-done); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(s.path); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(s.path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return raw
	}
}
