package pool

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestForkFromRunning(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)

	children, err := m.Fork(t.Context(), parent.ID, parent.Token, 3, time.Hour)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if len(children) != 3 {
		t.Fatalf("got %d children, want 3", len(children))
	}
	if len(eng.snapSaves) != 1 || !strings.HasPrefix(eng.snapSaves[0], forkPrefix) {
		t.Errorf("snapSaves %v, want one %s* snapshot", eng.snapSaves, forkPrefix)
	}
	if !slices.Equal(eng.exports, eng.snapSaves) {
		t.Errorf("exported %v, want the fork snapshot %v", eng.exports, eng.snapSaves)
	}
	if !slices.Contains(eng.snapRemoves, eng.snapSaves[0]) {
		t.Errorf("snapRemoves %v, want the transient fork snapshot dropped", eng.snapRemoves)
	}
	ids := map[string]bool{parent.ID: true}
	for _, c := range children {
		if ids[c.ID] {
			t.Errorf("duplicate child id %q", c.ID)
		}
		ids[c.ID] = true
		if c.Key != parent.Key {
			t.Errorf("child key %+v, want parent's %+v", c.Key, parent.Key)
		}
		if d := c.Deadline.Sub(time.Now().Add(time.Hour)).Abs(); d > time.Second {
			t.Errorf("child deadline off requested TTL by %v", d)
		}
	}
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 4 {
		t.Errorf("persisted %d claims, want parent + 3 children", len(got))
	}
	leftovers, _ := filepath.Glob(filepath.Join(m.dataDir, "fork-*"))
	if len(leftovers) != 0 {
		t.Errorf("export staging left behind: %v", leftovers)
	}
}

func TestForkFromHibernatedUsesWakeImage(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	children, err := m.Fork(t.Context(), parent.ID, parent.Token, 2, 0)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("got %d children, want 2", len(children))
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("snapSaves %v, want none — the hibernate image is the source", eng.snapSaves)
	}
	hibSnap := eng.hibernates[0]
	if !slices.Equal(eng.exports, []string{hibSnap}) {
		t.Errorf("exports %v, want the hibernate snapshot %q", eng.exports, hibSnap)
	}
	if slices.Contains(eng.snapRemoves, hibSnap) {
		t.Error("hibernate snapshot dropped by fork — the parent could never wake")
	}
	// The parent stays hibernated and must still wake.
	if _, err := m.WakeAgentSocket(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("wake after fork: %v", err)
	}
}

func TestForkCountValidation(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	for _, count := range []int{0, -1, m.maxFork + 1} {
		if _, err := m.Fork(t.Context(), parent.ID, parent.Token, count, 0); !errors.Is(err, ErrBadCount) {
			t.Errorf("count %d: err %v, want ErrBadCount", count, err)
		}
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("invalid counts still snapshotted: %v", eng.snapSaves)
	}
}

func TestForkUnknownSandbox(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	if _, err := m.Fork(t.Context(), parent.ID, "wrong-token", 1, 0); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("err %v, want ErrUnknownSandbox", err)
	}
}

func TestForkAllOrNothing(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent, err := claimAny(t.Context(), m, testKey, 0) // cold boot, no Clone calls yet
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	eng.cloneFailNth = 2 // fail one of the three fork clones

	if _, err := m.Fork(t.Context(), parent.ID, parent.Token, 3, 0); err == nil {
		t.Fatal("Fork succeeded despite a failed clone")
	}
	// Every successfully built child is destroyed; only the parent's claim
	// survives in memory and on disk.
	if _, claimed, _ := m.Info(); claimed != 1 {
		t.Errorf("claimed=%d, want only the parent", claimed)
	}
	if got, _ := newClaimStore(m.dataDir).load(); len(got) != 1 {
		t.Errorf("persisted %d claims, want only the parent", len(got))
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.vms) != 1 {
		t.Errorf("%d VMs alive, want only the parent (children destroyed)", len(eng.vms))
	}
}

func TestReconcileSweepsOrphanSnapshots(t *testing.T) {
	eng := newFakeEngine()
	dataDir := t.TempDir()
	m := newTestManagerAt(t, eng, dataDir)
	parent := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	referenced := eng.hibernates[0]
	eng.mu.Lock()
	eng.snapshots = []string{
		referenced,
		hibernatePrefix + "orphan-1",
		forkPrefix + "stale-2",
		goldenPrefix + "stale-3",
		"user-snapshot", // not ours: never touched
	}
	eng.mu.Unlock()

	// A fresh manager over the same journal models the post-crash restart.
	m2 := newTestManagerAt(t, eng, dataDir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, want := range []string{hibernatePrefix + "orphan-1", forkPrefix + "stale-2", goldenPrefix + "stale-3"} {
		if !slices.Contains(eng.snapRemoves, want) {
			t.Errorf("snapshot %q not swept (removed: %v)", want, eng.snapRemoves)
		}
	}
	for _, keep := range []string{referenced, "user-snapshot"} {
		if slices.Contains(eng.snapRemoves, keep) {
			t.Errorf("snapshot %q swept but must be kept", keep)
		}
	}
	// The adopted hibernated claim still wakes after the restart.
	if _, err := m2.WakeAgentSocket(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("wake after reconcile: %v", err)
	}
}
