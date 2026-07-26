package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// TestArchivePublishWindowPinsCheckpoint guards the pre-pin: between an
// archive's checkpoint publish and the ArchiveCk commit, the checkpoint must
// be invisible to listings and refuse deletion — a delete landing in that
// window stranded the claim.
func TestArchivePublishWindowPinsCheckpoint(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	stall := &stallingStore{Store: m.ckpts, published: make(chan string, 1), release: make(chan struct{})}
	m.ckpts = stall

	archived := make(chan error, 1)
	go func() { archived <- m.archive(t.Context(), sb) }()
	ck := <-stall.published

	if ckpts, err := m.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 0 {
		t.Errorf("mid-publish checkpoint visible: %v, %v", ckpts, err)
	}
	if err := m.DeleteCheckpoint(t.Context(), ck, "", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("mid-publish delete: %v, want ErrUnknownCheckpoint", err)
	}
	close(stall.release)
	if err := <-archived; err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake archived: %v", err)
	}
}

// TestArchiveWakeRehibernateWindowAborts guards the commit's identity check: a
// wake + re-hibernate landing inside the publish window leaves HibernateSnap
// non-empty but pointing at NEWER state than the exported checkpoint —
// committing would archive stale data and destroy the only copy of the new
// snapshot.
func TestArchiveWakeRehibernateWindowAborts(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	stall := &stallingStore{Store: m.ckpts, published: make(chan string, 1), release: make(chan struct{})}
	m.ckpts = stall

	archived := make(chan error, 1)
	go func() { archived <- m.archive(t.Context(), sb) }()
	ck := <-stall.published

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake mid-publish: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("re-hibernate mid-publish: %v", err)
	}
	m.mu.Lock()
	newSnap := sb.HibernateSnap
	m.mu.Unlock()
	close(stall.release)
	if err := <-archived; !errors.Is(err, errWokeMeanwhile) {
		t.Fatalf("archive: %v, want errWokeMeanwhile", err)
	}
	m.mu.Lock()
	archiveCk, hibSnap := sb.ArchiveCk, sb.HibernateSnap
	m.mu.Unlock()
	if archiveCk != "" || hibSnap != newSnap {
		t.Errorf("claim mutated: ArchiveCk=%q HibernateSnap=%q, want \"\" %q", archiveCk, hibSnap, newSnap)
	}
	if _, err := m.ckpts.ReadMeta(t.Context(), ck); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("orphan archive ck not reclaimed: %v", err)
	}
	eng.mu.Lock()
	dropped := slices.Contains(eng.snapRemoves, newSnap)
	eng.mu.Unlock()
	if dropped {
		t.Error("abort dropped the newer hibernate snapshot")
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake after aborted archive: %v", err)
	}
}

// TestArchiveCkHiddenAcrossNodes: on a shared store, another node has no
// in-memory pin for this node's archive wake images — the meta Archive flag
// must hide them from its listings, refuse its deletes, and spare its TTL
// sweep, or a routine cleanup elsewhere strands the claim.
func TestArchiveCkHiddenAcrossNodes(t *testing.T) {
	shared := t.TempDir()
	mkMgr := func() *Manager {
		m, err := NewManager(t.Context(), &config.Config{
			DataDir: t.TempDir(), CheckpointDir: shared,
			Pools: []config.PoolSpec{archivePool(3600)},
		}, newFakeEngine(), testSecrets(t))
		if err != nil {
			t.Fatalf("manager: %v", err)
		}
		return m
	}
	mA, mB := mkMgr(), mkMgr()
	sb := mustClaim(t, mA, testKey)
	mustArchive(t, mA, sb)
	ck := sb.ArchiveCk

	if ckpts, err := mB.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 0 {
		t.Errorf("peer lists the archive wake image: %v, %v", ckpts, err)
	}
	if err := mB.DeleteCheckpoint(t.Context(), ck, "", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("peer delete: %v, want ErrUnknownCheckpoint", err)
	}
	mB.ckptTTL = time.Nanosecond
	mB.sweepExpiredCheckpoints(t.Context())
	if !ckExists(t, mA, ck) {
		t.Fatal("peer TTL sweep deleted the archive wake image")
	}
	if _, err := mA.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("owner wake after peer sweep: %v", err)
	}
}

// TestReconcileReclaimsOrphanArchiveCk: a crash between an archive's publish
// and its journal commit leaves a flagged record listings hide and deletes
// refuse; reconcile identifies it by the journal (the sandbox is ours, under
// a different ArchiveCk) and reclaims it. A flagged record of an unknown
// sandbox is left alone.
func TestReconcileReclaimsOrphanArchiveCk(t *testing.T) {
	eng := newFakeEngine()
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_mine": {ID: "sb_mine", VMName: "sbx-gone-1", Key: testKey, Token: "tok"},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)
	seedCk := func(id, sandboxID string) {
		t.Helper()
		staging, err := m.ckpts.Stage(id)
		if err != nil {
			t.Fatalf("stage: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		meta := `{"id":"` + id + `","sandbox_id":"` + sandboxID + `","archive":true}`
		if err := os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(meta), 0o600); err != nil {
			t.Fatalf("meta: %v", err)
		}
		if err := m.ckpts.Publish(t.Context(), staging, id); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	seedCk("ck_00000000000000aa", "sb_mine")
	seedCk("ck_00000000000000bb", "sb_elsewhere")

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ckExists(t, m, "ck_00000000000000aa") {
		t.Error("orphan archive ck of a journaled sandbox not reclaimed")
	}
	if !ckExists(t, m, "ck_00000000000000bb") {
		t.Error("another node's archive ck reclaimed")
	}
}

// TestClaimCheckpointRefusesArchive: wake images are lifecycle-internal —
// branching from one would race its consumption by the owner's wake.
func TestClaimCheckpointRefusesArchive(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)

	if _, err := m.ClaimCheckpoint(t.Context(), sb.ArchiveCk, 0, ""); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("branch from archive ck: %v, want ErrUnknownCheckpoint", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("owner wake: %v", err)
	}
}

// TestArchiveLifecycleRoundTrip exercises the real sweep wiring:
// claim→idle-hibernate→archive→wake, and asserts the id/token survive the
// store round-trip while the local VM and its wake image are reclaimed.
func TestArchiveLifecycleRoundTrip(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token, origVM := sb.ID, sb.Token, sb.VMName

	backdate(m, sb, 5*time.Second)
	m.idleOnce(t.Context())
	waitFor(t, func() bool { return hibernated(m) == 1 })

	m.archiveOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 1 })

	m.mu.Lock()
	ck, vm, snap := sb.ArchiveCk, sb.VMName, sb.HibernateSnap
	m.mu.Unlock()
	if ck == "" || vm != "" || snap != "" {
		t.Fatalf("archived record ck=%q vm=%q snap=%q, want ck set + vm/snap cleared", ck, vm, snap)
	}
	if hibernated(m) != 0 {
		t.Error("archived claim still counted as hibernated")
	}
	if !ckExists(t, m, ck) {
		t.Fatal("archive did not publish the checkpoint")
	}
	waitFor(t, func() bool { return eng.removed(origVM) })

	sock, err := m.WakeAgentSocket(t.Context(), id, token)
	if err != nil {
		t.Fatalf("wake archived: %v", err)
	}
	if sock == "" {
		t.Error("wake returned an empty socket")
	}
	m.mu.Lock()
	wokeCk, wokeVM := sb.ArchiveCk, sb.VMName
	m.mu.Unlock()
	if wokeCk != "" || wokeVM == "" {
		t.Errorf("woke record ck=%q vm=%q, want ck cleared + vm set", wokeCk, wokeVM)
	}
	if archivedCount(m) != 0 {
		t.Error("woke claim still counted as archived")
	}
	if ckExists(t, m, ck) {
		t.Error("wake left the consumed checkpoint in the store (double storage billing)")
	}
}

// TestArchiveTTLSweepSparesLiveCheckpoint guards the fix where the checkpoint
// TTL sweep deleted the store image backing a live archived claim, bricking it.
func TestArchiveTTLSweepSparesLiveCheckpoint(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	m.ckptTTL = time.Nanosecond // every checkpoint is instantly age-expired
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk

	m.sweepExpiredCheckpoints(t.Context())
	if !ckExists(t, m, ck) {
		t.Fatal("TTL sweep deleted the checkpoint backing a live archived sandbox")
	}
	if _, err := m.WakeAgentSocket(t.Context(), id, token); err != nil {
		t.Fatalf("wake after sweep: %v", err)
	}
}

// TestArchiveKeepsForeverWhenDeleteZero guards the fix where a zero
// archive-delete window purged archives immediately instead of keeping them.
func TestArchiveKeepsForeverWhenDeleteZero(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(0))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk

	m.mu.Lock()
	zero := sb.Deadline.IsZero()
	m.mu.Unlock()
	if !zero {
		t.Fatalf("archive with delete=0 set Deadline %v, want zero (keep forever)", sb.Deadline)
	}

	m.reapOnce(t.Context())
	if archivedCount(m) != 1 || !ckExists(t, m, ck) {
		t.Fatal("reap purged a keep-forever archived sandbox")
	}
	if _, err := m.WakeAgentSocket(t.Context(), id, token); err != nil {
		t.Fatalf("wake keep-forever archive: %v", err)
	}
}

// TestArchiveRetentionPurge covers the reapPurge path: an archive past its
// retention deadline drops from claims, deletes its checkpoint, and releases
// its tenant slot.
func TestArchiveRetentionPurge(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk

	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second) // retention window elapsed
	m.mu.Unlock()

	m.reapOnce(t.Context())
	waitFor(t, func() bool { return archivedCount(m) == 0 })
	// archiveDeletes is the purge goroutine's last side effect, sequenced after
	// the checkpoint delete; waiting on it makes both observable (a bare read
	// races the async purge, flaking under -race).
	waitFor(t, func() bool { return m.Counters().ArchiveDeletes > 0 })
	if ckExists(t, m, ck) {
		t.Error("purge did not delete the archived checkpoint")
	}

	m.mu.Lock()
	live := m.tenantLive["acme"]
	m.mu.Unlock()
	if live != 0 {
		t.Errorf("tenant acme has %d live after purge, want 0", live)
	}
}

// TestReleaseArchivedDeletesCheckpoint guards the fix where releasing an
// archived claim orphaned its store checkpoint and called Remove on an empty
// VM name.
func TestReleaseArchivedDeletesCheckpoint(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	removesBefore := len(eng.removedNames())

	if err := m.Release(t.Context(), id, Cred{Token: token}); err != nil {
		t.Fatalf("release archived: %v", err)
	}
	if ckExists(t, m, ck) {
		t.Error("release orphaned the archive checkpoint in the store")
	}
	if got := len(eng.removedNames()); got != removesBefore {
		t.Errorf("release ran Remove %d extra times on a VM-less archived claim", got-removesBefore)
	}
	if _, ok := m.claim(id, token); ok {
		t.Error("released claim still present")
	}
}

// TestIdleOnceSkipsArchived guards the fix where the idle sweep tried to
// hibernate an archived (VM-less) claim, corrupting its record.
func TestIdleOnceSkipsArchived(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	hibBefore := eng.hibernateCount()
	backdate(m, sb, time.Hour)

	m.idleOnce(t.Context())
	waitFor(t, func() bool { return !m.idleSweep.Load() })
	if got := eng.hibernateCount(); got != hibBefore {
		t.Errorf("idle sweep hibernated an archived claim: %d→%d", hibBefore, got)
	}
	if archivedCount(m) != 1 {
		t.Error("idle sweep disturbed the archived claim")
	}
}

// TestDeleteCheckpointCannotBrickArchive guards the fix where the archive
// image was listable and deletable through the ordinary checkpoint API,
// letting a tenant permanently strand its own archived sandbox.
func TestDeleteCheckpointCannotBrickArchive(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk

	// The archive image is lifecycle-internal — it must not surface as a user
	// checkpoint, or be deletable as one.
	if ckpts, err := m.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 0 {
		t.Fatalf("Checkpoints listed %d records, want the archive image hidden (%v)", len(ckpts), err)
	}
	if err := m.DeleteCheckpoint(t.Context(), ck, "", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Fatalf("DeleteCheckpoint(archive ck) = %v, want ErrUnknownCheckpoint", err)
	}
	if !ckExists(t, m, ck) {
		t.Fatal("DeleteCheckpoint removed the archive image backing a live sandbox")
	}
	if _, err := m.WakeAgentSocket(t.Context(), id, token); err != nil {
		t.Fatalf("wake after delete attempt: %v", err)
	}
}

// TestArchivePersistFailureRollsBack guards the durability fix where archive
// destroyed the local VM+snapshot before its transition reached disk: a failed
// persist must roll the record back to hibernated and keep the local backing.
func TestArchivePersistFailureRollsBack(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	snap, vm := sb.HibernateSnap, sb.VMName
	breakStore(t, m)

	if err := m.archive(t.Context(), sb); err == nil {
		t.Fatal("archive succeeded despite a persist failure")
	}
	m.mu.Lock()
	ck, gotSnap, gotVM := sb.ArchiveCk, sb.HibernateSnap, sb.VMName
	m.mu.Unlock()
	if ck != "" || gotSnap != snap || gotVM != vm {
		t.Errorf("not rolled back: ArchiveCk=%q snap=%q vm=%q, want hibernated %q/%q", ck, gotSnap, gotVM, snap, vm)
	}
	if eng.removed(vm) {
		t.Error("archive destroyed the VM after a failed persist")
	}
	if ckpts, err := m.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 0 {
		t.Errorf("orphaned archive ck not cleaned up: %d records (%v)", len(ckpts), err)
	}
	healStore(t, m)
}

// TestReleaseArchivedRollsBackOnPersistFailure guards the durability fix where
// a release whose removal did not persist must roll back — the claim survives
// and its ck stays pinned, so a restart still wakes it.
func TestReleaseArchivedRollsBackOnPersistFailure(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	breakStore(t, m)

	if err := m.Release(t.Context(), id, Cred{Token: token}); err == nil {
		t.Fatal("release succeeded despite a persist failure")
	}
	if _, ok := m.claim(id, token); !ok {
		t.Error("release dropped the claim despite a failed persist")
	}
	if !pinnedHidden(t, m, ck) {
		t.Error("kept ck is not pinned after a failed release")
	}
	healStore(t, m)
}

// TestWakeArchivedRollsBackOnPersistFailure guards the durability fix where a
// wake whose cleared-ArchiveCk record did not persist must roll back to
// archived, keeping the ck pinned.
func TestWakeArchivedRollsBackOnPersistFailure(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	breakStore(t, m)

	if _, err := m.WakeAgentSocket(t.Context(), id, token); err == nil {
		t.Fatal("wake succeeded despite a persist failure")
	}
	m.mu.Lock()
	archived := sb.ArchiveCk == ck && sb.VMName == ""
	m.mu.Unlock()
	if !archived {
		t.Error("wake did not roll the record back to archived on a failed persist")
	}
	if !pinnedHidden(t, m, ck) {
		t.Error("kept ck is not pinned after a failed wake")
	}
	healStore(t, m)
}

// TestReapPurgeRollsBackOnPersistFailure guards the same rollback on the reaper:
// a retention purge that does not persist keeps the claim and its pinned ck.
func TestReapPurgeRollsBackOnPersistFailure(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id := sb.ID
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second) // retention elapsed
	m.mu.Unlock()
	breakStore(t, m)

	m.reapOnce(t.Context())
	m.mu.Lock()
	_, present := m.claimed[id]
	m.mu.Unlock()
	if !present {
		t.Error("reap dropped the archived claim despite a failed persist")
	}
	if !pinnedHidden(t, m, ck) {
		t.Error("kept ck is not pinned after a failed reap purge")
	}
	healStore(t, m)
}

// TestArchiveOnceSkipsInFlight guards the in-flight dedup: a sandbox already
// being archived (marked by a racing reap) must not be exported twice.
func TestArchiveOnceSkipsInFlight(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	backdate(m, sb, time.Hour)

	m.mu.Lock()
	m.archiving[sb.ID] = struct{}{} // a reap-triggered archive is already exporting it
	m.mu.Unlock()
	exportsBefore := eng.exportCount()

	m.archiveOnce(t.Context())
	waitFor(t, func() bool { return !m.archiveSweep.Load() })
	if got := eng.exportCount(); got != exportsBefore {
		t.Errorf("archiveOnce double-exported an in-flight sandbox: %d→%d", exportsBefore, got)
	}
	if archivedCount(m) != 0 {
		t.Error("archiveOnce archived a sandbox already being archived elsewhere")
	}
}

// TestArchiveWakeEvictsRecLock proves the archive->wake cycle leaves no
// recLocks residue: wakeArchived recLocks the consumed ck, so it must evict it
// after the delete or a repeatedly idle->archive->wake workload leaks a lock
// per cycle.
func TestArchiveWakeEvictsRecLock(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	countLocks := func() int {
		n := 0
		m.recLocks.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	base := countLocks()
	if err := m.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake archived: %v", err)
	}
	if got := countLocks(); got != base {
		t.Errorf("recLocks grew %d->%d over an archive+wake cycle, want no growth", base, got)
	}
}

// stallingStore parks Publish after the record becomes visible, exposing the
// window between an archive's publish and its ArchiveCk commit.
type stallingStore struct {
	store.Store
	published chan string
	release   chan struct{}
}

func (s *stallingStore) Publish(ctx context.Context, staging, id string) error {
	if err := s.Store.Publish(ctx, staging, id); err != nil {
		return err
	}
	s.published <- id
	<-s.release
	return nil
}

// breakStore points the claim store at a path whose parent is missing, so the
// next save fails — the fault used to exercise the persist-failure paths.
func breakStore(t *testing.T, m *Manager) {
	t.Helper()
	m.store.path = filepath.Join(t.TempDir(), "gone", "claims.json")
}

// healStore repairs breakStore's path and waits for the detached recommit to
// converge, so its retry loop does not outlive the test.
func healStore(t *testing.T, m *Manager) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
		t.Fatalf("heal store: %v", err)
	}
	waitFor(t, m.store.synced)
}

func archivedCount(m *Manager) int {
	_, g := m.Info()
	return g.Archived
}

// archivePool is the pool shape the archive tests share: idle→hibernate at 1s,
// archive at 2s, a long retention window (overridden per test).
func archivePool(delete int) config.PoolSpec {
	return config.PoolSpec{
		PoolKey:                   testKey,
		Warm:                      1,
		IdleHibernateSeconds:      1,
		ArchiveAfterSeconds:       2,
		ArchiveDeleteAfterSeconds: delete,
	}
}

// mustArchive drives a fresh claim to the archived state deterministically
// (hibernate then archive directly), bypassing the sweep thresholds so a test
// can start from a known archived record.
func mustArchive(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if err := m.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
}

func ckExists(t *testing.T, m *Manager, ck string) bool {
	t.Helper()
	_, _, release, err := m.ckpts.Fetch(t.Context(), ck)
	if err != nil {
		return false
	}
	release()
	return true
}

// pinnedHidden reports whether the store holds exactly one checkpoint and it is
// hidden from listings — i.e. a live claim's ArchiveCk still pins it.
func pinnedHidden(t *testing.T, m *Manager, ck string) bool {
	t.Helper()
	ckpts, err := m.Checkpoints(t.Context(), "")
	return err == nil && len(ckpts) == 0 && ckExists(t, m, ck)
}
