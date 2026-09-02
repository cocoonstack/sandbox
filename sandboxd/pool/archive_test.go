package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	testArchiveRelease = "release"
	testArchiveReap    = "reap"
)

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

func TestArchiveLifecycleRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestArchiveTTLSweepSparesLiveCheckpoint(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	m.ckptTTL = time.Nanosecond
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

func TestArchiveRetentionPurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(3600))
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		mustArchive(t, m, sb)
		ck := sb.ArchiveCk

		m.mu.Lock()
		sb.Deadline = time.Now().Add(-time.Second)
		m.mu.Unlock()

		m.reapOnce(t.Context())
		waitFor(t, func() bool { return archivedCount(m) == 0 })

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
	})
}

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

func TestArchiveDeleteRetryAfterRestart(t *testing.T) {
	for _, action := range []string{testArchiveRelease, testArchiveReap} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				dataDir := t.TempDir()
				m := newTestManagerAt(t, eng, dataDir, archivePool(3600))
				sb := mustClaim(t, m, testKey)
				id, token := sb.ID, sb.Token
				mustArchive(t, m, sb)
				ck := sb.ArchiveCk
				failed := &failingDeleteStore{Store: m.ckpts, attempts: make(chan string, 1)}
				m.ckpts = failed

				switch action {
				case testArchiveRelease:
					if err := m.Release(t.Context(), id, Cred{Token: token}); err != nil {
						t.Fatalf("release: %v", err)
					}
				case testArchiveReap:
					m.mu.Lock()
					sb.Deadline = time.Now().Add(-time.Second)
					m.mu.Unlock()
					m.reapOnce(t.Context())
					select {
					case <-failed.attempts:
					case <-time.After(3 * time.Second):
						t.Fatal("reap did not attempt archive deletion")
					}
					waitFor(t, func() bool {
						m.mu.Lock()
						defer m.mu.Unlock()
						_, pending := m.pendingCks[ck]
						return !pending
					})
				}
				if _, ok := m.claim(id, token); ok {
					t.Fatal("removed archive claim survived")
				}
				if !ckExists(t, m, ck) || !archiveCkMarked(m, ck) {
					t.Fatal("failed deletion did not retain the checkpoint and cleanup marker")
				}

				m2 := newTestManagerAt(t, eng, dataDir, archivePool(3600))
				if err := m2.Reconcile(t.Context()); err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
				if ckExists(t, m2, ck) || archiveCkMarked(m2, ck) {
					t.Fatal("restart did not finish archive deletion")
				}
				if _, ok := m2.claim(id, token); ok {
					t.Fatal("restart revived the removed claim")
				}
			})
		})
	}
}

func TestReconcileClearsLiveArchiveMarker(t *testing.T) {
	eng := newFakeEngine()
	dataDir := t.TempDir()
	m := newTestManagerAt(t, eng, dataDir, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	id, token, ck := sb.ID, sb.Token, sb.ArchiveCk
	if archiveCkMarked(m, ck) {
		t.Fatal("archive left a delete marker for its live checkpoint")
	}
	if err := m.markArchiveCk(ck); err != nil {
		t.Fatalf("seed live archive marker: %v", err)
	}

	m2 := newTestManagerAt(t, eng, dataDir, archivePool(3600))
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !ckExists(t, m2, ck) || archiveCkMarked(m2, ck) {
		t.Fatal("reconcile did not retain the live archive unmarked")
	}
	if _, err := m2.WakeAgentSocket(t.Context(), id, token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	if archiveCkMarked(m2, ck) {
		t.Fatal("wake left an archive delete marker")
	}
}

func TestArchiveWakeDeleteFailureRetries(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	base := m.ckpts
	failed := &failingDeleteStore{Store: base, attempts: make(chan string, 1)}
	m.ckpts = failed

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake: %v", err)
	}
	select {
	case got := <-failed.attempts:
		if got != ck {
			t.Fatalf("deleted checkpoint %s, want %s", got, ck)
		}
	default:
		t.Fatal("wake did not attempt archive deletion")
	}
	if !ckExists(t, m, ck) || !archiveCkMarked(m, ck) {
		t.Fatal("failed wake deletion did not retain the checkpoint and cleanup marker")
	}

	m.ckpts = base
	m.retryArchiveDeletes(t.Context())
	if ckExists(t, m, ck) || archiveCkMarked(m, ck) {
		t.Fatal("retry did not finish wake archive deletion")
	}
}

func TestArchiveWakeRequiresDeleteMarker(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk
	blocker := filepath.Join(m.dataDir, archiveDeleteDir)
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatalf("block marker dir: %v", err)
	}
	removesBefore := len(eng.removedNames())

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake succeeded with an unwritable marker dir")
	}
	m.mu.Lock()
	archived := m.claimed[sb.ID] == sb && sb.ArchiveCk == ck && sb.VMName == ""
	m.mu.Unlock()
	if !archived || !ckExists(t, m, ck) {
		t.Fatal("failed wake did not retain the archived claim and checkpoint")
	}
	claims, err := m.store.load()
	if err != nil || claims[sb.ID] == nil || claims[sb.ID].ArchiveCk != ck {
		t.Fatalf("failed wake changed the durable archive claim: %+v, %v", claims[sb.ID], err)
	}
	if got := len(eng.removedNames()); got != removesBefore+1 {
		t.Errorf("failed wake removed %d VMs, want 1", got-removesBefore)
	}

	if err := os.Remove(blocker); err != nil {
		t.Fatalf("unblock marker dir: %v", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake after heal: %v", err)
	}
	if ckExists(t, m, ck) || archiveCkMarked(m, ck) {
		t.Fatal("healed wake did not delete the checkpoint")
	}
}

func TestArchiveRemovalCommitPinsCheckpoint(t *testing.T) {
	for _, action := range []string{testArchiveRelease, testArchiveReap} {
		t.Run(action, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, archivePool(3600))
			sb := mustClaim(t, m, testKey)
			mustArchive(t, m, sb)
			id, token, ck := sb.ID, sb.Token, sb.ArchiveCk
			if action == testArchiveReap {
				m.mu.Lock()
				sb.Deadline = time.Now().Add(-time.Second)
				m.mu.Unlock()
			}

			m.store.writeMu.Lock()
			locked := true
			t.Cleanup(func() {
				if locked {
					m.store.writeMu.Unlock()
				}
			})
			done := make(chan error, 1)
			go func() {
				if action == testArchiveRelease {
					done <- m.Release(t.Context(), id, Cred{Token: token})
					return
				}
				m.reapOnce(t.Context())
				done <- nil
			}()
			waitFor(t, func() bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				_, claimed := m.claimed[id]
				_, pending := m.pendingCks[ck]
				return !claimed && pending
			})
			m.retryArchiveDeletes(t.Context())
			if !ckExists(t, m, ck) {
				t.Error("retry deleted the checkpoint before the claims commit")
			}
			m.store.path = filepath.Join(t.TempDir(), "gone", "claims.json")
			m.store.writeMu.Unlock()
			locked = false
			if err := <-done; action == testArchiveRelease && err == nil {
				t.Fatal("release succeeded despite a persist failure")
			}
			waitFor(t, func() bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				_, claimed := m.claimed[id]
				_, pending := m.pendingCks[ck]
				return claimed && !pending
			})
			m.retryArchiveDeletes(t.Context())
			if !ckExists(t, m, ck) {
				t.Error("retry deleted the checkpoint restored by rollback")
			}
			healStore(t, m)
			if _, err := m.WakeAgentSocket(t.Context(), id, token); err != nil {
				t.Fatalf("wake after rollback: %v", err)
			}
		})
	}
}

func TestArchiveRemovalRequiresDeleteMarker(t *testing.T) {
	for _, action := range []string{testArchiveRelease, testArchiveReap} {
		t.Run(action, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, archivePool(3600))
			sb := mustClaim(t, m, testKey)
			id, token := sb.ID, sb.Token
			mustArchive(t, m, sb)
			ck := sb.ArchiveCk
			blocker := filepath.Join(m.dataDir, archiveDeleteDir)
			if err := os.WriteFile(blocker, nil, 0o600); err != nil {
				t.Fatalf("block marker dir: %v", err)
			}

			switch action {
			case testArchiveRelease:
				if err := m.Release(t.Context(), id, Cred{Token: token}); err == nil {
					t.Fatal("release succeeded with an unwritable marker dir")
				}
			case testArchiveReap:
				m.mu.Lock()
				sb.Deadline = time.Now().Add(-time.Second)
				m.mu.Unlock()
				m.reapOnce(t.Context())
			}
			if _, ok := m.claim(id, token); !ok {
				t.Fatal("claim dropped despite an unmarkable delete intent")
			}
			if !pinnedHidden(t, m, ck) {
				t.Error("kept ck is not pinned after an aborted removal")
			}

			if err := os.Remove(blocker); err != nil {
				t.Fatalf("unblock marker dir: %v", err)
			}
			if err := m.Release(t.Context(), id, Cred{Token: token}); err != nil {
				t.Fatalf("release after heal: %v", err)
			}
			if ckExists(t, m, ck) || archiveCkMarked(m, ck) {
				t.Fatal("healed release did not delete the checkpoint")
			}
		})
	}
}

func TestArchiveDeleteRetryRechecksWakeRollback(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	id, token, ck := sb.ID, sb.Token, sb.ArchiveCk

	m.store.writeMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			m.store.writeMu.Unlock()
		}
	})
	woke := make(chan error, 1)
	go func() {
		_, err := m.WakeAgentSocket(t.Context(), id, token)
		woke <- err
	}()
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return sb.ArchiveCk == ""
	})
	if !archiveCkMarked(m, ck) {
		t.Fatal("wake dropped the journal reference before marking the checkpoint")
	}
	retried := make(chan struct{})
	go func() {
		m.retryArchiveDeletes(t.Context())
		close(retried)
	}()
	waitFor(t, func() bool {
		m.recLocksMu.Lock()
		defer m.recLocksMu.Unlock()
		return m.recRefs[ck] >= 2
	})
	m.store.path = filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.writeMu.Unlock()
	locked = false
	if err := <-woke; err == nil {
		t.Fatal("wake succeeded despite a persist failure")
	}
	<-retried
	m.mu.Lock()
	archived := sb.ArchiveCk == ck && sb.VMName == ""
	m.mu.Unlock()
	if !archived || !ckExists(t, m, ck) {
		t.Fatal("retry deleted the archive restored by wake rollback")
	}
	healStore(t, m)
	if _, err := m.WakeAgentSocket(t.Context(), id, token); err != nil {
		t.Fatalf("wake after rollback: %v", err)
	}
	if ckExists(t, m, ck) || archiveCkMarked(m, ck) {
		t.Fatal("wake after rollback did not delete the checkpoint")
	}
}

func TestIdleOnceSkipsArchived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestDeleteCheckpointCannotBrickArchive(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	mustArchive(t, m, sb)
	ck := sb.ArchiveCk

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

func TestArchivePersistFailureRollsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestReleaseArchivedRollsBackOnPersistFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestWakeArchivedRollsBackOnPersistFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestReapPurgeRollsBackOnPersistFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(3600))
		sb := mustClaim(t, m, testKey)
		id := sb.ID
		mustArchive(t, m, sb)
		ck := sb.ArchiveCk
		m.mu.Lock()
		sb.Deadline = time.Now().Add(-time.Second)
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
	})
}

func TestArchiveOnceSkipsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(3600))
		sb := mustClaim(t, m, testKey)
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("hibernate: %v", err)
		}
		backdate(m, sb, time.Hour)

		m.mu.Lock()
		m.archiving[sb.ID] = struct{}{}
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
	})
}

func TestArchiveWakeEvictsRecLock(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, archivePool(3600))
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	base := lockCount(m)
	if err := m.archive(t.Context(), sb); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake archived: %v", err)
	}
	if got := lockCount(m); got != base {
		t.Errorf("recLocks grew %d->%d over an archive+wake cycle, want no growth", base, got)
	}
}

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

type failingDeleteStore struct {
	store.Store
	attempts chan string
}

func (s *failingDeleteStore) Delete(_ context.Context, id string) error {
	select {
	case s.attempts <- id:
	default:
	}
	return errors.New("delete failed")
}

func breakStore(t *testing.T, m *Manager) {
	t.Helper()
	m.store.path = filepath.Join(t.TempDir(), "gone", "claims.json")
}

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

func archivePool(delete int) config.PoolSpec {
	return config.PoolSpec{
		PoolKey:                   testKey,
		Warm:                      1,
		IdleHibernateSeconds:      1,
		ArchiveAfterSeconds:       2,
		ArchiveDeleteAfterSeconds: delete,
	}
}

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
	_, _, _, release, err := m.ckpts.Fetch(t.Context(), ck) //nolint:dogsled // existence only needs Fetch success
	if err != nil {
		return false
	}
	release()
	return true
}

func archiveCkMarked(m *Manager, ck string) bool {
	_, err := os.Stat(filepath.Join(m.dataDir, archiveDeleteDir, ck))
	return err == nil
}

func pinnedHidden(t *testing.T, m *Manager, ck string) bool {
	t.Helper()
	ckpts, err := m.Checkpoints(t.Context(), "")
	return err == nil && len(ckpts) == 0 && ckExists(t, m, ck)
}
