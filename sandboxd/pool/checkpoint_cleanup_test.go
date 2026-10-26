package pool

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var strandedStates = []strandedState{
	{name: "published replica", setup: func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) string {
		replicateNow(t, m, sb)
		return sb.ReplicaCk
	}},
	{name: "pending replica", setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
		m.archives = &hookRegistry{Registry: c.reg, failPut: true}
		if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
			t.Fatal(err)
		}
		return sb.ReplicaPending
	}},
	{name: "archive intent", setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatal(err)
		}
		m.archives = &hookRegistry{Registry: c.reg, failPut: true}
		if err := m.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
			t.Fatal(err)
		}
		return sb.ArchivePending
	}},
}

func TestReplicaRetirementKeepsTombstoneUntilClaimCommit(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			c := newReplicaCluster(t)
			m := c.node(t, "a", t.TempDir(), keepAnHour)
			sb := mustClaim(t, m, testKey)
			replicateNow(t, m, sb)
			ck := sb.ReplicaCk
			m.store.recommitting.Store(true)
			restoreWrites := failClaimWrites(t, m.store)
			sb.Transition.Lock()
			err := m.dropReplica(t.Context(), sb)
			sb.Transition.Unlock()
			if err == nil || sb.HasReplica() {
				t.Fatalf("expected an unacknowledged memory clear: replica=%t, %v", sb.HasReplica(), err)
			}
			rec, ok, err := c.reg.Get(t.Context(), sb.ID)
			if err != nil || !ok || !rec.Consumed || !ckExists(t, m, ck) {
				t.Fatalf("failed commit lost recovery tombstone/checkpoint: %+v, exists=%t, err=%v", rec, ok, err)
			}
			if !m.deleteMarked(ck) {
				t.Error("failed replica clear did not queue its checkpoint for cleanup")
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if rec, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok || !rec.Consumed {
				t.Fatalf("sweep erased unacknowledged retirement: %+v, exists=%t, err=%v", rec, ok, err)
			}
			m.retryArchiveDeletes(t.Context())
			if !ckExists(t, m, ck) || !m.deleteMarked(ck) {
				t.Fatal("cleanup discarded unacknowledged checkpoint or retry marker")
			}
			restoreWrites()
			if restart {
				m = c.restart(t, m)
				if err = m.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				restored, present := m.byID(sb.ID)
				if !present {
					t.Fatal("restart dropped a claim after failed retirement commit")
				}
				m.retireReplica(t.Context(), restored)
				if restored.HasReplica() {
					t.Fatal("restart did not finish retirement")
				}
			} else if err = m.store.commit(m.store.mark()); err != nil {
				t.Fatal(err)
			}
			m.retryArchiveDeletes(t.Context())
			if ckExists(t, m, ck) || m.deleteMarked(ck) {
				t.Fatal("checkpoint cleanup did not finish after its reference retired")
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || ok {
				t.Fatalf("acknowledged retirement retained tombstone: %t, %v", ok, err)
			}
		})
	}
}

func TestReplicaUnpublishedIntentSurvivesFailedClearAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			c := newReplicaCluster(t)
			m := c.node(t, "a", t.TempDir(), keepAnHour)
			sb := mustClaim(t, m, testKey)
			m.archives = &hookRegistry{Registry: c.reg, failPut: true}
			if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
				t.Fatal(err)
			}
			pending := sb.ReplicaPending
			if pending == "" || !ckExists(t, m, pending) {
				t.Fatal("missing unpublished checkpoint")
			}
			m.archives = c.reg
			m.store.recommitting.Store(true)
			restoreWrites := failClaimWrites(t, m.store)
			m.retireReplica(t.Context(), sb)
			persisted, err := newClaimStore(m.dataDir, false).load()
			if err != nil || persisted[sb.ID].ReplicaPending != pending {
				t.Fatalf("failure did not retain old disk state: %+v, %v", persisted, err)
			}
			if sb.ReplicaPending != "" || m.store.synced() {
				t.Fatal("expected memory clear before snapshot acknowledgment")
			}
			if deleted, err := m.deleteArchiveCk(t.Context(), pending); err != nil || deleted {
				t.Fatalf("orphan delete ignored unacknowledged claim: deleted=%t, %v", deleted, err)
			}
			m.retryArchiveDeletes(t.Context())
			if !ckExists(t, m, pending) || !m.deleteMarked(pending) {
				t.Fatal("cleanup discarded pending checkpoint or retry marker")
			}
			restoreWrites()
			if restart {
				m = c.restart(t, m)
				if err := m.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				restored, ok := m.byID(sb.ID)
				if !ok {
					t.Fatal("restart mistook a failed pending clear for adoption and discarded the claim")
				}
				m.retireReplica(t.Context(), restored)
				if restored.HasReplica() {
					t.Fatal("restart failed to retire unpublished replica")
				}
			} else if err := m.store.commit(m.store.mark()); err != nil {
				t.Fatal(err)
			}
			other := mustClaim(t, m, testKey)
			m.mu.Lock()
			other.LeaseSeconds++
			m.store.set(other)
			m.mu.Unlock()
			m.retryArchiveDeletes(t.Context())
			if ckExists(t, m, pending) || m.deleteMarked(pending) {
				t.Fatal("acknowledged cleanup leaked checkpoint or retry marker")
			}
		})
	}
}

func TestReplicaSettledCheckpointCleanupRetriesAfterFailedCommit(t *testing.T) {
	c := newReplicaCluster(t)
	m := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	previous := sb.ReplicaCk
	m.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatal(err)
	}
	pending := sb.ReplicaPending
	m.archives = c.reg
	m.store.recommitting.Store(true)
	restoreWrites := failClaimWrites(t, m.store)
	sb.Transition.Lock()
	err := m.settleReplicaIntent(t.Context(), sb)
	sb.Transition.Unlock()
	if err == nil || sb.ReplicaPending != "" || sb.ReplicaCk != pending || m.store.synced() {
		t.Fatalf("settle state: %+v, %v", sb, err)
	}
	if !ckExists(t, m, previous) || !m.deleteMarked(previous) {
		t.Fatal("failed settle lost old checkpoint or its queued cleanup")
	}
	m.retryArchiveDeletes(t.Context())
	if !ckExists(t, m, previous) {
		t.Fatal("old checkpoint deleted before clear acknowledgment")
	}
	restoreWrites()
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	m.retryArchiveDeletes(t.Context())
	if ckExists(t, m, previous) || m.deleteMarked(previous) || !ckExists(t, m, pending) {
		t.Fatal("acknowledged settle failed to clean old checkpoint or removed current checkpoint")
	}
}

func TestReplicaConsumedRowPurgeWaitsForSnapshot(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(fmt.Sprintf("claimed=%t", claimed), func(t *testing.T) {
			c := newReplicaCluster(t)
			m := c.node(t, "a", t.TempDir(), keepAnHour)
			sb := mustClaim(t, m, testKey)
			replicateNow(t, m, sb)
			m.store.recommitting.Store(true)
			restoreWrites := failClaimWrites(t, m.store)
			m.retireReplica(t.Context(), sb)
			if !claimed {
				m.mu.Lock()
				delete(m.claimed, sb.ID)
				m.store.del(sb.ID)
				m.mu.Unlock()
			}
			rec, ok, err := c.reg.Get(t.Context(), sb.ID)
			if err != nil || !ok || !rec.Consumed {
				t.Fatalf("setup: %+v, %t, %v", rec, ok, err)
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok {
				t.Fatalf("sweep dropped unacknowledged row: %t, %v", ok, err)
			}
			restoreWrites()
			if err = m.store.commit(m.store.mark()); err != nil {
				t.Fatal(err)
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || ok {
				t.Fatalf("sweep retained acknowledged row: %t, %v", ok, err)
			}
		})
	}
}

func TestArchiveCheckpointCleanupIgnoresUnrelatedClaimWrites(t *testing.T) {
	c := newReplicaCluster(t)
	m := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, m, testKey)
	checkpoint, _, err := m.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "orphan", "", true)
	if err != nil {
		t.Fatal(err)
	}
	ck := checkpoint.ID
	other := mustClaim(t, m, testKey)
	m.mu.Lock()
	other.LeaseSeconds++
	m.store.set(other)
	m.mu.Unlock()
	if m.store.synced() {
		t.Fatal("expected an unrelated unacknowledged claim write")
	}
	if deleted, err := m.deleteArchiveCk(t.Context(), ck); err != nil || !deleted {
		t.Fatalf("unrelated claim delayed checkpoint cleanup: deleted=%t, %v", deleted, err)
	}
	if ckExists(t, m, ck) || m.deleteMarked(ck) {
		t.Fatal("unreferenced checkpoint or delete marker remained")
	}
}

func TestFailoverDisabledExitKeepsReplicaCheckpoint(t *testing.T) {
	exits := []struct {
		name string
		run  func(t *testing.T, m *Manager, sb *types.Sandbox)
	}{
		{"release", func(t *testing.T, m *Manager, sb *types.Sandbox) {
			if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
		}},
		{"expire", func(t *testing.T, m *Manager, sb *types.Sandbox) {
			m.mu.Lock()
			sb.Deadline = time.Now().Add(-time.Minute)
			m.mu.Unlock()
			m.reapOnce(t.Context())
			waitFor(t, func() bool { return m.Counters().Reaps == 1 })
		}},
	}
	for _, state := range strandedStates {
		for _, exit := range exits {
			t.Run(state.name+"/"+exit.name, func(t *testing.T) {
				m, sb, ck := restartStranded(t, state)
				exit.run(t, m, sb)
				if _, held := m.byID(sb.ID); held || !ckExists(t, m, ck) {
					t.Fatalf("%s without a registry: held=%t checkpoint kept=%t", exit.name, held, ckExists(t, m, ck))
				}
			})
		}
	}
}

type strandedState struct {
	name  string
	setup func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string
}

func restartStranded(t *testing.T, state strandedState) (*Manager, *types.Sandbox, string) {
	t.Helper()
	c := newReplicaCluster(t)
	m := c.node(t, "a", t.TempDir(), keepAnHour)
	sb := mustClaim(t, m, testKey)
	ck := state.setup(t, m, c, sb)
	if ck == "" {
		t.Fatal("fixture left no failover checkpoint")
	}
	restarted := newManagerWithoutRegistry(t, m, c)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("startup without a registry lost the claim")
	}
	if restored.ReplicaCk != sb.ReplicaCk || restored.ReplicaPending != sb.ReplicaPending || restored.ArchivePending != sb.ArchivePending {
		t.Fatalf("startup without a registry changed failover fields: replica=%q pending=%q archive=%q", restored.ReplicaCk, restored.ReplicaPending, restored.ArchivePending)
	}
	if !ckExists(t, restarted, ck) || restarted.deleteMarked(ck) {
		t.Fatalf("startup without a registry queued or deleted checkpoint %s", ck)
	}
	return restarted, restored, ck
}

func newManagerWithoutRegistry(t *testing.T, old *Manager, c *failoverCluster) *Manager {
	t.Helper()
	cfg := &config.Config{
		APIToken: "root", DataDir: old.dataDir, CheckpointDir: c.ckDir,
		Pools: []config.PoolSpec{{PoolKey: testKey}},
	}
	m, err := NewManager(t.Context(), cfg, old.eng, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if m.archives != nil {
		t.Fatal("failover-disabled fixture unexpectedly has a registry")
	}
	return m
}
