package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestReconcileRetiresReplicasWhenFailoverDisabled(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			m, c := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			if pending {
				m.archives = &hookRegistry{Registry: c.reg, failPut: true}
				if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
					t.Fatal(err)
				}
			} else {
				replicateNow(t, m, sb)
			}
			ck := sb.ReplicaCk
			if pending {
				ck = sb.ReplicaPending
			}
			drainClaimRow(t, c, sb.ID)
			restarted := newManagerWithoutRegistry(t, m, c)
			if err := restarted.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			restored, ok := restarted.byID(sb.ID)
			if !ok {
				t.Fatal("disabling failover lost the running claim")
			}
			if restored.HasReplica() || restored.ArchiveEpoch != 0 {
				t.Errorf("disabled failover retained replica fields: replica=%q pending=%q epoch=%d", restored.ReplicaCk, restored.ReplicaPending, restored.ArchiveEpoch)
			}
			want := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
			for range 3 {
				restarted.replicateOnce(t.Context())
			}
			if view, applyErr := restarted.SetVolumes(t.Context(), sb.ID, "", want); applyErr != nil || len(view.Volumes) != 1 {
				t.Errorf("disabled failover left live writer permanently blocked: %+v, %v", view, applyErr)
			}
			if ckExists(t, restarted, ck) && !restarted.deleteMarked(ck) {
				t.Error("disabled failover left the retired checkpoint without a cleanup marker")
			}
		})
	}
}

func TestDisabledReplicaCleanupSurvivesFailedStartupRewrite(t *testing.T) {
	for _, renamed := range []bool{false, true} {
		t.Run(fmt.Sprintf("renamed=%t", renamed), func(t *testing.T) {
			m, c := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			replicateNow(t, m, sb)
			ck := sb.ReplicaCk
			drainClaimRow(t, c, sb.ID)
			restarted := newManagerWithoutRegistry(t, m, c)
			restarted.store.replaceFile = func(raw []byte) error {
				if renamed {
					if err := restarted.store.replace(raw); err != nil {
						return err
					}
				}
				return errors.New("injected startup replacement failure")
			}
			if err := restarted.Reconcile(t.Context()); err == nil {
				t.Fatal("startup replacement unexpectedly succeeded")
			}
			if !restarted.deleteMarked(ck) || !ckExists(t, restarted, ck) {
				t.Fatal("failed startup lost a checkpoint or its cleanup intent")
			}
			persisted, err := newClaimStore(m.dataDir, false).load()
			if err != nil || persisted[sb.ID] == nil || persisted[sb.ID].HasReplica() == renamed {
				t.Fatalf("startup failure did not reach the expected persisted state: %+v, %v", persisted, err)
			}
			recovered := newManagerWithoutRegistry(t, restarted, c)
			if err = recovered.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if restored, ok := recovered.byID(sb.ID); !ok || restored.HasReplica() || ckExists(t, recovered, ck) || recovered.deleteMarked(ck) {
				t.Fatal("next startup did not retain the claim and finish queued cleanup")
			}
		})
	}
}

func TestDisabledReplicaCleanupRequiresDurableMarker(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	ck := sb.ReplicaCk
	drainClaimRow(t, c, sb.ID)
	markerDir := filepath.Join(m.dataDir, archiveDeleteDir)
	if err := os.WriteFile(markerDir, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := newManagerWithoutRegistry(t, m, c)
	if err := restarted.Reconcile(t.Context()); err == nil {
		t.Fatal("startup retired a replica without a durable cleanup marker")
	}
	persisted, err := newClaimStore(m.dataDir, false).load()
	if err != nil || persisted[sb.ID] == nil || persisted[sb.ID].ReplicaCk != ck || !ckExists(t, restarted, ck) {
		t.Fatalf("failed marker write changed the recoverable replica: %+v, %v", persisted, err)
	}
	if err = os.Remove(markerDir); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ckExists(t, restarted, ck) {
		t.Fatal("marker repair did not permit checkpoint cleanup")
	}
}

func TestDisabledRegistryKeepsArchivedClaimCheckpoint(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	ck, epoch := sb.ArchiveCk, sb.ArchiveEpoch
	drainClaimRow(t, c, sb.ID)
	restarted := newManagerWithoutRegistry(t, m, c)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored, ok := restarted.byID(sb.ID)
	if !ok || restored.ArchiveCk != ck || restored.ArchiveEpoch != epoch || !ckExists(t, restarted, ck) || restarted.deleteMarked(ck) {
		t.Fatal("replica retirement changed the archived claim or its checkpoint")
	}
}

func TestDisabledRegistryKeepsArchiveIntentCheckpoint(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	replica, epoch := sb.ReplicaCk, sb.ArchiveEpoch
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	m.archives = &hookRegistry{Registry: c.reg, failPut: true}
	if err := m.archive(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatalf("archive intent fixture: %v", err)
	}
	pending := sb.ArchivePending
	if pending == "" || sb.ArchiveCk != "" || sb.ReplicaCk != replica || sb.ArchiveEpoch != epoch {
		t.Fatal("failed archive did not retain its checkpoint intent and replica")
	}
	drainClaimRow(t, c, sb.ID)
	restarted := newManagerWithoutRegistry(t, m, c)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored, ok := restarted.byID(sb.ID)
	if !ok || restored.ArchivePending != pending || restored.ReplicaCk != replica || restored.ArchiveEpoch != epoch {
		t.Fatal("replica retirement changed an in-progress archive")
	}
	for _, ck := range []string{pending, replica} {
		if !ckExists(t, restarted, ck) || restarted.deleteMarked(ck) {
			t.Fatal("replica retirement queued or deleted an archive dependency")
		}
	}
}

func TestReplicaFailedClearQueuesCheckpointCleanup(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	ck := sb.ReplicaCk
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected replica clear failure") }
	sb.Transition.Lock()
	err := m.dropReplica(t.Context(), sb)
	sb.Transition.Unlock()
	if err == nil || sb.HasReplica() {
		t.Fatalf("expected an unacknowledged memory clear: replica=%t, %v", sb.HasReplica(), err)
	}
	if !m.deleteMarked(ck) {
		t.Error("failed replica clear did not queue its checkpoint for cleanup")
	}
	m.retryArchiveDeletes(t.Context())
	if !ckExists(t, m, ck) {
		t.Fatal("queued cleanup deleted a checkpoint still referenced by claims.json")
	}
	m.store.replaceFile = nil
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	m.retryArchiveDeletes(t.Context())
	if ckExists(t, m, ck) || m.deleteMarked(ck) {
		t.Fatal("checkpoint cleanup did not finish after its reference retired")
	}
}

func newManagerWithoutRegistry(t *testing.T, old *Manager, c *failoverCluster) *Manager {
	t.Helper()
	cfg := &config.Config{
		APIToken: "root", DataDir: old.dataDir, CheckpointDir: c.ckDir,
		Pools: []config.PoolSpec{{PoolKey: testKey}}, Volumes: old.cfg.Volumes,
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

func drainClaimRow(t *testing.T, c *failoverCluster, id string) {
	t.Helper()
	rec, ok, err := c.reg.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		if err = c.reg.Delete(t.Context(), id, rec.Epoch); err != nil {
			t.Fatal(err)
		}
	}
}
