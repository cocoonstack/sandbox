package pool

import (
	"errors"
	"fmt"
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
			cfg := &config.Config{
				APIToken: "root", DataDir: m.dataDir, CheckpointDir: c.ckDir,
				Pools:   []config.PoolSpec{{PoolKey: testKey}},
				Volumes: []config.VolumeSpec{{Name: "data", Path: m.catalog.entries["data"].disk.Path, Writable: true}},
			}
			restarted, err := NewManager(t.Context(), cfg, m.eng, testSecrets(t), testBackends(t, cfg))
			if err != nil {
				t.Fatal(err)
			}
			if restarted.archives != nil {
				t.Fatal("failover-disabled fixture unexpectedly has a registry")
			}
			if err = restarted.Reconcile(t.Context()); err != nil {
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
