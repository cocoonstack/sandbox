package pool

import (
	"errors"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestFailoverDisabledKeepsReplicaAndRefusesLiveWriter(t *testing.T) {
	for _, pending := range []string{"published", "pending"} {
		t.Run(pending, func(t *testing.T) {
			m, c := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			if pending == "pending" {
				m.archives = &hookRegistry{Registry: c.reg, failPut: true}
				if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
					t.Fatal(err)
				}
			} else {
				replicateNow(t, m, sb)
			}
			replica, intent := sb.ReplicaCk, sb.ReplicaPending
			restarted := newManagerWithoutRegistry(t, m, c)
			if err := restarted.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			restored, ok := restarted.byID(sb.ID)
			if !ok || restored.ReplicaCk != replica || restored.ReplicaPending != intent {
				t.Fatalf("startup without a registry changed replica fields: ok=%t replica=%q pending=%q", ok, restored.ReplicaCk, restored.ReplicaPending)
			}
			for _, ck := range []string{replica, intent} {
				if ck != "" && (!ckExists(t, restarted, ck) || restarted.deleteMarked(ck)) {
					t.Fatalf("startup without a registry queued or deleted checkpoint %s", ck)
				}
			}
			want := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
			view, err := restarted.SetVolumes(t.Context(), sb.ID, "", want)
			if !errors.Is(err, ErrReplicaStranded) || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
				t.Fatalf("live writer on a stranded replica: err=%v volumes=%v want=%v", err, view.Volumes, view.VolumesWant)
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
