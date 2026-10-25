package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type strandedState struct {
	name  string
	setup func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string
	// wake: the fixture leaves the claim hibernated.
	wake bool
	// lifecycle: also check that release and expiry keep the replica checkpoint.
	lifecycle bool
}

var strandedStates = []strandedState{
	{name: "published replica", lifecycle: true, setup: func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) string {
		replicateNow(t, m, sb)
		return sb.ReplicaCk
	}},
	{name: "pending replica", lifecycle: true, setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
		m.archives = &hookRegistry{Registry: c.reg, failPut: true}
		if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
			t.Fatal(err)
		}
		return sb.ReplicaPending
	}},
	{name: "archive intent", wake: true, setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
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

func restartStranded(t *testing.T, state strandedState) (*Manager, *types.Sandbox, string) {
	t.Helper()
	m, c := newReplicaVolumeManager(t)
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

func requireStrandedWriter(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	want := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	view, err := m.SetVolumes(t.Context(), sb.ID, "", want)
	if !errors.Is(err, ErrReplicaStranded) || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
		t.Fatalf("live writer on stranded failover state: err=%v volumes=%v want=%v", err, view.Volumes, view.VolumesWant)
	}
}

func TestFailoverDisabledKeepsStateAndRefusesLiveWriter(t *testing.T) {
	for _, state := range strandedStates {
		t.Run(state.name, func(t *testing.T) {
			m, sb, _ := restartStranded(t, state)
			if state.wake {
				if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
					t.Fatal(err)
				}
			}
			requireStrandedWriter(t, m, sb)
		})
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
		if !state.lifecycle {
			continue
		}
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
