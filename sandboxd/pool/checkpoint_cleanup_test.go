package pool

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestCheckpointCleanupWaitsForTheClaimWrite(t *testing.T) {
	replicated := func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) string {
		replicateNow(t, m, sb)
		return sb.ReplicaCk
	}
	keepsTombstone := func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) {
		rec, ok, err := c.reg.Get(t.Context(), sb.ID)
		if err != nil || !ok || !rec.Consumed {
			t.Fatalf("retirement lost its tombstone: %+v, %t, %v", rec, ok, err)
		}
		m.reconcileOwnRow(t.Context(), rec, time.Now())
		if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok {
			t.Fatalf("sweep erased an unacknowledged retirement: %t, %v", ok, err)
		}
	}
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string
		act   func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox)
	}{
		{
			name:  "replica retirement",
			setup: replicated,
			act: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) {
				sb.Transition.Lock()
				err := m.dropReplica(t.Context(), sb)
				sb.Transition.Unlock()
				if err == nil || sb.HasReplica() {
					t.Fatalf("dropReplica: replica=%t, %v; want a memory clear and the write error", sb.HasReplica(), err)
				}
				keepsTombstone(t, m, c, sb)
			},
		},
		{
			name:  "retirement of a released claim",
			setup: replicated,
			act: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) {
				m.retireReplica(t.Context(), sb)
				m.mu.Lock()
				delete(m.claimed, sb.ID)
				m.store.del(sb.ID)
				m.mu.Unlock()
				keepsTombstone(t, m, c, sb)
			},
		},
		{
			name: "unpublished replica intent",
			setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
				m.archives = &hookRegistry{Registry: c.reg, failPut: true}
				if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
					t.Fatal(err)
				}
				m.archives = c.reg
				return sb.ReplicaPending
			},
			act: func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) {
				m.retireReplica(t.Context(), sb)
				if sb.ReplicaPending != "" {
					t.Fatal("retirement kept the intent in memory")
				}
			},
		},
		{
			name: "settled replica intent",
			setup: func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
				replicateNow(t, m, sb)
				previous := sb.ReplicaCk
				m.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
				if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
					t.Fatal(err)
				}
				m.archives = c.reg
				return previous
			},
			act: func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) {
				pending := sb.ReplicaPending
				sb.Transition.Lock()
				err := m.settleReplicaIntent(t.Context(), sb)
				sb.Transition.Unlock()
				if err == nil || sb.ReplicaPending != "" || sb.ReplicaCk != pending {
					t.Fatalf("settle: %+v, %v; want the intent settled in memory and the write error", sb, err)
				}
			},
		},
	} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart=%t", tc.name, restart), func(t *testing.T) {
				c := newReplicaCluster(t)
				m := c.node(t, "a", t.TempDir(), keepAnHour)
				sb := mustClaim(t, m, testKey)
				ck := tc.setup(t, m, c, sb)
				restoreWrites := failClaimWrites(t, m.store)
				tc.act(t, m, c, sb)
				m.retryArchiveDeletes(t.Context())
				if !ckExists(t, m, ck) || !m.deleteMarked(ck) {
					t.Fatal("cleanup dropped the checkpoint or its delete marker before the claim write landed")
				}
				restoreWrites()
				if restart {
					m = c.restart(t, m)
					if err := m.Reconcile(t.Context()); err != nil {
						t.Fatal(err)
					}
					var ok bool
					if sb, ok = m.byID(sb.ID); !ok {
						t.Fatal("restart dropped the claim")
					}
					m.retireReplica(t.Context(), sb)
				} else if err := m.store.commit(m.store.mark()); err != nil {
					t.Fatal(err)
				}
				other := mustClaim(t, m, testKey)
				m.mu.Lock()
				other.LeaseSeconds++
				m.store.set(other)
				m.mu.Unlock()
				m.retryArchiveDeletes(t.Context())
				if ckExists(t, m, ck) || m.deleteMarked(ck) {
					t.Fatal("cleanup did not finish once the claim write landed")
				}
				for _, live := range checkpointRefs(sb) {
					if live != "" && !ckExists(t, m, live) {
						t.Errorf("cleanup deleted live checkpoint %s", live)
					}
				}
				m.sweepOrphanArchives(t.Context())
				if rec, ok, _ := c.reg.Get(t.Context(), sb.ID); ok && rec.Consumed {
					t.Error("sweep kept the tombstone after the claim write landed")
				}
			})
		}
	}
}

func TestFailoverDisabledExitKeepsReplicaCheckpoint(t *testing.T) {
	states := []struct {
		name  string
		setup func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string
	}{
		{"published replica", func(t *testing.T, m *Manager, _ *failoverCluster, sb *types.Sandbox) string {
			replicateNow(t, m, sb)
			return sb.ReplicaCk
		}},
		{"pending replica", func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
			m.archives = &hookRegistry{Registry: c.reg, failPut: true}
			if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
				t.Fatal(err)
			}
			return sb.ReplicaPending
		}},
		{"archive intent", func(t *testing.T, m *Manager, c *failoverCluster, sb *types.Sandbox) string {
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
	for _, state := range states {
		for _, exit := range exits {
			t.Run(state.name+"/"+exit.name, func(t *testing.T) {
				c := newReplicaCluster(t)
				old := c.node(t, "a", t.TempDir(), keepAnHour)
				sb := mustClaim(t, old, testKey)
				ck := state.setup(t, old, c, sb)
				cfg := &config.Config{
					APIToken: "root", DataDir: old.dataDir, CheckpointDir: c.ckDir,
					Pools: []config.PoolSpec{{PoolKey: testKey}},
				}
				m, err := NewManager(t.Context(), cfg, old.eng, testSecrets(t), testBackends(t, cfg))
				if err != nil {
					t.Fatal(err)
				}
				if err = m.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				restored, ok := m.byID(sb.ID)
				if !ok || checkpointRefs(restored) != checkpointRefs(sb) {
					t.Fatalf("startup without a registry changed the claim: %+v", restored)
				}
				if !ckExists(t, m, ck) || m.deleteMarked(ck) {
					t.Fatalf("startup without a registry queued or deleted checkpoint %s", ck)
				}
				exit.run(t, m, restored)
				if _, held := m.byID(sb.ID); held || !ckExists(t, m, ck) {
					t.Fatalf("%s without a registry: held=%t checkpoint kept=%t", exit.name, held, ckExists(t, m, ck))
				}
			})
		}
	}
}
