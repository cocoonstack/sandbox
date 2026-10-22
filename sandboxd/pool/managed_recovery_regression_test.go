package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestManagedQuarantineRemovalSurvivesRestart(t *testing.T) {
	for _, syncClaims := range []bool{false, true} {
		t.Run(fmt.Sprint(syncClaims), func(t *testing.T) {
			cfg := managedConfig(t)
			cfg.SyncClaims = syncClaims
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			createManagedTest(t, m, "data")
			sb, claimErr := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			if claimErr != nil {
				t.Fatal(claimErr)
			}
			sb.Key.Net = types.NetEgress
			if err := m.store.commit(m.store.set(sb)); err != nil {
				t.Fatal(err)
			}
			e.stuckRemove.Store(true)
			if m.quarantineClaim(t.Context(), sb) {
				t.Fatal("ineffective removal reported success")
			}
			waitFor(t, m.store.synced)
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			m = openManagedTest(t, cfg, e)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, claimErr = m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
			requireVolumeStatus(t, claimErr, 409)
			e.stuckRemove.Store(false)
			m.retryRemovals(t.Context()).Wait()
			if _, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || present {
				t.Fatalf("quarantined VM survived removal retry after restart: present=%v err=%v", present, err)
			}
			requireManagedClaimable(t, m, "data")
		})
	}
}

func TestManagedOrdinaryFailedClaimRetainsOwnershipOnRestart(t *testing.T) {
	cfg := managedConfig(t)
	e := newManagedEngine()
	m := openManagedTest(t, cfg, e)
	createManagedTest(t, m, "data")
	sb, claimErr := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	sb.Failed = "ordinary failure requiring explicit release"
	if err := m.store.commit(m.store.set(sb)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = openManagedTest(t, cfg, e)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.retryRemovals(t.Context()).Wait()
	if _, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || !present {
		t.Fatalf("ordinary failed claim removed: %v %v", present, err)
	}
	_, claimErr = m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	requireVolumeStatus(t, claimErr, 409)
	if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedStoreRejectsSweepOwnedPathsBeforeCreatingFiles(t *testing.T) {
	for _, rel := range []string{"readers", "readers/managed", "fork-persistent", "fork-persistent/managed", "goldens", "goldens/managed"} {
		t.Run(rel, func(t *testing.T) {
			cfg := managedConfig(t)
			cfg.ManagedVolumeDir = filepath.Join(cfg.DataDir, rel)
			m, err := NewManager(t.Context(), cfg, newManagedEngine(), testSecrets(t), testBackends(t, cfg))
			if err == nil {
				m.Close()
				t.Fatal("accepted managed store inside startup cleanup namespace")
			}
			if _, err := os.Stat(cfg.ManagedVolumeDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe store modified before rejection: %v", err)
			}
		})
	}
}

func TestManagedStorePreservesBackingWhenCleanupAliasesDisks(t *testing.T) {
	for _, namespace := range []string{"readers", "goldens"} {
		t.Run(namespace, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			v := createManagedTest(t, m, "data")
			backing := m.managed.path(volumeID(v.ID))
			f, openErr := os.OpenFile(backing, os.O_WRONLY, 0)
			if openErr != nil {
				t.Fatal(openErr)
			}
			_, writeErr := f.WriteAt([]byte("preserved"), 4096)
			if err := errors.Join(writeErr, f.Close(), m.Close()); err != nil {
				t.Fatal(err)
			}
			cleanup := filepath.Join(cfg.DataDir, namespace)
			if err := os.Remove(cleanup); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(cfg.ManagedVolumeDir, "disks"), cleanup); err != nil {
				t.Fatal(err)
			}
			next, openErr := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
			if openErr == nil {
				t.Error("constructor accepted cleanup alias into managed backing directory")
				if err := next.Reconcile(t.Context()); err != nil {
					t.Errorf("reconcile: %v", err)
				}
				next.Close()
			}
			f, openErr = os.Open(backing)
			if openErr != nil {
				t.Fatalf("managed backing lost: %v", openErr)
			}
			defer f.Close()
			marker := make([]byte, len("preserved"))
			if _, err := f.ReadAt(marker, 4096); err != nil || string(marker) != "preserved" {
				t.Fatalf("managed bytes changed: %q %v", marker, err)
			}
		})
	}
}

func TestManagedStoppedClaimRetainsOwnershipOnRestart(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "new-failure-release"
		if failed {
			name = "previous-failure-expiry"
		}
		t.Run(name, func(t *testing.T) {
			cfg := managedConfig(t)
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			v := createManagedTest(t, m, "data")
			sb := foundationClaim(t, m, "data")
			backing := m.managed.path(volumeID(v.ID))
			identity, err := inspectBacking(backing)
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				sb.Failed = "ordinary failure requiring explicit release"
				if err := m.store.commit(m.store.set(sb)); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			e.mu.Lock()
			e.stopped[sb.VMName] = true
			e.pids[sb.VMName] = 0
			e.mu.Unlock()
			for restart := range 2 {
				m = openManagedTest(t, cfg, e)
				if err := m.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, live := m.Sandbox(sb.ID); !live {
					t.Fatalf("restart %d discarded the durable stopped claim", restart)
				}
				runLiveness(t, m)
				listed := m.Sandboxes("t1", "", nil)
				if len(listed) != 1 || listed[0].ID != sb.ID || listed[0].Failed == "" {
					t.Fatalf("restart %d did not list the failed owner: %+v", restart, listed)
				}
				if rec, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || !present || rec.State != vmStateStopped || rec.PID != 0 {
					t.Fatalf("restart %d changed stopped VM record: %+v present=%t err=%v", restart, rec, present, err)
				}
				if current, err := inspectBacking(backing); err != nil || current != identity {
					t.Fatalf("restart %d changed backing identity: %+v %v", restart, current, err)
				}
				_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
				requireVolumeStatus(t, err, 409)
				if _, _, _, wakeErr := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(wakeErr, ErrFailed) {
					t.Fatalf("new data plane request returned %v, want ErrFailed", wakeErr)
				}
				if wakeErr := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(wakeErr, ErrFailed) {
					t.Fatalf("explicit wake returned %v, want ErrFailed", wakeErr)
				}
				e.mu.Lock()
				changed := len(e.removes) != 0 || len(e.starts) != 0
				e.mu.Unlock()
				if changed {
					t.Fatal("recovery cold-booted or removed the stopped owner")
				}
				persisted, err := m.store.load()
				if err != nil || persisted[sb.ID] == nil || persisted[sb.ID].Failed == "" || persisted[sb.ID].VMName != sb.VMName {
					t.Fatalf("failed claim identity not durable: %+v %v", persisted[sb.ID], err)
				}
				if !failed {
					if events := usageEventsOf(t, m, sb.ID); events != "claim,vmm_exit,vmm_failed" {
						t.Fatalf("restart %d lost or repeated VMM exit audit: %s", restart, events)
					}
				}
				if restart == 0 {
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if failed {
				m.mu.Lock()
				m.claimed[sb.ID].Deadline = time.Now().Add(-time.Minute)
				m.mu.Unlock()
				m.reapOnce(t.Context())
			} else if err := m.Release(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
			if _, live := m.Sandbox(sb.ID); live {
				t.Fatal("release or expiry retained the failed claim")
			}
			if _, present, err := e.Inspect(t.Context(), sb.VMName); err != nil || present {
				t.Fatalf("release or expiry retained VM record: present=%t err=%v", present, err)
			}
			if current, err := inspectBacking(backing); err != nil || current != identity {
				t.Fatalf("release or expiry changed backing identity: %+v %v", current, err)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data")); err != nil {
				t.Fatalf("release or expiry did not free writer admission: %v", err)
			}
		})
	}
}
