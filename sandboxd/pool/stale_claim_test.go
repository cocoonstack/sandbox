package pool

import (
	"errors"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestHibernateRejectsStaleClaim(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "released"
		if replace {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng)
			sb := mustClaim(t, m, testKey)
			ck, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			if replace {
				m.mu.Lock()
				replacement := &types.Sandbox{ID: sb.ID, ReplicaCk: ck.ID}
				m.addClaimLocked(replacement)
				js := m.store.set(replacement)
				m.mu.Unlock()
				if err = m.store.commit(js); err != nil {
					t.Fatal(err)
				}
			}
			sb.Transition.Lock()
			err = m.hibernateLocked(t.Context(), sb)
			sb.Transition.Unlock()
			if !errors.Is(err, ErrUnknownSandbox) {
				t.Errorf("stale Hibernate: %v, want ErrUnknownSandbox", err)
			}
			if eng.hibernateCount() != 0 {
				t.Error("stale Hibernate reached the engine")
			}
			assertStaleClaimPreserved(t, m, sb.ID, ck.ID, replace)
		})
	}
}

func TestIntentRollbackPreservesCurrentClaim(t *testing.T) {
	for _, operation := range []string{"hibernate", "archive"} {
		for _, replace := range []bool{false, true} {
			name := operation + "/released"
			if replace {
				name = operation + "/replaced"
			}
			t.Run(name, func(t *testing.T) {
				var m *Manager
				if operation == "archive" {
					m = newFailoverCluster(t).node(t, "a", t.TempDir(), keepAnHour)
				} else {
					m = newTestManager(t, newFakeEngine())
				}
				sb := mustClaim(t, m, testKey)
				ck, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
				if err != nil {
					t.Fatal(err)
				}
				if operation == "archive" {
					if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
						t.Fatal(err)
					}
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
					if operation == "archive" {
						done <- m.archive(t.Context(), sb)
					} else {
						done <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
					}
				}()
				waitFor(t, func() bool {
					m.mu.Lock()
					defer m.mu.Unlock()
					return sb.PendingSnap != "" || sb.ArchivePending != ""
				})
				m.mu.Lock()
				m.dropClaimLocked(sb)
				m.store.del(sb.ID)
				if replace {
					replacement := &types.Sandbox{ID: sb.ID, ReplicaCk: ck.ID}
					m.addClaimLocked(replacement)
					m.store.set(replacement)
				}
				m.mu.Unlock()
				restore := failClaimWrites(t, m.store)
				m.store.writeMu.Unlock()
				locked = false
				if err := <-done; err == nil {
					t.Fatal("intent write succeeded despite filesystem failure")
				}
				restore()
				if err := m.store.commit(m.store.mark()); err != nil {
					t.Fatal(err)
				}
				assertStaleClaimPreserved(t, m, sb.ID, ck.ID, replace)
			})
		}
	}
}

func assertStaleClaimPreserved(t *testing.T, m *Manager, id, ck string, replaced bool) {
	t.Helper()
	loaded, err := newClaimStore(m.dataDir, false).load()
	if err != nil {
		t.Fatal(err)
	}
	if replaced {
		if loaded[id] == nil || loaded[id].ReplicaCk != ck || loaded[id].PendingSnap != "" {
			t.Errorf("stale operation overwrote replacement on disk: %+v", loaded[id])
		}
	} else if loaded[id] != nil {
		t.Errorf("stale operation resurrected released claim on disk: %+v", loaded[id])
	}
	if err := m.markArchiveCk(ck); err != nil {
		t.Fatal(err)
	}
	m.retryArchiveDeletes(t.Context())
	if exists := ckExists(t, m, ck); exists != replaced {
		t.Errorf("checkpoint survived cleanup = %v, want %v", exists, replaced)
	}
}
