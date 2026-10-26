package pool

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestStaleClaimOperationsPreserveTheCurrentClaim(t *testing.T) {
	hibernate := func(t *testing.T, m *Manager, sb *types.Sandbox) error {
		return m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
	}
	rollback := func(op func(*testing.T, *Manager, *types.Sandbox) error) func(*testing.T, *Manager, *types.Sandbox, func()) {
		return func(t *testing.T, m *Manager, sb *types.Sandbox, stale func()) {
			m.store.writeMu.Lock()
			done := make(chan error, 1)
			go func() { done <- op(t, m, sb) }()
			waitFor(t, func() bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				return sb.PendingSnap != "" || sb.ArchivePending != ""
			})
			stale()
			restore := failClaimWrites(t, m.store)
			m.store.writeMu.Unlock()
			if err := <-done; err == nil {
				t.Fatal("intent write succeeded despite filesystem failure")
			}
			restore()
		}
	}
	for _, tc := range []struct {
		name       string
		newManager func(t *testing.T) *Manager
		prepare    func(t *testing.T, m *Manager, sb *types.Sandbox)
		run        func(t *testing.T, m *Manager, sb *types.Sandbox, stale func())
	}{
		{
			name:       "hibernate",
			newManager: func(t *testing.T) *Manager { return newTestManager(t, newFakeEngine()) },
			run: func(t *testing.T, m *Manager, sb *types.Sandbox, stale func()) {
				stale()
				sb.Transition.Lock()
				err := m.hibernateLocked(t.Context(), sb)
				sb.Transition.Unlock()
				if !errors.Is(err, ErrUnknownSandbox) {
					t.Errorf("stale Hibernate: %v, want ErrUnknownSandbox", err)
				}
				if m.eng.(*fakeEngine).hibernateCount() != 0 {
					t.Error("stale Hibernate reached the engine")
				}
			},
		},
		{
			name:       "hibernate intent rollback",
			newManager: func(t *testing.T) *Manager { return newTestManager(t, newFakeEngine()) },
			run:        rollback(hibernate),
		},
		{
			name:       "archive intent rollback",
			newManager: func(t *testing.T) *Manager { return newFailoverCluster(t).node(t, "a", t.TempDir(), keepAnHour) },
			prepare: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				if err := hibernate(t, m, sb); err != nil {
					t.Fatal(err)
				}
			},
			run: rollback(func(t *testing.T, m *Manager, sb *types.Sandbox) error { return m.archive(t.Context(), sb) }),
		},
	} {
		for _, replace := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replace=%t", tc.name, replace), func(t *testing.T) {
				m := tc.newManager(t)
				sb := mustClaim(t, m, testKey)
				ck, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
				if err != nil {
					t.Fatal(err)
				}
				if tc.prepare != nil {
					tc.prepare(t, m, sb)
				}
				tc.run(t, m, sb, func() {
					m.mu.Lock()
					defer m.mu.Unlock()
					m.dropClaimLocked(sb)
					m.store.del(sb.ID)
					if replace {
						replacement := &types.Sandbox{ID: sb.ID, ReplicaCk: ck.ID}
						m.addClaimLocked(replacement)
						m.store.set(replacement)
					}
				})
				if err = m.store.commit(m.store.mark()); err != nil {
					t.Fatal(err)
				}
				loaded, err := newClaimStore(m.dataDir, false).load()
				if err != nil {
					t.Fatal(err)
				}
				if replace {
					if loaded[sb.ID] == nil || loaded[sb.ID].ReplicaCk != ck.ID || loaded[sb.ID].PendingSnap != "" {
						t.Errorf("stale operation overwrote replacement on disk: %+v", loaded[sb.ID])
					}
				} else if loaded[sb.ID] != nil {
					t.Errorf("stale operation resurrected released claim on disk: %+v", loaded[sb.ID])
				}
				if err = m.markArchiveCk(ck.ID); err != nil {
					t.Fatal(err)
				}
				m.retryArchiveDeletes(t.Context())
				if exists := ckExists(t, m, ck.ID); exists != replace {
					t.Errorf("checkpoint survived cleanup = %v, want %v", exists, replace)
				}
			})
		}
	}
}
