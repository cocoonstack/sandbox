package pool

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestDispatchedCleanupRetainsCatalogReference(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, nil)
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes")}); err != nil {
		t.Fatal(err)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
	if err != nil {
		t.Fatal(err)
	}
	base.removeErrFor = sb.VMName
	if err = m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("expected survivor")
	}
	wg := dispatchBlocked(t.Context(), t, m)
	err = m.UnregisterVolume(t.Context(), "data")
	unblock(m, wg)
	if !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Fatalf("dispatched cleanup lost guard: %v", err)
	}
	base.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()
	if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
}

// dispatchBlocked moves the removal queue into workers parked on a full maintenance semaphore.
func dispatchBlocked(ctx context.Context, t *testing.T, m *Manager) *sync.WaitGroup {
	t.Helper()
	for range cap(m.maintSem) {
		m.maintSem <- struct{}{}
	}
	wg := m.retryRemovals(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pendingRemovals) != 0 {
		t.Fatal("cleanup was not dispatched")
	}
	return wg
}

func unblock(m *Manager, wg *sync.WaitGroup) {
	for range cap(m.maintSem) {
		<-m.maintSem
	}
	wg.Wait()
}
