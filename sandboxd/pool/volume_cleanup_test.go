package pool

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRemovedVMKeepsVolumeUntilCanceledWorkerReturns(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, nil)
	if err := m.RegisterVolume(t.Context(), "data", types.VolumeRegistration{Path: writeVolumeImage(t, "runtime", "bytes"), Writable: true}); err != nil {
		t.Fatal(err)
	}
	sb := mustClaim(t, m, testKey)
	eng := newGatedVolumeEngine(base)
	m.eng = eng
	changed := make(chan error, 1)
	go func() { _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); changed <- err }()
	<-eng.entered
	resumed := false
	defer func() {
		if !resumed {
			close(eng.resume)
			<-changed
		}
	}()
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("release waited for paused worker")
	}
	m.retryRemovals(t.Context()).Wait()
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{readers: 1}) {
		t.Errorf("removed VM freed active worker count: %+v", got)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("writer overtook paused canceled worker: %v", err)
	}
	if err := m.UnregisterVolume(t.Context(), "data"); !errors.Is(err, ErrVolumeRegistrationConflict) {
		t.Errorf("unregister overtook paused worker: %v", err)
	}
	close(eng.resume)
	resumed = true
	if err := <-changed; err == nil {
		t.Fatal("removed claim accepted late attachment")
	}
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("completed worker leaked count: %+v", got)
	}
	if n := len(slices.DeleteFunc(base.removedNames(), func(n string) bool { return n != sb.VMName })); n != 1 {
		t.Fatalf("vm rm ran %d times, want once", n)
	}
	if err := m.UnregisterVolume(t.Context(), "data"); err != nil {
		t.Fatal(err)
	}
}

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
