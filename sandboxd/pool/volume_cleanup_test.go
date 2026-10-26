package pool

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
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

func TestStaleVMFenceFollowsInspection(t *testing.T) {
	writer, reader := []types.VMDisk{{Name: "data"}}, []types.VMDisk{{Name: "data", ReadOnly: true}}
	busy, fenced, inspectErr := ErrVolumeBusy, ErrVolumeCatalogUnavailable, errors.New("inspection unavailable")
	for _, tc := range []struct {
		name, swept, listed, state string
		disks                      []types.VMDisk
		inspectErr                 error
		want                       [3]error // data, data rw, other
		register                   error
	}{
		{"running writer", vmStateRunning, "sbx-stale", vmStateRunning, writer, nil, [3]error{busy, busy, nil}, nil},
		{"running reader", vmStateRunning, "sbx-stale", vmStateRunning, reader, nil, [3]error{nil, busy, nil}, nil},
		{"running empty", vmStateRunning, "sbx-stale", vmStateRunning, []types.VMDisk{}, nil, [3]error{}, nil},
		{"running omitted disks", vmStateRunning, "sbx-stale", vmStateRunning, nil, nil, [3]error{fenced, fenced, fenced}, fenced},
		{"stopped", vmStateRunning, "sbx-stale", vmStateStopped, writer, nil, [3]error{}, nil},
		{"inspected creating", vmStateRunning, "sbx-stale", vmStateCreating, writer, nil, [3]error{fenced, fenced, fenced}, fenced},
		{"inspect error", vmStateRunning, "sbx-stale", vmStateRunning, writer, inspectErr, [3]error{fenced, fenced, fenced}, fenced},
		{"busy creating", vmStateCreating, "sbx-stale", vmStateCreating, writer, nil, [3]error{fenced, fenced, fenced}, fenced},
		{"busy running", vmStateCreating, "sbx-stale", vmStateRunning, reader, nil, [3]error{nil, busy, nil}, nil},
		{"busy omitted disks", vmStateCreating, "sbx-stale", vmStateRunning, nil, nil, [3]error{fenced, fenced, fenced}, fenced},
		{"busy stopped", vmStateCreating, "sbx-stale", vmStateStopped, writer, nil, [3]error{}, nil},
		{"busy absent", vmStateCreating, "sbx-other", vmStateRunning, writer, nil, [3]error{}, nil},
		{"busy inspect error", vmStateCreating, "sbx-stale", vmStateRunning, writer, inspectErr, [3]error{fenced, fenced, fenced}, fenced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			name := "sbx-stale"
			eng.vms[tc.listed], eng.removeErrFor, eng.staleOutcome = "/vsock/stale", name, engine.StaleCreateBusy
			eng.stopped[tc.listed], eng.creating[tc.listed] = tc.state == vmStateStopped, tc.state == vmStateCreating
			eng.inspectHook = func() error { return tc.inspectErr }
			if m.removeStaleVM(t.Context(), name, types.VMRecord{State: tc.swept, Config: types.VMConfig{Name: name}}) {
				t.Fatal("stale VM reported gone")
			}
			// the disk list appears after the sweep, so running rows are classified by the retry
			eng.disks = map[string][]types.VMDisk{name: tc.disks}
			m.retryRemovals(t.Context()).Wait()
			for i, volume := range []types.Volume{{Name: "data"}, {Name: "data", Mode: types.VolumeModeRW}, {Name: "other"}} {
				if err := claimAndRelease(t, m, volume); !errors.Is(err, tc.want[i]) {
					t.Errorf("claim %+v = %v, want %v", volume, err, tc.want[i])
				}
			}
			if err := m.RegisterVolume(t.Context(), "fresh", types.VolumeRegistration{Path: writeVolumeImage(t, "fresh", "bytes")}); !errors.Is(err, tc.register) {
				t.Errorf("unrelated registration = %v, want %v", err, tc.register)
			}
			if err := claimAndRelease(t, m); err != nil {
				t.Errorf("volume-free claim: %v", err)
			}
			eng.removeErrFor, eng.staleOutcome = "", engine.StaleCreateCollected
			m.retryRemovals(t.Context()).Wait()
			for _, volume := range []types.Volume{{Name: "data", Mode: types.VolumeModeRW}, {Name: "other", Mode: types.VolumeModeRW}} {
				if err := claimAndRelease(t, m, volume); err != nil {
					t.Errorf("claim %+v after confirmed removal: %v", volume, err)
				}
			}
			if m.catalog.unknown != 0 {
				t.Fatalf("cleanup retained or double-released its fence: unknown=%d", m.catalog.unknown)
			}
		})
	}
}

func TestStaleInventoryIsTakenAfterFailedRemoval(t *testing.T) {
	sweep := func(ctx context.Context, m *Manager, name string) {
		m.removeStaleVM(ctx, name, types.VMRecord{Config: types.VMConfig{Name: name}})
	}
	retry := func(ctx context.Context, m *Manager, _ string) { m.retryRemovals(ctx).Wait() }
	for _, tc := range []struct {
		name          string
		before, after func(context.Context, *Manager, string)
	}{{"sweep", func(context.Context, *Manager, string) {}, sweep}, {"retry", sweep, retry}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				m := newVolumeManager(t, eng, liveCatalog(t))
				name := "sbx-attach-during-remove"
				eng.vms[name], eng.removeErrFor = "/vsock/stale", name
				tc.before(t.Context(), m, name)
				eng.disks = map[string][]types.VMDisk{name: {{Name: "data", ReadOnly: true}}}
				eng.removeStall = make(chan struct{})
				done := make(chan struct{})
				go func() { defer close(done); tc.after(t.Context(), m, name) }()
				synctest.Wait()
				if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
					t.Errorf("in-flight removal admitted a reader: %v", err)
				}
				eng.mu.Lock()
				eng.disks[name] = append(eng.disks[name], types.VMDisk{Name: "other"})
				eng.mu.Unlock()
				close(eng.removeStall)
				<-done
				for _, c := range []struct {
					volume types.Volume
					want   error
				}{
					{types.Volume{Name: "other", Mode: types.VolumeModeRW}, ErrVolumeBusy},
					{types.Volume{Name: "data", Mode: types.VolumeModeRW}, ErrVolumeBusy},
					{types.Volume{Name: "data"}, nil},
				} {
					if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{c.volume}}); !errors.Is(err, c.want) {
						t.Errorf("claim %+v = %v, want %v", c.volume, err, c.want)
					}
				}
			})
		})
	}
}

func TestUnknownFencePrecedesInspection(t *testing.T) {
	base := newFakeEngine()
	m := newVolumeManager(t, base, liveCatalog(t))
	name := "sbx-stale-before-inspect"
	base.vms[name], base.removeErrFor = "/vsock/stale", name
	entered, resume := make(chan struct{}), make(chan struct{})
	var gated atomic.Bool
	base.inspectHook = func() error {
		if gated.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
		return errors.New("inspection unavailable")
	}
	done := make(chan bool, 1)
	go func() { done <- m.removeStaleVM(t.Context(), name, base.record(name)) }()
	<-entered
	_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	close(resume)
	if gone := <-done; gone || !errors.Is(err, ErrVolumeCatalogUnavailable) {
		t.Fatalf("admission while inspection was blocked: %v (gone=%t)", err, gone)
	}
}

func TestCanceledRemovalDispatchKeepsUnknownFenceOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newFakeEngine()
		m := newVolumeManager(t, base, liveCatalog(t))
		name := "sbx-canceled-dispatch"
		base.vms[name], base.removeErrFor = "/vsock/stale", name
		base.inspectHook = func() error { return errors.New("inspection unavailable") }
		if m.removeStaleVM(t.Context(), name, base.record(name)) {
			t.Fatal("expected survivor")
		}
		ctx, cancel := context.WithCancel(t.Context())
		wg := dispatchBlocked(ctx, t, m)
		cancel()
		synctest.Wait()
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}}); !errors.Is(err, ErrVolumeCatalogUnavailable) {
			t.Fatalf("dispatched unknown fence lost: %v", err)
		}
		base.removeErrFor = ""
		unblock(m, wg)
		m.retryRemovals(t.Context()).Wait()
		if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
			t.Fatalf("canceled dispatch stranded unknown fence: %v", err)
		}
	})
}

func claimAndRelease(t *testing.T, m *Manager, volumes ...types.Volume) error {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: volumes})
	if err != nil {
		return err
	}
	return m.Release(t.Context(), sb.ID, Cred{Token: sb.Token})
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
