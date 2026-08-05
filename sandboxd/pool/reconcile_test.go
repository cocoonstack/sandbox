package pool

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestReconcileStaleCreateSweep(t *testing.T) {
	tests := []staleCreateCase{
		{"collected frees the record", engine.StaleCreateCollected, nil, false, false, false},
		{"not-found treats the record as gone", engine.StaleCreateNotFound, nil, false, false, false},
		{"busy leaves the in-flight clone", engine.StaleCreateBusy, nil, true, false, true},
		{"not-creating removes normally", engine.StaleCreateNotCreating, nil, false, true, false},
		{"verb failure falls back to remove", "", errors.New("unknown command"), false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.vms["sbx-stale-1"] = "/vsock/sbx-stale-1"
			eng.creating["sbx-stale-1"] = true
			eng.staleOutcome, eng.staleErr = tt.outcome, tt.err
			m := newTestManager(t, eng)

			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if !slices.Contains(eng.staleReconciles, "sbx-stale-1") {
				t.Fatal("reconcile-stale-create not attempted for the creating VM")
			}
			eng.mu.Lock()
			_, present := eng.vms["sbx-stale-1"]
			eng.mu.Unlock()
			if present != tt.wantPresent {
				t.Errorf("present=%v, want %v", present, tt.wantPresent)
			}
			if eng.removed("sbx-stale-1") != tt.wantRemove {
				t.Errorf("removed=%v, want %v", eng.removed("sbx-stale-1"), tt.wantRemove)
			}
			m.mu.Lock()
			_, pending := m.pendingRemovals["sbx-stale-1"]
			m.mu.Unlock()
			if pending != tt.wantPending {
				t.Errorf("pending=%v, want %v", pending, tt.wantPending)
			}
		})
	}
}

func TestBusyStaleCreateRetryConverges(t *testing.T) {
	tests := []staleCreateCase{
		{"collected", engine.StaleCreateCollected, nil, false, false, false},
		{"not-found", engine.StaleCreateNotFound, nil, false, false, false},
		{"not-creating", engine.StaleCreateNotCreating, nil, false, true, false},
		{"still busy", engine.StaleCreateBusy, nil, true, false, true},
		{"verb failure", "", errors.New("temporary failure"), true, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.vms["sbx-stale-1"] = "/vsock/sbx-stale-1"
			eng.creating["sbx-stale-1"] = true
			eng.staleOutcome = engine.StaleCreateBusy
			m := newTestManager(t, eng)

			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			eng.mu.Lock()
			eng.staleOutcome, eng.staleErr = tt.outcome, tt.err
			eng.mu.Unlock()
			m.retryRemovals(t.Context()).Wait()

			eng.mu.Lock()
			_, present := eng.vms["sbx-stale-1"]
			eng.mu.Unlock()
			m.mu.Lock()
			_, pending := m.pendingRemovals["sbx-stale-1"]
			m.mu.Unlock()
			removed := eng.removed("sbx-stale-1")
			if present != tt.wantPresent || removed != tt.wantRemove || pending != tt.wantPending {
				t.Errorf("present=%v removed=%v pending=%v, want %v %v %v",
					present, removed, pending,
					tt.wantPresent, tt.wantRemove, tt.wantPending)
			}
		})
	}
}

func TestReconcileBusyStaleCreateKeepsTap(t *testing.T) {
	eng := newFakeEngine()
	eng.tap = "tap-busy"
	eng.vms["sbx-stale-1"] = "/vsock/sbx-stale-1"
	eng.creating["sbx-stale-1"] = true
	eng.staleOutcome = engine.StaleCreateBusy
	m := newTestManager(t, eng)

	var gotKeep map[string]bool
	m.sweep = func(keep map[string]bool) error { gotKeep = keep; return nil }
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !gotKeep["tap-busy"] {
		t.Error("busy VM's tap not kept; the sweep would tear down an in-flight clone's tables")
	}
	m.mu.Lock()
	pending, ok := m.pendingRemovals["sbx-stale-1"]
	m.mu.Unlock()
	if !ok || !pending.staleCreate || pending.tap != "tap-busy" {
		t.Errorf("pending=%+v present=%v, want stale-create retry with tap-busy", pending, ok)
	}
}

func TestRemoveStaleVMCanceledCtxStillChecksBusy(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-stale-1"] = "/vsock/sbx-stale-1"
	eng.creating["sbx-stale-1"] = true
	eng.staleOutcome = engine.StaleCreateBusy
	m := newTestManager(t, eng)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := types.VMRecord{State: vmStateCreating, Config: types.VMConfig{Name: "sbx-stale-1"}}
	if m.removeStaleVM(ctx, "sbx-stale-1", rec) {
		t.Error("busy VM reported gone under a canceled ctx")
	}
	if len(eng.staleReconciles) != 1 {
		t.Errorf("verb ran %d times, want 1 (cancellation-immune)", len(eng.staleReconciles))
	}
	if eng.removed("sbx-stale-1") {
		t.Error("canceled ctx fell through to the forced remove the verb guards")
	}
}

func TestReconcileStaleRunningVMSkipsVerb(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-orphan-1"] = "/vsock/sbx-orphan-1"
	m := newTestManager(t, eng)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(eng.staleReconciles) != 0 {
		t.Errorf("reconcile-stale-create ran on a running VM: %v", eng.staleReconciles)
	}
	if !eng.removed("sbx-orphan-1") {
		t.Error("unowned running VM not removed")
	}
}

// staleCreateCase drives both the startup sweep and the reap-tick retry
// through the same outcome/end-state row shape.
type staleCreateCase struct {
	name        string
	outcome     engine.StaleCreateOutcome
	err         error
	wantPresent bool
	wantRemove  bool
	wantPending bool
}
