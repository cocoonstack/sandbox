package pool

import (
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestRemoveVMClassifiesOutcome(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)

	eng.vms["survivor"] = "/run/survivor.sock"
	eng.removeErrFor = "survivor"
	if m.removeVM(t.Context(), "survivor") {
		t.Fatal("running VM reported gone")
	}

	delete(eng.vms, "survivor")
	if !m.removeOrRetry(t.Context(), "survivor", "", "", volumeTeardown{}) {
		t.Fatal("absent VM reported present")
	}
	m.mu.Lock()
	pending := len(m.pendingRemovals)
	m.mu.Unlock()
	if pending != 0 {
		t.Fatalf("confirmed-gone VM queued for retry: %d", pending)
	}

	eng.vms["ordinary"] = "/run/ordinary.sock"
	eng.removeErrFor = ""
	if !m.removeVM(t.Context(), "ordinary") || !eng.removed("ordinary") {
		t.Fatal("ordinary removal failed")
	}
}

func TestRemovalRetryConvergesConfirmedSurvivor(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	eng.vms["survivor"] = "/run/survivor.sock"
	eng.removeErrFor = "survivor"

	m.destroy(t.Context(), "survivor")
	m.mu.Lock()
	_, pending := m.pendingRemovals["survivor"]
	m.mu.Unlock()
	if !pending {
		t.Fatal("surviving VM was not retained for retry")
	}

	// A failed retry re-queues itself, so the next tick tries again.
	m.retryRemovals(t.Context()).Wait()
	m.mu.Lock()
	_, pending = m.pendingRemovals["survivor"]
	m.mu.Unlock()
	if !pending {
		t.Fatal("failed retry dropped the survivor from the queue")
	}

	eng.mu.Lock()
	eng.removeErrFor = ""
	eng.mu.Unlock()
	m.retryRemovals(t.Context()).Wait()
	m.mu.Lock()
	_, pending = m.pendingRemovals["survivor"]
	m.mu.Unlock()
	if pending || !eng.removed("survivor") {
		t.Fatalf("retry did not converge: pending=%v removed=%v", pending, eng.removed("survivor"))
	}
}

func TestRemovalRetryFinishesEgressCleanup(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.lockEgress = true
	eng.vms["survivor"] = "/run/survivor.sock"
	eng.removeErrFor = "survivor"
	m.egressTaps["sb_survivor"] = "tap-survivor"

	removed := m.removeOrRetry(t.Context(), "survivor", "sb_survivor", "", volumeTeardown{})
	m.disarmEgress("sb_survivor", removed)
	if removed {
		t.Fatal("surviving VM reported gone")
	}

	eng.mu.Lock()
	eng.removeErrFor = ""
	eng.mu.Unlock()
	m.retryRemovals(t.Context()).Wait()
	m.mu.Lock()
	_, pending := m.pendingRemovals["survivor"]
	_, locked := m.egressTaps["sb_survivor"]
	m.mu.Unlock()
	if pending || locked || !eng.removed("survivor") {
		t.Fatalf("egress cleanup incomplete: pending=%v locked=%v removed=%v",
			pending, locked, eng.removed("survivor"))
	}
}

func TestStaleRemovalRetryKeepsTapForCleanup(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	eng.vms["sbx-stale"] = "/run/stale.sock"
	eng.removeErrFor = "sbx-stale"
	live := map[string]types.VMRecord{
		"sbx-stale": {
			Config:         types.VMConfig{Name: "sbx-stale"},
			NetworkConfigs: []types.VMNetConfig{{TAP: "tap-stale"}},
		},
	}

	m.sweepStaleVMs(t.Context(), live, nil)
	m.mu.Lock()
	tap := m.pendingRemovals["sbx-stale"].tap
	m.mu.Unlock()
	if tap != "tap-stale" {
		t.Fatalf("pending tap %q, want tap-stale", tap)
	}

	eng.mu.Lock()
	eng.removeErrFor = ""
	eng.mu.Unlock()
	m.retryRemovals(t.Context()).Wait()
	m.mu.Lock()
	_, pending := m.pendingRemovals["sbx-stale"]
	m.mu.Unlock()
	if pending || !eng.removed("sbx-stale") {
		t.Fatalf("stale retry did not converge: pending=%v removed=%v", pending, eng.removed("sbx-stale"))
	}
}
