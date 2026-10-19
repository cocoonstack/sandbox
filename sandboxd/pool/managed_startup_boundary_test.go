package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

func TestManagedStartupCollectsUnclaimedCreatingVMBeforeProcessProof(t *testing.T) {
	for _, tc := range []struct {
		name      string
		outcome   engine.StaleCreateOutcome
		collector error
		available bool
	}{
		{name: "collected", outcome: engine.StaleCreateCollected, available: true},
		{name: "not-found", outcome: engine.StaleCreateNotFound, available: true},
		{name: "busy", outcome: engine.StaleCreateBusy},
		{name: "not-creating", outcome: engine.StaleCreateNotCreating},
		{name: "collector-error", collector: errors.New("collector unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newManagedEngine()
			m := openManagedTest(t, managedConfig(t), e)
			createManagedTest(t, m, "data")
			name := "sbx-unclaimed-create"
			e.mu.Lock()
			e.vms[name] = "/fake/vsock"
			e.creating[name] = true
			e.staleOutcome = tc.outcome
			e.staleErr = tc.collector
			e.mu.Unlock()

			err := m.Reconcile(t.Context())
			if tc.available {
				if err != nil {
					t.Fatalf("proven stale create blocked startup: %v", err)
				}
			} else {
				requireVolumeStatus(t, err, 503)
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			if len(e.staleReconciles) != 1 || e.staleReconciles[0] != name {
				t.Fatalf("collector calls=%v, want %s", e.staleReconciles, name)
			}
			if len(e.removes) != 0 {
				t.Fatalf("unsafe Remove fallback called: %v", e.removes)
			}
			_, present := e.vms[name]
			if present == tc.available {
				t.Fatalf("creating VM present=%v after outcome %q", present, tc.outcome)
			}
		})
	}
}

func TestManagedDirectoryRequiresTrustedAncestorOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("fixture needs root to create a root-owned child below a different owner")
	}
	for _, tc := range []struct {
		name   string
		owner  int
		sticky bool
		allow  bool
	}{
		{name: "untrusted-0755", owner: 65534},
		{name: "untrusted-sticky", owner: 65534, sticky: true},
		{name: "root-0755", owner: 0, allow: true},
		{name: "root-sticky", owner: 0, sticky: true, allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "parent")
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			mode := os.FileMode(0755)
			if tc.sticky {
				mode = 0777 | os.ModeSticky
			}
			if err := os.Chmod(parent, mode); err != nil {
				t.Fatal(err)
			}
			managed := filepath.Join(parent, "managed")
			if err := os.Mkdir(managed, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(parent, tc.owner, tc.owner); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chown(parent, 0, 0) })
			err := privateVolumeDir(managed)
			if (err == nil) != tc.allow {
				t.Fatalf("trusted=%v, validation error=%v", tc.allow, err)
			}
		})
	}
}
