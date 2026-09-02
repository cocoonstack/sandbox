package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestCheckpointThenBranch(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)

	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "step-1", "")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !strings.HasPrefix(ckpt.ID, "ck_") || ckpt.SandboxID != src.ID || ckpt.Key != src.Key {
		t.Fatalf("record %+v, want ck_* id bound to source", ckpt)
	}
	if len(eng.snapSaves) != 1 || !strings.HasPrefix(eng.snapSaves[0], forkPrefix) {
		t.Errorf("snapSaves %v, want one transient capture", eng.snapSaves)
	}
	if len(eng.removedNames()) != 0 {
		t.Error("checkpoint destroyed a VM; the source must keep running")
	}

	branch, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour, "")
	if err != nil {
		t.Fatalf("ClaimCheckpoint: %v", err)
	}
	if branch.ID == src.ID || branch.Key != src.Key || branch.FromCheckpoint != ckpt.ID {
		t.Errorf("branch %+v, want fresh id, source key, lineage to %s", branch, ckpt.ID)
	}

	ckpts, err := m.Checkpoints(t.Context(), "")
	if err != nil || len(ckpts) != 1 || ckpts[0].ID != ckpt.ID {
		t.Errorf("Checkpoints() = %+v, %v; want the one record", ckpts, err)
	}

	if err := m.DeleteCheckpoint(t.Context(), ckpt.ID, "", DeleteFleet); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if _, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour, ""); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("claim after delete: %v, want ErrUnknownCheckpoint", err)
	}
}

func TestCheckpointValidation(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)

	if _, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: "wrong-token"}, "", ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("bad token: %v, want ErrUnknownSandbox", err)
	}
	if _, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "bad name", ""); !errors.Is(err, ErrBadName) {
		t.Errorf("bad name: %v, want ErrBadName", err)
	}
	for _, id := range []string{"../../etc", "ck_zz", "", "ck_0011223344556677x"} {
		if _, err := m.ClaimCheckpoint(t.Context(), id, time.Hour, ""); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("claim %q: %v, want ErrUnknownCheckpoint", id, err)
		}
		if err := m.DeleteCheckpoint(t.Context(), id, "", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("delete %q: %v, want ErrUnknownCheckpoint", id, err)
		}
	}
}

func TestReconcileSweepsCheckpointStaging(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	stale := filepath.Join(m.dataDir, "checkpoints", "ck_00ff00ff00ff00ff-1234.tmp")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("staging survived reconcile: %v", err)
	}
	if ckpts, err := m.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 0 {
		t.Errorf("Checkpoints() = %+v, %v; want none", ckpts, err)
	}
}

func TestRetentionSweepsExpiredCheckpoints(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.ckptTTL = time.Hour
	plant := func(id string, age time.Duration) {
		t.Helper()
		dir := filepath.Join(m.dataDir, "checkpoints", id)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("setup: %v", err)
		}
		meta, _ := json.Marshal(types.Checkpoint{ID: id, CreatedAt: time.Now().Add(-age)})
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0o600); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	plant("ck_000000000000000a", 2*time.Hour)
	plant("ck_000000000000000b", time.Minute)

	m.sweepExpiredCheckpoints(t.Context())

	ckpts, err := m.Checkpoints(t.Context(), "")
	if err != nil || len(ckpts) != 1 || ckpts[0].ID != "ck_000000000000000b" {
		t.Fatalf("after sweep: %+v, %v — want only the fresh checkpoint", ckpts, err)
	}
}

func TestCheckpointTenantIsolation(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	srcA, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
	if err != nil {
		t.Fatalf("acme claim: %v", err)
	}
	srcB, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "beta", "", nil)
	if err != nil {
		t.Fatalf("beta claim: %v", err)
	}
	ckA, err := m.Checkpoint(t.Context(), srcA.ID, Cred{Token: srcA.Token}, "a-step", "acme")
	if err != nil {
		t.Fatalf("acme checkpoint: %v", err)
	}
	ckB, err := m.Checkpoint(t.Context(), srcB.ID, Cred{Token: srcB.Token}, "b-step", "beta")
	if err != nil {
		t.Fatalf("beta checkpoint: %v", err)
	}
	if ckA.Tenant != "acme" || ckB.Tenant != "beta" {
		t.Errorf("tenants %q/%q, want acme/beta", ckA.Tenant, ckB.Tenant)
	}

	if ckpts, err := m.Checkpoints(t.Context(), "acme"); err != nil || len(ckpts) != 1 || ckpts[0].ID != ckA.ID {
		t.Errorf("acme listing %+v, %v; want only its record", ckpts, err)
	}
	if ckpts, err := m.Checkpoints(t.Context(), ""); err != nil || len(ckpts) != 2 {
		t.Errorf("root listing %+v, %v; want both records", ckpts, err)
	}

	if err := m.DeleteCheckpoint(t.Context(), ckA.ID, "beta", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("cross-tenant delete: %v, want ErrUnknownCheckpoint", err)
	}
	if err := m.DeleteCheckpoint(t.Context(), ckA.ID, "acme", DeleteFleet); err != nil {
		t.Errorf("own delete: %v", err)
	}
	if err := m.DeleteCheckpoint(t.Context(), ckB.ID, "", DeleteFleet); err != nil {
		t.Errorf("root delete: %v", err)
	}
}

func TestCheckpointDeleteEvictsRecLock(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)

	base := lockCount(m)

	var ids []string
	for range 5 {
		ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "")
		if err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		ids = append(ids, ckpt.ID)
	}
	for _, id := range ids {
		if err := m.DeleteCheckpoint(t.Context(), id, "", DeleteFleet); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
		if hasRecLock(m, id) {
			t.Errorf("recLocks retained a lock for deleted checkpoint %s", id)
		}
	}
	if got := lockCount(m); got != base {
		t.Errorf("recLocks grew %d->%d over 5 checkpoint create+delete cycles, want no net growth", base, got)
	}
}

func TestRejectedCheckpointOpsLeakNoRecLock(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	base := lockCount(m)

	for _, id := range []string{"../../etc", "ck_zz", "", "ck_00112233445566ff", "ck_deadbeefdeadbeef"} {
		if _, err := m.ClaimCheckpoint(t.Context(), id, time.Hour, ""); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("claim %q: %v, want ErrUnknownCheckpoint", id, err)
		}
		if err := m.DeleteCheckpoint(t.Context(), id, "", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("delete %q: %v, want ErrUnknownCheckpoint", id, err)
		}
	}
	if got := lockCount(m); got != base {
		t.Errorf("recLocks grew %d->%d over rejected checkpoint ops, want no growth", base, got)
	}

	src := mustClaim(t, m, testKey)
	ckpt, err := m.Checkpoint(t.Context(), src.ID, Cred{Token: src.Token}, "", "acme")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	before := lockCount(m)
	if err := m.DeleteCheckpoint(t.Context(), ckpt.ID, "other", DeleteFleet); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("wrong-tenant delete: %v, want ErrUnknownCheckpoint", err)
	}
	if got := lockCount(m); got != before {
		t.Errorf("recLocks grew %d->%d over a wrong-tenant delete, want no growth", before, got)
	}
}
