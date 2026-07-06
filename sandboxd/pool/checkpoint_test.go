package pool

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckpointThenBranch(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)

	ckpt, err := m.Checkpoint(t.Context(), src.ID, src.Token, "step-1")
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

	branch, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour)
	if err != nil {
		t.Fatalf("ClaimCheckpoint: %v", err)
	}
	if branch.ID == src.ID || branch.Key != src.Key || branch.FromCheckpoint != ckpt.ID {
		t.Errorf("branch %+v, want fresh id, source key, lineage to %s", branch, ckpt.ID)
	}

	ckpts, err := m.Checkpoints()
	if err != nil || len(ckpts) != 1 || ckpts[0].ID != ckpt.ID {
		t.Errorf("Checkpoints() = %+v, %v; want the one record", ckpts, err)
	}

	if err := m.DeleteCheckpoint(ckpt.ID); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}
	if _, err := m.ClaimCheckpoint(t.Context(), ckpt.ID, time.Hour); !errors.Is(err, ErrUnknownCheckpoint) {
		t.Errorf("claim after delete: %v, want ErrUnknownCheckpoint", err)
	}
}

func TestCheckpointValidation(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	src := mustClaim(t, m, testKey)

	if _, err := m.Checkpoint(t.Context(), src.ID, "wrong-token", ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("bad token: %v, want ErrUnknownSandbox", err)
	}
	if _, err := m.Checkpoint(t.Context(), src.ID, src.Token, "bad name"); !errors.Is(err, ErrBadName) {
		t.Errorf("bad name: %v, want ErrBadName", err)
	}
	for _, id := range []string{"../../etc", "ck_zz", "", "ck_0011223344556677x"} {
		if _, err := m.ClaimCheckpoint(t.Context(), id, time.Hour); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("claim %q: %v, want ErrUnknownCheckpoint", id, err)
		}
		if err := m.DeleteCheckpoint(id); !errors.Is(err, ErrUnknownCheckpoint) {
			t.Errorf("delete %q: %v, want ErrUnknownCheckpoint", id, err)
		}
	}
}

func TestReconcileSweepsCheckpointStaging(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	stale := filepath.Join(m.ckpts.(*dirCheckpointStore).root, "ck_00ff00ff00ff00ff-1234.tmp")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("staging survived reconcile: %v", err)
	}
	if ckpts, err := m.Checkpoints(); err != nil || len(ckpts) != 0 {
		t.Errorf("Checkpoints() = %+v, %v; want none", ckpts, err)
	}
}
