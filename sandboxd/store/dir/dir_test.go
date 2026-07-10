package dir

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestDirBackendContract(t *testing.T) {
	st, err := New(t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}

// TestSweepRecoversInterruptedPublish guards the re-publish swap: a crash
// between moving the old generation aside and renaming the new one in leaves
// only <id>.old, and the startup sweep must restore it — the delete-then-
// rename it replaces lost the record outright.
func TestSweepRecoversInterruptedPublish(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id+oldSuffix), id)

	if err = st.SweepStaging(); err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}
	if _, err = st.ReadMeta(t.Context(), id); err != nil {
		t.Fatalf("record not restored after interrupted publish: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id+oldSuffix)); !os.IsNotExist(err) {
		t.Errorf("moved-aside generation still present: %v", err)
	}
}

// TestSweepDropsSupersededOldGeneration: once the new generation is in place,
// the sweep reclaims the parked one instead of resurrecting it.
func TestSweepDropsSupersededOldGeneration(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id), id)
	seedRecord(t, filepath.Join(root, id+oldSuffix), "stale")

	if err = st.SweepStaging(); err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}
	raw, err := st.ReadMeta(t.Context(), id)
	if err != nil || string(raw) != `{"id":"`+id+`"}` {
		t.Fatalf("ReadMeta: %q, %v, want the live generation", raw, err)
	}
	if _, err := os.Stat(filepath.Join(root, id+oldSuffix)); !os.IsNotExist(err) {
		t.Errorf("superseded generation not reclaimed: %v", err)
	}
}

func seedRecord(t *testing.T, dir, metaID string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), []byte(`{"id":"`+metaID+`"}`), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
}
