package dir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

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

func TestFetchPinsGenerationAcrossStoreInstances(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	reader, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	writer, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	staging, err := writer.Stage(id)
	if err != nil {
		t.Fatalf("stage first: %v", err)
	}
	seedRecord(t, staging, "first")
	if err = writer.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish first: %v", err)
	}

	dir, meta, release, err := reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if string(meta) != `{"id":"first"}` {
		t.Fatalf("fetch meta %q, want first generation", meta)
	}
	if lock, lockErr := writer.lockRecord(id, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(lockErr, unix.EWOULDBLOCK) {
		if lockErr == nil {
			unlockRecord(lock)
		}
		t.Fatalf("writer lock while fetch is live: %v, want EWOULDBLOCK", lockErr)
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Fatalf("pinned export: %v", statErr)
	}
	release()

	staging, err = writer.Stage(id)
	if err != nil {
		t.Fatalf("stage second: %v", err)
	}
	seedRecord(t, staging, "second")
	if err = writer.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	_, meta, release, err = reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch second: %v", err)
	}
	defer release()
	if string(meta) != `{"id":"second"}` {
		t.Fatalf("fetch meta %q, want second generation", meta)
	}
}

func TestRecordLockFilesStayBoundedAcrossIDChurn(t *testing.T) {
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := map[string]struct{}{}
	for i := range recordLockStripes * 16 {
		id := fmt.Sprintf("ck_%016x", i)
		want[filepath.Base(st.recordLockPath(id))] = struct{}{}
		if _, _, _, err = st.Fetch(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("Fetch missing %s: %v, want store.ErrNotFound", id, err)
		}
	}
	locks, err := os.ReadDir(filepath.Join(root, lockDir))
	if err != nil {
		t.Fatalf("read lock stripes: %v", err)
	}
	if len(locks) != len(want) {
		t.Fatalf("lock files after churn = %d, want %d used stripes", len(locks), len(want))
	}
	if len(locks) > recordLockStripes {
		t.Fatalf("lock files after churn = %d, want at most %d", len(locks), recordLockStripes)
	}
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

// TestDeleteRemovesStaleOldGeneration: a <id>.old left by a Publish whose
// cleanup failed must not survive Delete — otherwise the next sweep renames it
// back over the deleted id and resurrects the record.
func TestDeleteRemovesStaleOldGeneration(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id), id)
	seedRecord(t, filepath.Join(root, id+oldSuffix), "stale")

	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.SweepStaging(); err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}
	if _, err := st.ReadMeta(t.Context(), id); err != store.ErrNotFound {
		t.Errorf("deleted record resurrected by a stale .old: %v", err)
	}
}

// TestPublishKeepsUnsweptOldWhenFinalMissing: a Publish must not destroy an
// unswept <id>.old (a crash artifact holding the last good generation) before
// installing the new one — the new record must land while .old is gone.
func TestPublishKeepsUnsweptOldWhenFinalMissing(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id+oldSuffix), "stale")
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	seedRecord(t, staging, id)

	if err = st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	raw, err := st.ReadMeta(t.Context(), id)
	if err != nil || string(raw) != `{"id":"`+id+`"}` {
		t.Fatalf("ReadMeta after publish = %q, %v, want the new generation", raw, err)
	}
	if _, err := os.Stat(filepath.Join(root, id+oldSuffix)); !os.IsNotExist(err) {
		t.Errorf("stale .old not reclaimed after a successful publish: %v", err)
	}
}

// TestMetasSurfacesReadError: a corrupt/unreadable meta must fail the listing,
// not vanish silently — mirroring the s3 backend.
func TestMetasSurfacesReadError(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A directory where meta.json is expected makes ReadFile fail with EISDIR.
	if err := os.MkdirAll(filepath.Join(root, id, store.MetaFile), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := st.Metas(t.Context()); err == nil {
		t.Error("Metas swallowed an unreadable meta; want the error surfaced")
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
