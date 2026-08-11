package dir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestFetchPinsGenerationAcrossStoreInstances: a fetched generation must
// survive a concurrent re-publish from another store instance (another
// process on a shared mount) — retention replaces the old cross-process lock.
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
	mustPublish(t, writer, id, "first")

	dir, meta, release, err := reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if string(meta) != metaJSON("first") {
		t.Fatalf("fetch meta %q, want first generation", meta)
	}
	mustPublish(t, writer, id, "second")
	if _, statErr := os.Stat(filepath.Join(dir, "disk.img")); statErr != nil {
		t.Fatalf("first generation disturbed by re-publish: %v", statErr)
	}
	release()

	_, meta, release, err = reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch second: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("second") {
		t.Fatalf("fetch meta %q, want second generation", meta)
	}
}

// TestPublishRetriesIntoInstalledGeneration: a crash between the generation
// install and the meta commit leaves the generation dir in place; retrying
// the publish with identical meta bytes must converge, not fail on the rename.
func TestPublishRetriesIntoInstalledGeneration(t *testing.T) {
	const id = "ck_00000000000000aa"
	st, err := New(t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mustPublish(t, st, id, "same")
	mustPublish(t, st, id, "same")

	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch after retried publish: %v", err)
	}
	defer release()
	if _, err := os.Stat(filepath.Join(dir, "disk.img")); err != nil {
		t.Fatalf("export after retried publish: %v", err)
	}
}

// TestFetchLegacyFlatLayout: records published before per-generation dirs
// keep a flat <id>/export; Fetch falls back to it, and a re-publish moves the
// record onto the generation layout.
func TestFetchLegacyFlatLayout(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id), "legacy")

	dir, meta, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch legacy: %v", err)
	}
	release()
	if string(meta) != metaJSON("legacy") || dir != filepath.Join(root, id, store.ExportDir) {
		t.Fatalf("legacy fetch = %q %q, want the flat export dir", dir, meta)
	}

	mustPublish(t, st, id, "modern")
	dir, meta, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch after re-publish: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("modern") || dir == filepath.Join(root, id, store.ExportDir) {
		t.Fatalf("re-published fetch = %q %q, want a generation dir", dir, meta)
	}
}

// TestSweepReclaimsSupersededGenerations: a superseded generation survives
// while the last publish is inside the grace, and is reclaimed once the
// current meta is older than the grace.
func TestSweepReclaimsSupersededGenerations(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	gen1 := filepath.Join(root, id, store.ExportGen([]byte(metaJSON("first"))))
	mustPublish(t, st, id, "first")
	mustPublish(t, st, id, "second")

	if sweepErr := st.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep inside grace: %v", sweepErr)
	}
	if _, statErr := os.Stat(gen1); statErr != nil {
		t.Fatalf("superseded generation reclaimed inside the grace: %v", statErr)
	}

	backdate(t, filepath.Join(root, id, store.MetaFile))
	if sweepErr := st.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep past grace: %v", sweepErr)
	}
	if _, statErr := os.Stat(gen1); !os.IsNotExist(statErr) {
		t.Fatalf("superseded generation survived the graced sweep: %v", statErr)
	}
	dir, meta, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch after sweep: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("second") {
		t.Fatalf("fetch meta %q, want the current generation", meta)
	}
	if _, err := os.Stat(filepath.Join(dir, "disk.img")); err != nil {
		t.Fatalf("current export after sweep: %v", err)
	}
}

// TestSweepSparesLegacyFallback: the flat export dir of a never-re-published
// legacy record is the live data — the graced sweep must not touch it, however
// old the meta is. Once a re-publish supersedes it, it is reclaimed.
func TestSweepSparesLegacyFallback(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	seedRecord(t, filepath.Join(root, id), "legacy")
	backdate(t, filepath.Join(root, id, store.MetaFile))

	if err := st.SweepStaging(); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, _, release, err := st.Fetch(t.Context(), id); err != nil {
		t.Fatalf("legacy record swept while still current: %v", err)
	} else {
		release()
	}

	mustPublish(t, st, id, "modern")
	backdate(t, filepath.Join(root, id, store.MetaFile))
	if err := st.SweepStaging(); err != nil {
		t.Fatalf("sweep after re-publish: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id, store.ExportDir)); !os.IsNotExist(err) {
		t.Fatalf("superseded legacy export survived the graced sweep: %v", err)
	}
}

// TestSweepReclaimsUncommittedRecord: a record dir with no committed meta is a
// crashed first publish — reclaimed only past the grace, because another node
// sharing the root may be mid-publish right now.
func TestSweepReclaimsUncommittedRecord(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, id, store.ExportDir+"-0011223344556677"), 0o750); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.SweepStaging(); err != nil {
		t.Fatalf("sweep fresh: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id)); err != nil {
		t.Fatalf("fresh uncommitted record swept: %v", err)
	}

	backdate(t, filepath.Join(root, id))
	if err := st.SweepStaging(); err != nil {
		t.Fatalf("sweep old: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("uncommitted crash residue survived the graced sweep: %v", err)
	}
}

// TestPublishReclaimsCrashResidue: publishing over an uncommitted record dir
// older than the grace reaps the residue in-line and still installs cleanly.
func TestPublishReclaimsCrashResidue(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	residue := filepath.Join(root, id, store.ExportDir+"-0011223344556677")
	if mkErr := os.MkdirAll(residue, 0o750); mkErr != nil {
		t.Fatalf("seed: %v", mkErr)
	}
	backdate(t, filepath.Join(root, id))

	mustPublish(t, st, id, "fresh")
	_, meta, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	release()
	if string(meta) != metaJSON("fresh") {
		t.Fatalf("fetch meta %q, want the fresh publish", meta)
	}
	if _, statErr := os.Stat(residue); !os.IsNotExist(statErr) {
		t.Errorf("crash residue survived the publish: %v", statErr)
	}
}

// TestDeleteRemovesLegacyResidue: Delete clears a pre-generation <id>.old
// crash artifact along with the record, and is idempotent on a missing id.
func TestDeleteRemovesLegacyResidue(t *testing.T) {
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
	if _, err := st.ReadMeta(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("ReadMeta after Delete: %v, want store.ErrNotFound", err)
	}
	if _, err := os.Stat(filepath.Join(root, id+oldSuffix)); !os.IsNotExist(err) {
		t.Errorf("legacy .old residue survived Delete: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Errorf("Delete of a missing record: %v, want nil", err)
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

func metaJSON(metaID string) string {
	return `{"id":"` + metaID + `"}`
}

// mustPublish stages and publishes a one-file export whose meta is
// metaJSON(metaID).
func mustPublish(t *testing.T, st *Store, id, metaID string) {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage %s: %v", metaID, err)
	}
	seedRecord(t, staging, metaID)
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish %s: %v", metaID, err)
	}
}

func seedRecord(t *testing.T, dir, metaID string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.ExportDir, "disk.img"), []byte(metaID), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), []byte(metaJSON(metaID)), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
}

func backdate(t *testing.T, path string) {
	t.Helper()
	old := time.Now().Add(-2 * generationGrace)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("backdate %s: %v", path, err)
	}
}
