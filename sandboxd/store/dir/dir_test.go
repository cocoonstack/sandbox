package dir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

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
	backdate(t, filepath.Join(root, id, store.ExportGen([]byte(metaJSON("first")))))
	backdate(t, filepath.Join(root, id, store.MetaFile))

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

// TestPublishRetriesAfterExpiredInstall: an expired uncommitted generation is
// never revived; GC then retry converges, and the committed retry is idempotent.
func TestPublishRetriesAfterExpiredInstall(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, gen := installUncommitted(t, st, root, id, "same")
	backdate(t, gen)

	retry, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage retry: %v", err)
	}
	seedRecord(t, retry, "same")
	if publishErr := st.Publish(t.Context(), retry, id); publishErr == nil {
		t.Fatal("expired installed generation was revived")
	}
	if removeErr := os.RemoveAll(retry); removeErr != nil {
		t.Fatalf("remove failed retry: %v", removeErr)
	}
	if _, readErr := st.ReadMeta(t.Context(), id); !errors.Is(readErr, store.ErrNotFound) {
		t.Fatalf("expired retry committed meta: %v", readErr)
	}
	if sweepErr := st.SweepGenerations(); sweepErr != nil {
		t.Fatalf("sweep expired generation: %v", sweepErr)
	}
	if _, statErr := os.Stat(gen); !os.IsNotExist(statErr) {
		t.Fatalf("expired generation survived sweep: %v", statErr)
	}

	mustPublish(t, st, id, "same")
	backdate(t, gen)
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

// TestPublishSweepsGenerationsBySupersessionAge: rolling re-promotes reclaim
// generations outside their own grace without disturbing newer generations.
func TestPublishSweepsGenerationsBySupersessionAge(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	gen1 := filepath.Join(root, id, store.ExportGen([]byte(metaJSON("first"))))
	gen2 := filepath.Join(root, id, store.ExportGen([]byte(metaJSON("second"))))
	mustPublish(t, st, id, "first")
	mustPublish(t, st, id, "second")
	mustPublish(t, st, id, "third")
	if _, statErr := os.Stat(gen1); statErr != nil {
		t.Fatalf("superseded generation reclaimed inside the grace: %v", statErr)
	}

	setAge(t, gen1, generationGrace+time.Minute)
	setAge(t, gen2, generationGrace/2)
	mustPublish(t, st, id, "fourth")
	if _, statErr := os.Stat(gen1); !os.IsNotExist(statErr) {
		t.Fatalf("generation past its supersession grace survived publish: %v", statErr)
	}
	if _, statErr := os.Stat(gen2); statErr != nil {
		t.Fatalf("generation inside its supersession grace was reclaimed: %v", statErr)
	}
	dir, meta, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch after sweep: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("fourth") {
		t.Fatalf("fetch meta %q, want the current generation", meta)
	}
	if _, err := os.Stat(filepath.Join(dir, "disk.img")); err != nil {
		t.Fatalf("current export after sweep: %v", err)
	}
}

// TestSweepSparesPublishingGeneration: a peer sweep cannot delete a fresh
// generation between its install and meta commit.
func TestSweepSparesPublishingGeneration(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	sweeper, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new sweeper: %v", err)
	}
	writer, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	mustPublish(t, writer, id, "old")
	backdate(t, filepath.Join(root, id, store.MetaFile))

	meta, gen := installUncommitted(t, writer, root, id, "new")
	if sweepErr := sweeper.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep during publish: %v", sweepErr)
	}
	if _, statErr := os.Stat(gen); statErr != nil {
		t.Fatalf("publishing generation was swept: %v", statErr)
	}

	tmp := filepath.Join(root, id, store.MetaFile+".tmp")
	if writeErr := os.WriteFile(tmp, meta, 0o600); writeErr != nil {
		t.Fatalf("write commit: %v", writeErr)
	}
	if renameErr := os.Rename(tmp, filepath.Join(root, id, store.MetaFile)); renameErr != nil {
		t.Fatalf("commit meta: %v", renameErr)
	}
	dir, gotMeta, release, err := writer.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch committed generation: %v", err)
	}
	defer release()
	if string(gotMeta) != metaJSON("new") {
		t.Fatalf("fetch meta %q, want new generation", gotMeta)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "new" {
		t.Fatalf("fetch export %q, %v, want new generation", got, readErr)
	}
}

// TestSweepSparesLegacyFallback: the flat export dir of a never-re-published
// legacy record is live data; once a re-publish supersedes it, grace applies.
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

	backdate(t, filepath.Join(root, id, store.ExportDir))
	mustPublish(t, st, id, "modern")
	if _, err := os.Stat(filepath.Join(root, id, store.ExportDir)); err != nil {
		t.Fatalf("legacy export reclaimed inside the grace: %v", err)
	}
	backdate(t, filepath.Join(root, id, store.ExportDir))
	if err := st.SweepStaging(); err != nil {
		t.Fatalf("sweep after re-publish: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, id, store.ExportDir)); !os.IsNotExist(err) {
		t.Fatalf("superseded legacy export survived the graced sweep: %v", err)
	}
}

// TestSweepReclaimsOnlyAgedUncommittedGenerations: an abandoned generation
// is reclaimed across ids while a peer's uncommitted publish remains intact.
func TestSweepReclaimsOnlyAgedUncommittedGenerations(t *testing.T) {
	const (
		orphanID  = "ck_00000000000000aa"
		publishID = "ck_00000000000000bb"
	)
	root := t.TempDir()
	sweeper, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new sweeper: %v", err)
	}
	writer, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	orphan := filepath.Join(root, orphanID, store.ExportDir+"-0011223344556677")
	if mkdirErr := os.MkdirAll(orphan, 0o750); mkdirErr != nil {
		t.Fatalf("seed orphan: %v", mkdirErr)
	}
	backdate(t, orphan)

	staging, err := writer.Stage(publishID)
	if err != nil {
		t.Fatalf("stage publishing generation: %v", err)
	}
	seedRecord(t, staging, "fresh")
	meta, err := os.ReadFile(filepath.Join(staging, store.MetaFile))
	if err != nil {
		t.Fatalf("read staged meta: %v", err)
	}
	backdate(t, filepath.Join(staging, store.ExportDir))
	commitBlocker := filepath.Join(root, publishID, store.MetaFile+".tmp")
	if mkdirErr := os.MkdirAll(commitBlocker, 0o750); mkdirErr != nil {
		t.Fatalf("block meta commit: %v", mkdirErr)
	}
	if publishErr := writer.Publish(t.Context(), staging, publishID); publishErr == nil {
		t.Fatal("publish with blocked meta commit succeeded")
	}
	fresh := filepath.Join(root, publishID, store.ExportGen(meta))

	if sweepErr := sweeper.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep old: %v", sweepErr)
	}
	if _, statErr := os.Stat(orphan); !os.IsNotExist(statErr) {
		t.Fatalf("aged uncommitted generation survived: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(root, orphanID)); statErr != nil {
		t.Fatalf("orphan record shell was removed: %v", statErr)
	}
	if _, statErr := os.Stat(fresh); statErr != nil {
		t.Fatalf("publishing generation was swept: %v", statErr)
	}

	if removeErr := os.RemoveAll(commitBlocker); removeErr != nil {
		t.Fatalf("remove commit blocker: %v", removeErr)
	}
	mustPublish(t, writer, publishID, "fresh")
	dir, gotMeta, release, err := writer.Fetch(t.Context(), publishID)
	if err != nil {
		t.Fatalf("fetch retried publish: %v", err)
	}
	defer release()
	if string(gotMeta) != metaJSON("fresh") {
		t.Fatalf("fetch meta %q, want fresh", gotMeta)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "fresh" {
		t.Fatalf("fetch export %q, %v, want fresh", got, readErr)
	}
}

// TestSweepGenerationsToleratesConcurrentDelete: a record deleted mid-sweep
// must not fail the pass — the first error aborts every remaining record.
func TestSweepGenerationsToleratesConcurrentDelete(t *testing.T) {
	const id = "ck_00000000000000aa"
	st, err := New(t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := t.Context()
	done := make(chan struct{})
	var g errgroup.Group
	g.Go(func() error {
		defer close(done)
		for range 500 {
			staging, err := st.Stage(id)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(metaJSON(id)), 0o600); err != nil {
				return err
			}
			if err := st.Publish(ctx, staging, id); err != nil {
				return fmt.Errorf("publish: %w", err)
			}
			if err := st.Delete(ctx, id); err != nil {
				return fmt.Errorf("delete: %w", err)
			}
		}
		return nil
	})
	g.Go(func() error {
		for {
			select {
			case <-done:
				return nil
			default:
			}
			if err := st.SweepGenerations(); err != nil {
				return fmt.Errorf("sweep: %w", err)
			}
		}
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
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

// installUncommitted installs metaID's generation without committing
// meta.json — the state a crash between install and commit leaves.
func installUncommitted(t *testing.T, st *Store, root, id, metaID string) (meta []byte, gen string) {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage %s: %v", metaID, err)
	}
	seedRecord(t, staging, metaID)
	meta, err = os.ReadFile(filepath.Join(staging, store.MetaFile))
	if err != nil {
		t.Fatalf("read staged meta: %v", err)
	}
	final := filepath.Join(root, id)
	if err := os.MkdirAll(final, 0o750); err != nil {
		t.Fatalf("create record dir: %v", err)
	}
	gen = filepath.Join(final, store.ExportGen(meta))
	if err := os.Rename(filepath.Join(staging, store.ExportDir), gen); err != nil {
		t.Fatalf("install generation: %v", err)
	}
	if err := os.RemoveAll(staging); err != nil {
		t.Fatalf("remove crashed staging: %v", err)
	}
	return meta, gen
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
	setAge(t, path, 2*generationGrace)
}

func setAge(t *testing.T, path string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("backdate %s: %v", path, err)
	}
}
