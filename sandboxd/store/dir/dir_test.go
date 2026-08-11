package dir

import (
	"bytes"
	"crypto/sha256"
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
	firstMeta := []byte(metaJSON("first"))
	firstDigest := mustPublishDigested(t, writer, id, "first")
	firstDigestPath := filepath.Join(root, id, digestName(firstMeta))
	backdate(t, filepath.Join(root, id, store.ExportGen(firstMeta)))
	backdate(t, firstDigestPath)
	backdate(t, filepath.Join(root, id, store.MetaFile))

	dir, meta, digest, release, err := reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch first: %v", err)
	}
	if string(meta) != metaJSON("first") {
		t.Fatalf("fetch meta %q, want first generation", meta)
	}
	if digest != firstDigest {
		t.Fatalf("fetch digest %q, want first generation %q", digest, firstDigest)
	}
	secondDigest := mustPublishDigested(t, writer, id, "second")
	if _, statErr := os.Stat(filepath.Join(dir, "disk.img")); statErr != nil {
		t.Fatalf("first generation disturbed by re-publish: %v", statErr)
	}
	if _, statErr := os.Stat(firstDigestPath); statErr != nil {
		t.Fatalf("first generation digest disturbed by re-publish: %v", statErr)
	}
	release()

	_, meta, digest, release, err = reader.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch second: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("second") {
		t.Fatalf("fetch meta %q, want second generation", meta)
	}
	if digest != secondDigest || digest == firstDigest {
		t.Fatalf("fetch digest %q, want second generation %q", digest, secondDigest)
	}
}

func TestPlainPublishReplacesDigestWithEmpty(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	firstMeta := []byte(metaJSON("digested"))
	firstDigest := mustPublishDigested(t, st, id, "digested")
	firstDigestPath := filepath.Join(root, id, digestName(firstMeta))
	mustPublish(t, st, id, "plain")

	_, meta, digest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("plain") || digest != "" {
		t.Fatalf("plain replacement meta/digest = %q/%q, want plain/empty", meta, digest)
	}
	if firstDigest == "" {
		t.Fatal("digested generation returned an empty digest")
	}
	if _, statErr := os.Stat(firstDigestPath); statErr != nil {
		t.Fatalf("superseded digest removed inside the grace: %v", statErr)
	}
}

func TestPublishDigestedChunkBoundaries(t *testing.T) {
	const id = "ck_00000000000000aa"
	content := bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize)+1)
	content[len(content)-1] = 'y'
	tests := []struct {
		name string
		size int64
		want string
	}{
		{name: "below", size: store.DigestChunkSize - 1, want: "sha256:9463427f8626e0b65863b401d333d32ad8e0b11efb34db7f502798aea753fc7e"},
		{name: "exact", size: store.DigestChunkSize, want: "sha256:20ef154fac2fa87246572b15e008adfef78fc47ce93e479c367112ea750006c6"},
		{name: "above", size: store.DigestChunkSize + 1, want: "sha256:da8dd289e7d66408bd7d202642f180f2c22bc508cf0fe1d6e9f0e0b7967f6a5e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := New(t.TempDir(), store.CheckpointIDRe)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			staging, err := st.Stage(id)
			if err != nil {
				t.Fatalf("Stage: %v", err)
			}
			export := filepath.Join(staging, store.ExportDir)
			if err = os.MkdirAll(export, 0o750); err != nil {
				t.Fatalf("mkdir export: %v", err)
			}
			if err = os.WriteFile(filepath.Join(export, "boundary.bin"), content[:int(tt.size)], 0o600); err != nil {
				t.Fatalf("write export: %v", err)
			}
			if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(metaJSON(tt.name)), 0o600); err != nil {
				t.Fatalf("write meta: %v", err)
			}
			digest, err := st.PublishDigested(t.Context(), staging, id)
			if err != nil {
				t.Fatalf("PublishDigested: %v", err)
			}
			if digest != tt.want {
				t.Errorf("digest = %q, want %q", digest, tt.want)
			}
			_, _, fetched, release, err := st.Fetch(t.Context(), id)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			defer release()
			if fetched != tt.want {
				t.Errorf("fetched digest = %q, want %q", fetched, tt.want)
			}
		})
	}
}

func TestDigestWorkersStoreChunksByOffset(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize))
	data = append(data, 'y')
	path := filepath.Join(t.TempDir(), "boundary.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	sources := []digestSource{{
		path: path,
		digest: store.DigestFile{
			Path: "boundary.bin", Size: int64(len(data)), Chunks: make([][sha256.Size]byte, 2),
		},
	}}
	secondDone := make(chan struct{})
	completed := make(chan int, 2)
	hashFn := func(source *digestSource, chunk int, buffer []byte) error {
		if chunk == 0 {
			<-secondDone
		}
		err := hashChunk(source, chunk, buffer)
		completed <- chunk
		if chunk == 1 {
			close(secondDone)
		}
		return err
	}
	if err := hashChunks(t.Context(), sources, 2, hashFn); err != nil {
		t.Fatalf("hash chunks: %v", err)
	}
	if first, second := <-completed, <-completed; first != 1 || second != 0 {
		t.Fatalf("completion order = %d, %d, want 1, 0", first, second)
	}
	digest, err := store.AssembleDigest([]store.DigestFile{sources[0].digest})
	if err != nil {
		t.Fatalf("assemble digest: %v", err)
	}
	const want = "sha256:da8dd289e7d66408bd7d202642f180f2c22bc508cf0fe1d6e9f0e0b7967f6a5e"
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
}

func TestPublishDigestedFailureAndFreshRetry(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	firstDigest := mustPublishDigested(t, st, id, "first")

	failed, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage failed publish: %v", err)
	}
	seedRecord(t, failed, "second")
	secondMeta, err := os.ReadFile(filepath.Join(failed, store.MetaFile))
	if err != nil {
		t.Fatalf("read second meta: %v", err)
	}
	blocker := filepath.Join(root, id, digestName(secondMeta))
	if err = os.Mkdir(blocker, 0o750); err != nil {
		t.Fatalf("block digest commit: %v", err)
	}
	if digest, publishErr := st.PublishDigested(t.Context(), failed, id); publishErr == nil || digest != "" {
		t.Fatalf("PublishDigested with blocked sidecar = %q, %v, want empty/error", digest, publishErr)
	}
	dir, meta, digest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch previous generation: %v", err)
	}
	content, readErr := os.ReadFile(filepath.Join(dir, "disk.img"))
	release()
	if string(meta) != metaJSON("first") || digest != firstDigest {
		t.Errorf("previous generation meta/digest = %q/%q, want first/%q", meta, digest, firstDigest)
	}
	if readErr != nil || string(content) != "first" {
		t.Errorf("previous generation content = %q, %v, want first", content, readErr)
	}

	if err = os.RemoveAll(blocker); err != nil {
		t.Fatalf("remove digest blocker: %v", err)
	}
	if err = os.RemoveAll(failed); err != nil {
		t.Fatalf("remove failed staging: %v", err)
	}
	secondDigest := mustPublishDigested(t, st, id, "second")
	dir, meta, digest, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch replacement generation: %v", err)
	}
	defer release()
	content, readErr = os.ReadFile(filepath.Join(dir, "disk.img"))
	if string(meta) != metaJSON("second") || digest != secondDigest || digest == firstDigest {
		t.Errorf("replacement generation meta/digest = %q/%q, want second/%q", meta, digest, secondDigest)
	}
	if readErr != nil || string(content) != "second" {
		t.Errorf("replacement generation content = %q, %v, want second", content, readErr)
	}
}

func TestSweepGenerationsPairsCurrentAndSupersededDigests(t *testing.T) {
	const id = "ck_00000000000000aa"
	root := t.TempDir()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	firstMeta := []byte(metaJSON("first"))
	firstGen := filepath.Join(root, id, store.ExportGen(firstMeta))
	firstSidecar := filepath.Join(root, id, digestName(firstMeta))
	mustPublishDigested(t, st, id, "first")
	secondMeta := []byte(metaJSON("second"))
	secondGen := filepath.Join(root, id, store.ExportGen(secondMeta))
	secondSidecar := filepath.Join(root, id, digestName(secondMeta))
	secondDigest := mustPublishDigested(t, st, id, "second")
	for _, path := range []string{firstGen, firstSidecar, secondGen, secondSidecar} {
		backdate(t, path)
	}

	if err = st.SweepGenerations(); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	for _, path := range []string{firstGen, firstSidecar} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Errorf("superseded entry %s survived sweep: %v", filepath.Base(path), statErr)
		}
	}
	for _, path := range []string{secondGen, secondSidecar} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Errorf("current entry %s was swept: %v", filepath.Base(path), statErr)
		}
	}
	_, meta, digest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch current generation: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("second") || digest != secondDigest {
		t.Fatalf("current meta/digest = %q/%q, want second/%q", meta, digest, secondDigest)
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
	dir, _, _, release, err := st.Fetch(t.Context(), id)
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

	dir, meta, digest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch legacy: %v", err)
	}
	release()
	if string(meta) != metaJSON("legacy") || digest != "" || dir != filepath.Join(root, id, store.ExportDir) {
		t.Fatalf("legacy fetch = %q %q %q, want the flat export dir without digest", dir, meta, digest)
	}

	mustPublish(t, st, id, "modern")
	dir, meta, digest, release, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch after re-publish: %v", err)
	}
	defer release()
	if string(meta) != metaJSON("modern") || digest != "" || dir == filepath.Join(root, id, store.ExportDir) {
		t.Fatalf("re-published fetch = %q %q %q, want a generation dir without digest", dir, meta, digest)
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
	dir, meta, _, release, err := st.Fetch(t.Context(), id)
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
	dir, gotMeta, digest, release, err := writer.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch committed generation: %v", err)
	}
	defer release()
	if string(gotMeta) != metaJSON("new") {
		t.Fatalf("fetch meta %q, want new generation", gotMeta)
	}
	if digest != "" {
		t.Fatalf("fetch digest %q, want empty", digest)
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
	if _, _, _, release, err := st.Fetch(t.Context(), id); err != nil {
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
	dir, gotMeta, digest, release, err := writer.Fetch(t.Context(), publishID)
	if err != nil {
		t.Fatalf("fetch retried publish: %v", err)
	}
	defer release()
	if string(gotMeta) != metaJSON("fresh") {
		t.Fatalf("fetch meta %q, want fresh", gotMeta)
	}
	if digest != "" {
		t.Fatalf("fetch digest %q, want empty", digest)
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

func mustPublishDigested(t *testing.T, st *Store, id, metaID string) string {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage %s: %v", metaID, err)
	}
	seedRecord(t, staging, metaID)
	digest, err := st.PublishDigested(t.Context(), staging, id)
	if err != nil {
		t.Fatalf("publish digested %s: %v", metaID, err)
	}
	return digest
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
