package dir

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

const testID = "ck_00000000000000aa"

func TestDirBackendContract(t *testing.T) {
	st := newStore(t, t.TempDir())
	storetest.RunContract(t, st)
}

func TestFetchPinsGenerationAcrossStoreInstances(t *testing.T) {
	root := t.TempDir()
	reader := newStore(t, root)
	writer := newStore(t, root)
	firstMeta := []byte(storetest.MetaJSON("first"))
	firstDigest := mustPublishDigested(t, writer, testID, "first")
	firstDigestPath := filepath.Join(root, testID, digestName(firstMeta))
	backdate(t, filepath.Join(root, testID, store.ExportGen(firstMeta)))
	backdate(t, firstDigestPath)
	backdate(t, filepath.Join(root, testID, store.MetaFile))

	dir, meta, digest := mustFetch(t, reader, testID)
	if string(meta) != storetest.MetaJSON("first") {
		t.Fatalf("fetch meta %q, want first generation", meta)
	}
	if digest != firstDigest {
		t.Fatalf("fetch digest %q, want first generation %q", digest, firstDigest)
	}
	secondDigest := mustPublishDigested(t, writer, testID, "second")
	assertPresentf(t, filepath.Join(dir, "disk.img"), "first generation disturbed by re-publish")
	assertPresentf(t, firstDigestPath, "first generation digest disturbed by re-publish")

	_, meta, digest = mustFetch(t, reader, testID)
	if string(meta) != storetest.MetaJSON("second") {
		t.Fatalf("fetch meta %q, want second generation", meta)
	}
	if digest != secondDigest || digest == firstDigest {
		t.Fatalf("fetch digest %q, want second generation %q", digest, secondDigest)
	}
}

func TestPlainPublishReplacesDigestWithEmpty(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	firstMeta := []byte(storetest.MetaJSON("digested"))
	firstDigest := mustPublishDigested(t, st, testID, "digested")
	firstDigestPath := filepath.Join(root, testID, digestName(firstMeta))
	mustPublish(t, st, testID, "plain")

	_, meta, digest := mustFetch(t, st, testID)
	if string(meta) != storetest.MetaJSON("plain") || digest != "" {
		t.Fatalf("plain replacement meta/digest = %q/%q, want plain/empty", meta, digest)
	}
	if firstDigest == "" {
		t.Fatal("digested generation returned an empty digest")
	}
	assertPresentf(t, firstDigestPath, "superseded digest removed inside the grace")
	mustPublish(t, st, testID, "digested")
	if _, _, digest = mustFetch(t, st, testID); digest != "" {
		t.Errorf("a plain publish of the digested meta kept digest %q, want empty", digest)
	}
}

func TestPublishDigestedChunkBoundaries(t *testing.T) {
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
			st := newStore(t, t.TempDir())
			staging, err := st.Stage(testID)
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
			if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(storetest.MetaJSON(tt.name)), 0o600); err != nil {
				t.Fatalf("write meta: %v", err)
			}
			digest, err := storetest.PublishDigested(t.Context(), st, staging, testID)
			if err != nil {
				t.Fatalf("PublishDigested: %v", err)
			}
			if digest != tt.want {
				t.Errorf("digest = %q, want %q", digest, tt.want)
			}
			_, _, fetched := mustFetch(t, st, testID)
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

func TestHoleyExportDigestEqualsTheReadEverythingDigest(t *testing.T) {
	root := t.TempDir()
	dataTail := storetest.WriteSparse(t, filepath.Join(root, "disk.img"), 3*store.DigestChunkSize+7, map[int64][]byte{
		store.DigestChunkSize + 5:   []byte("data"),
		2 * store.DigestChunkSize:   make([]byte, 4096),
		3*store.DigestChunkSize + 6: []byte("z"),
	})
	storetest.WriteSparse(t, filepath.Join(root, "mem"), 2*store.DigestChunkSize+5, map[int64][]byte{0: []byte("head")})
	f, err := os.Open(dataTail)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, hole := store.HoleSum(f, store.DigestChunkSize, store.DigestChunkSize); hole {
		t.Fatal("a chunk holding data reads as a hole")
	}
	if _, hole := store.HoleSum(f, 0, store.DigestChunkSize); !hole {
		t.Fatal("an unwritten chunk does not read as a hole; the filesystem lacks SEEK_DATA")
	}
	if _, hole := store.HoleSum(f, 2*store.DigestChunkSize, store.DigestChunkSize); hole {
		t.Fatal("a chunk of written zeros reads as a hole")
	}

	got, err := hashExport(t.Context(), root)
	if err != nil {
		t.Fatalf("hashExport: %v", err)
	}
	if want := readEverythingDigest(t, root); got != want {
		t.Errorf("hole-skipping digest %q, want the read-everything digest %q", got, want)
	}
}

func TestPublishDigestedFailureAndFreshRetry(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	firstDigest := mustPublishDigested(t, st, testID, "first")

	failed := stagedRecord(t, st, testID, "second")
	secondMeta, err := os.ReadFile(filepath.Join(failed, store.MetaFile))
	if err != nil {
		t.Fatalf("read second meta: %v", err)
	}
	blocker := filepath.Join(root, testID, digestName(secondMeta))
	if err = os.Mkdir(blocker, 0o750); err != nil {
		t.Fatalf("block digest commit: %v", err)
	}
	if digest, publishErr := storetest.PublishDigested(t.Context(), st, failed, testID); publishErr == nil || digest != "" {
		t.Fatalf("PublishDigested with blocked sidecar = %q, %v, want empty/error", digest, publishErr)
	}
	dir, meta, digest := mustFetch(t, st, testID)
	content, readErr := os.ReadFile(filepath.Join(dir, "disk.img"))
	if string(meta) != storetest.MetaJSON("first") || digest != firstDigest {
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
	secondDigest := mustPublishDigested(t, st, testID, "second")
	dir, meta, digest = mustFetch(t, st, testID)
	content, readErr = os.ReadFile(filepath.Join(dir, "disk.img"))
	if string(meta) != storetest.MetaJSON("second") || digest != secondDigest || digest == firstDigest {
		t.Errorf("replacement generation meta/digest = %q/%q, want second/%q", meta, digest, secondDigest)
	}
	if readErr != nil || string(content) != "second" {
		t.Errorf("replacement generation content = %q, %v, want second", content, readErr)
	}
}

func TestSweepGenerationsPairsCurrentAndSupersededDigests(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	firstMeta := []byte(storetest.MetaJSON("first"))
	firstGen := filepath.Join(root, testID, store.ExportGen(firstMeta))
	firstSidecar := filepath.Join(root, testID, digestName(firstMeta))
	mustPublishDigested(t, st, testID, "first")
	secondMeta := []byte(storetest.MetaJSON("second"))
	secondGen := filepath.Join(root, testID, store.ExportGen(secondMeta))
	secondSidecar := filepath.Join(root, testID, digestName(secondMeta))
	secondDigest := mustPublishDigested(t, st, testID, "second")
	if err := st.SetLabels(t.Context(), testID, []byte(`{"tag":"v1"}`)); err != nil {
		t.Fatalf("SetLabels: %v", err)
	}
	labels := filepath.Join(root, testID, store.LabelsFile)
	for _, path := range []string{firstGen, firstSidecar, secondGen, secondSidecar, labels} {
		backdate(t, path)
	}

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	for _, path := range []string{firstGen, firstSidecar} {
		assertGonef(t, path, "superseded entry %s survived sweep", filepath.Base(path))
	}
	for _, path := range []string{secondGen, secondSidecar, labels} {
		assertPresentf(t, path, "current entry %s was swept", filepath.Base(path))
	}
	_, meta, digest := mustFetch(t, st, testID)
	if string(meta) != storetest.MetaJSON("second") || digest != secondDigest {
		t.Fatalf("current meta/digest = %q/%q, want second/%q", meta, digest, secondDigest)
	}
}

func TestPublishRetriesAfterExpiredInstall(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	_, gen := installUncommitted(t, st, root, testID, "same")
	backdate(t, gen)

	retry := stagedRecord(t, st, testID, "same")
	if publishErr := st.Publish(t.Context(), retry, testID, ""); publishErr == nil {
		t.Fatal("expired installed generation was revived")
	}
	if removeErr := os.RemoveAll(retry); removeErr != nil {
		t.Fatalf("remove failed retry: %v", removeErr)
	}
	if _, readErr := st.ReadMeta(t.Context(), testID); !errors.Is(readErr, store.ErrNotFound) {
		t.Fatalf("expired retry committed meta: %v", readErr)
	}
	if sweepErr := st.SweepGenerations(t.Context()); sweepErr != nil {
		t.Fatalf("sweep expired generation: %v", sweepErr)
	}
	assertGonef(t, gen, "expired generation survived sweep")

	mustPublish(t, st, testID, "same")
	backdate(t, gen)
	mustPublish(t, st, testID, "same")
	dir, _, _ := mustFetch(t, st, testID)
	assertPresentf(t, filepath.Join(dir, "disk.img"), "export after retried publish")
}

func TestPublishSweepsGenerationsBySupersessionAge(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	gen1 := filepath.Join(root, testID, store.ExportGen([]byte(storetest.MetaJSON("first"))))
	gen2 := filepath.Join(root, testID, store.ExportGen([]byte(storetest.MetaJSON("second"))))
	mustPublish(t, st, testID, "first")
	mustPublish(t, st, testID, "second")
	mustPublish(t, st, testID, "third")
	assertPresentf(t, gen1, "superseded generation reclaimed inside the grace")

	setAge(t, gen1, store.GenerationGrace+time.Minute)
	setAge(t, gen2, store.GenerationGrace/2)
	mustPublish(t, st, testID, "fourth")
	assertGonef(t, gen1, "generation past its supersession grace survived publish")
	assertPresentf(t, gen2, "generation inside its supersession grace was reclaimed")
	dir, meta, _ := mustFetch(t, st, testID)
	if string(meta) != storetest.MetaJSON("fourth") {
		t.Fatalf("fetch meta %q, want the current generation", meta)
	}
	assertPresentf(t, filepath.Join(dir, "disk.img"), "current export after sweep")
}

func TestSweepSparesPublishingGeneration(t *testing.T) {
	root := t.TempDir()
	sweeper := newStore(t, root)
	writer := newStore(t, root)
	mustPublish(t, writer, testID, "old")
	backdate(t, filepath.Join(root, testID, store.MetaFile))

	meta, gen := installUncommitted(t, writer, root, testID, "new")
	if sweepErr := sweeper.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep during publish: %v", sweepErr)
	}
	assertPresentf(t, gen, "publishing generation was swept")

	tmp := filepath.Join(root, testID, store.MetaFile+".tmp")
	if writeErr := os.WriteFile(tmp, meta, 0o600); writeErr != nil {
		t.Fatalf("write commit: %v", writeErr)
	}
	if renameErr := os.Rename(tmp, filepath.Join(root, testID, store.MetaFile)); renameErr != nil {
		t.Fatalf("commit meta: %v", renameErr)
	}
	dir, gotMeta, digest := mustFetch(t, writer, testID)
	if string(gotMeta) != storetest.MetaJSON("new") {
		t.Fatalf("fetch meta %q, want new generation", gotMeta)
	}
	if digest != "" {
		t.Fatalf("fetch digest %q, want empty", digest)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "new" {
		t.Fatalf("fetch export %q, %v, want new generation", got, readErr)
	}
}

func TestSweepReclaimsOnlyAgedUncommittedGenerations(t *testing.T) {
	const (
		orphanID  = "ck_00000000000000aa"
		publishID = "ck_00000000000000bb"
	)
	root := t.TempDir()
	sweeper := newStore(t, root)
	writer := newStore(t, root)
	orphan := filepath.Join(root, orphanID, store.ExportDir+"-0011223344556677")
	if mkdirErr := os.MkdirAll(orphan, 0o750); mkdirErr != nil {
		t.Fatalf("seed orphan: %v", mkdirErr)
	}
	backdate(t, orphan)

	staging := stagedRecord(t, writer, publishID, "fresh")
	meta, err := os.ReadFile(filepath.Join(staging, store.MetaFile))
	if err != nil {
		t.Fatalf("read staged meta: %v", err)
	}
	backdate(t, filepath.Join(staging, store.ExportDir))
	commitBlocker := filepath.Join(root, publishID, store.MetaFile+".tmp")
	if mkdirErr := os.MkdirAll(commitBlocker, 0o750); mkdirErr != nil {
		t.Fatalf("block meta commit: %v", mkdirErr)
	}
	if publishErr := writer.Publish(t.Context(), staging, publishID, ""); publishErr == nil {
		t.Fatal("publish with blocked meta commit succeeded")
	}
	fresh := filepath.Join(root, publishID, store.ExportGen(meta))

	if sweepErr := sweeper.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep old: %v", sweepErr)
	}
	assertGonef(t, orphan, "aged uncommitted generation survived")
	assertPresentf(t, filepath.Join(root, orphanID), "orphan record shell was removed")
	assertPresentf(t, fresh, "publishing generation was swept")

	if removeErr := os.RemoveAll(commitBlocker); removeErr != nil {
		t.Fatalf("remove commit blocker: %v", removeErr)
	}
	mustPublish(t, writer, publishID, "fresh")
	dir, gotMeta, digest := mustFetch(t, writer, publishID)
	if string(gotMeta) != storetest.MetaJSON("fresh") {
		t.Fatalf("fetch meta %q, want fresh", gotMeta)
	}
	if digest != "" {
		t.Fatalf("fetch digest %q, want empty", digest)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "fresh" {
		t.Fatalf("fetch export %q, %v, want fresh", got, readErr)
	}
}

func TestSweepStagingSparesAPeersFreshStaging(t *testing.T) {
	root := t.TempDir()
	peer := newStore(t, root)
	restarted := newStore(t, root)
	staging := stagedRecord(t, peer, testID, "in-progress")

	if sweepErr := restarted.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep: %v", sweepErr)
	}
	assertPresentf(t, filepath.Join(staging, store.MetaFile), "a peer's in-progress staging was swept")

	backdate(t, staging)
	if sweepErr := restarted.SweepStaging(); sweepErr != nil {
		t.Fatalf("sweep aged: %v", sweepErr)
	}
	assertGonef(t, staging, "aged staging survived")
}

func TestSweepGenerationsRemovesOnlyAgedStaging(t *testing.T) {
	st := newStore(t, t.TempDir())
	aged := stagedRecord(t, st, testID, "interrupted")
	backdate(t, aged)
	fresh, err := st.Stage("ck_00000000000000bb")
	if err != nil {
		t.Fatalf("stage: %v", err)
	}

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}

	assertGonef(t, aged, "staging aged past the grace survived the periodic sweep")
	assertPresentf(t, fresh, "fresh staging was swept")
}

func TestSweepGenerationsToleratesConcurrentDelete(t *testing.T) {
	st := newStore(t, t.TempDir())
	ctx := t.Context()
	done := make(chan struct{})
	var g errgroup.Group
	g.Go(func() error {
		defer close(done)
		for range 500 {
			staging, err := st.Stage(testID)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(storetest.MetaJSON(testID)), 0o600); err != nil {
				return err
			}
			if err := st.Publish(ctx, staging, testID, ""); err != nil {
				return fmt.Errorf("publish: %w", err)
			}
			if err := st.Delete(ctx, testID); err != nil {
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
			if err := st.SweepGenerations(ctx); err != nil {
				return fmt.Errorf("sweep: %w", err)
			}
		}
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestMetasSurfacesReadError(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)

	if err := os.MkdirAll(filepath.Join(root, testID, store.MetaFile), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := st.Metas(t.Context(), ""); err == nil {
		t.Error("Metas swallowed an unreadable meta; want the error surfaced")
	}
}

func TestMetasReadsEveryRecordAndSkipsOnesWithoutMeta(t *testing.T) {
	root := t.TempDir()
	st := newStore(t, root)
	want := map[string]string{}
	for i := range 100 {
		id := fmt.Sprintf("ck_%016x", i)
		mustPublish(t, st, id, id)
		want[storetest.MetaJSON(id)] = ""
		if i%3 == 0 {
			labels := `{"n":"` + id + `"}`
			if err := st.SetLabels(t.Context(), id, []byte(labels)); err != nil {
				t.Fatalf("SetLabels: %v", err)
			}
			want[storetest.MetaJSON(id)] = labels
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "ck_00000000000fffff"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := st.Stage("ck_0000000000000000"); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	recs, err := st.Metas(t.Context(), "")
	if err != nil {
		t.Fatalf("Metas: %v", err)
	}
	got := map[string]string{}
	for _, r := range recs {
		got[string(r.Meta)] = string(r.Labels)
	}
	if len(recs) != len(want) || !maps.Equal(got, want) {
		t.Errorf("Metas returned %d records, want the %d published ones with their labels", len(recs), len(want))
	}
}

func BenchmarkMetas(b *testing.B) {
	st := newStore(b, b.TempDir())
	for i := range 2000 {
		id := fmt.Sprintf("ck_%016x", i)
		mustPublish(b, st, id, id)
	}
	b.ReportAllocs()
	for b.Loop() {
		if recs, err := st.Metas(b.Context(), ""); err != nil || len(recs) != 2000 {
			b.Fatalf("Metas: %d, %v", len(recs), err)
		}
	}
}

func mustFetch(t *testing.T, st *Store, id string) (dir string, meta []byte, digest string) {
	t.Helper()
	dir, meta, digest, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch %s: %v", id, err)
	}
	return dir, meta, digest
}

func newStore(t testing.TB, root string) *Store {
	t.Helper()
	st, err := New(root, store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return st
}

func readEverythingDigest(t testing.TB, root string) string {
	t.Helper()
	sources, err := collectDigestSources(root)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	readAll := func(source *digestSource, chunk int, buffer []byte) error {
		f, openErr := os.Open(source.path)
		if openErr != nil {
			return openErr
		}
		defer func() { _ = f.Close() }()
		offset := int64(chunk) * store.DigestChunkSize
		hash := sha256.New()
		if _, copyErr := io.CopyBuffer(hash, io.NewSectionReader(f, offset, min(store.DigestChunkSize, source.digest.Size-offset)), buffer); copyErr != nil {
			return copyErr
		}
		copy(source.digest.Chunks[chunk][:], hash.Sum(nil))
		return nil
	}
	if err = hashChunks(t.Context(), sources, min(runtime.GOMAXPROCS(0), digestWorkerLimit), readAll); err != nil {
		t.Fatalf("read every chunk: %v", err)
	}
	files := make([]store.DigestFile, len(sources))
	for i := range sources {
		files[i] = sources[i].digest
	}
	digest, err := store.AssembleDigest(files)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	return digest
}

func installUncommitted(t *testing.T, st *Store, root, id, metaID string) (meta []byte, gen string) {
	t.Helper()
	staging := stagedRecord(t, st, id, metaID)
	meta, err := os.ReadFile(filepath.Join(staging, store.MetaFile))
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

func mustPublish(t testing.TB, st *Store, id, metaID string) {
	t.Helper()
	staging := stagedRecord(t, st, id, metaID)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("publish %s: %v", metaID, err)
	}
}

func mustPublishDigested(t *testing.T, st *Store, id, metaID string) string {
	t.Helper()
	staging := stagedRecord(t, st, id, metaID)
	digest, err := storetest.PublishDigested(t.Context(), st, staging, id)
	if err != nil {
		t.Fatalf("publish digested %s: %v", metaID, err)
	}
	return digest
}

func stagedRecord(t testing.TB, st *Store, id, metaID string) string {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage %s: %v", metaID, err)
	}
	seedRecord(t, staging, metaID)
	return staging
}

func seedRecord(t testing.TB, dir, metaID string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, store.ExportDir), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.ExportDir, "disk.img"), []byte(metaID), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, store.MetaFile), []byte(storetest.MetaJSON(metaID)), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
}

func backdate(t *testing.T, path string) {
	t.Helper()
	setAge(t, path, 2*store.GenerationGrace)
}

func setAge(t *testing.T, path string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("backdate %s: %v", path, err)
	}
}

func assertGonef(t *testing.T, path, format string, args ...any) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf(format+": %v", append(args, err)...)
	}
}

func assertPresentf(t *testing.T, path, format string, args ...any) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf(format+": %v", append(args, err)...)
	}
}
