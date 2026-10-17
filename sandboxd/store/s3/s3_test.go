package s3

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestS3BackendContract(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	storetest.RunContract(t, st)
}

func TestDeleteRetryConverges(t *testing.T) {
	const id = "ck_00000000000000bb"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                             []byte(`{"id":"` + id + `"}`),
		"ck/" + id + "/" + store.ExportDir + "-first/" + manifestName: []byte(`{"files":[]}`),
	}}
	st := newTestStore(t, fake)
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete retry after keys already gone: %v", err)
	}
	if keys := fake.keys(); len(keys) != 1 || !strings.HasSuffix(keys[0], manifestName) {
		t.Errorf("objects after Delete = %v, want only the manifest", keys)
	}
}

func TestDeleteRetainsMetaUntilManifestsAreRefreshed(t *testing.T) {
	const id = "ck_00000000000000bb"
	metaKey := "ck/" + id + "/" + store.MetaFile
	manifestKeys := []string{
		"ck/" + id + "/export-first/" + manifestName,
		"ck/" + id + "/export-second/" + manifestName,
	}
	for _, tc := range []struct {
		name       string
		failList   bool
		failDelete bool
	}{
		{name: "list failure", failList: true},
		{name: "delete failure", failDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeS3{
				objects: map[string][]byte{
					metaKey:         []byte(`{"id":"` + id + `"}`),
					manifestKeys[0]: []byte(`{"files":[]}`),
					manifestKeys[1]: []byte(`{"files":[]}`),
				},
				failList:   tc.failList,
				failDelete: tc.failDelete,
			}
			st := newTestStore(t, fake)

			if err := st.Delete(t.Context(), id); err == nil {
				t.Fatal("first Delete succeeded with an injected failure")
			}
			fake.mu.Lock()
			if _, ok := fake.objects[metaKey]; !ok {
				fake.mu.Unlock()
				t.Fatal("first Delete removed meta before the cleanup succeeded")
			}
			for _, batch := range fake.deleteBatches {
				if slices.Contains(batch, metaKey) {
					fake.mu.Unlock()
					t.Fatalf("first Delete included meta in batch %v", batch)
				}
			}
			fake.failList = false
			fake.failDelete = false
			fake.deleteBatches = nil
			fake.mu.Unlock()

			if err := st.Delete(t.Context(), id); err != nil {
				t.Fatalf("second Delete: %v", err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.objects) != len(manifestKeys) {
				t.Errorf("objects after retry = %d, want only the manifests", len(fake.objects))
			}
			for _, key := range manifestKeys {
				if time.Since(fake.modified[key]) > time.Minute {
					t.Errorf("manifest %s was not refreshed by Delete", key)
				}
			}
			if len(fake.deleteBatches) < 2 {
				t.Fatalf("delete batches = %v, want labels then meta", fake.deleteBatches)
			}
			last := len(fake.deleteBatches) - 1
			for i, batch := range fake.deleteBatches {
				hasMeta := slices.Contains(batch, metaKey)
				if (i == last) != hasMeta {
					t.Errorf("delete batch %d = %v, meta must appear only in final batch", i, batch)
				}
			}
			if batch := fake.deleteBatches[last]; len(batch) != 1 || batch[0] != metaKey {
				t.Errorf("final delete batch = %v, want only %s", batch, metaKey)
			}
		})
	}
}

func TestFetchReportsMetaWithoutExportAsNotFound(t *testing.T) {
	const id = "ck_00000000000000cc"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile: []byte(`{"id":"` + id + `"}`),
	}}
	st := newTestStore(t, fake)
	if _, _, _, err := st.Fetch(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch = %v, want store.ErrNotFound", err)
	}
}

func TestFetchOutlivesTheCallerThatStartedIt(t *testing.T) {
	const id = "ck_00000000000000dd"
	metaKey := "ck/" + id + "/" + store.MetaFile
	fake := &fakeS3{objects: map[string][]byte{}, exportGate: make(chan struct{}), exportEntered: make(chan struct{}, 1)}
	st := newTestStore(t, fake)
	publishRecord(t, st, id, []byte(`{"id":"`+id+`"}`), "payload")
	fake.resetRequests()

	first, hangUp := context.WithCancel(t.Context())
	started := make(chan error, 1)
	go func() {
		_, _, _, err := st.Fetch(first, id)
		started <- err
	}()
	<-fake.exportEntered
	joined := make(chan error, 1)
	go func() {
		_, _, _, err := st.Fetch(t.Context(), id)
		joined <- err
	}()
	for fake.requestCount(http.MethodGet, metaKey) < 2 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	hangUp()
	select {
	case err := <-started:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first caller's Fetch = %v, want its own cancellation", err)
		}
	case <-time.After(5 * time.Second):
		close(fake.exportGate)
		t.Fatal("first caller's Fetch still blocked on the stalled download after it hung up")
	}
	close(fake.exportGate)
	if err := <-joined; err != nil {
		t.Fatalf("second caller's Fetch = %v after the first caller hung up, want the shared download", err)
	}
}

func TestPublishDigestedRetryAndMetaRequestAccounting(t *testing.T) {
	const id = "ck_00000000000000dd"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	meta := []byte(`{"id":"` + id + `","gen":1}`)
	content := "retry-body"
	chunk := sha256.Sum256([]byte(content))
	exportKey := "ck/chunks/" + id + "/" + hex.EncodeToString(chunk[:])
	metaKey := "ck/" + id + "/" + store.MetaFile
	fake.setPutFailures(exportKey, 1)

	staging := stageRecord(t, st, id, meta, content)
	digest, err := st.PublishDigested(t.Context(), staging, id)
	if err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	want, err := store.AssembleDigest([]store.DigestFile{{
		Path: "disk.img", Size: int64(len(content)), Chunks: [][sha256.Size]byte{chunk},
	}})
	if err != nil {
		t.Fatalf("assemble expected digest: %v", err)
	}
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
	if got := fake.requestCount(http.MethodPut, exportKey); got != 2 {
		t.Errorf("export PUT requests = %d, want 2", got)
	}
	if got := fake.contentLengths(exportKey); !slices.Equal(got, []int64{int64(len(content)), int64(len(content))}) {
		t.Errorf("export content lengths = %v, want two %d-byte requests", got, len(content))
	}
	if got := fake.objectMetadata(metaKey)[digestMetadataKey]; got != want {
		t.Errorf("marker digest metadata = %q, want %q", got, want)
	}

	fake.resetRequests()
	_, _, fetchedDigest, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch miss: %v", err)
	}
	if fetchedDigest != want {
		t.Errorf("Fetch miss digest = %q, want %q", fetchedDigest, want)
	}
	assertSingleMetaGet(t, fake, metaKey)

	fake.resetRequests()
	_, _, fetchedDigest, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch hit: %v", err)
	}
	if fetchedDigest != want {
		t.Errorf("Fetch hit digest = %q, want %q", fetchedDigest, want)
	}
	assertSingleMetaGet(t, fake, metaKey)

	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":2}`), "plain")
	_, _, fetchedDigest, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch plain replacement: %v", err)
	}
	if fetchedDigest != "" {
		t.Errorf("plain replacement digest = %q, want empty", fetchedDigest)
	}
	if got := fake.objectMetadata(metaKey)[digestMetadataKey]; got != "" {
		t.Errorf("plain marker retained digest metadata %q", got)
	}
}

func TestPublishDigestedFailurePreservesCommittedGeneration(t *testing.T) {
	const id = "ck_00000000000000ee"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	oldMeta := []byte(`{"id":"` + id + `","gen":1}`)
	oldStaging := stageRecord(t, st, id, oldMeta, "old")
	oldDigest, err := st.PublishDigested(t.Context(), oldStaging, id)
	if err != nil {
		t.Fatalf("publish old generation: %v", err)
	}

	newMeta := []byte(`{"id":"` + id + `","gen":2}`)
	newStaging := stageRecord(t, st, id, newMeta, "replacement")
	replacement := sha256.Sum256([]byte("replacement"))
	newExportKey := "ck/chunks/" + id + "/" + hex.EncodeToString(replacement[:])
	metaKey := "ck/" + id + "/" + store.MetaFile
	markerPuts := fake.requestCount(http.MethodPut, metaKey)
	fake.setPutFailures(newExportKey, 100)
	digest, err := st.PublishDigested(t.Context(), newStaging, id)
	if err == nil {
		t.Fatal("PublishDigested succeeded with injected export failure")
	}
	if digest != "" {
		t.Errorf("failed PublishDigested returned digest %q", digest)
	}
	if got := fake.requestCount(http.MethodPut, metaKey); got != markerPuts {
		t.Errorf("marker PUT requests after export failure = %d, want %d", got, markerPuts)
	}

	dir, meta, fetchedDigest, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch old generation: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "disk.img"))
	if err != nil {
		t.Fatalf("read old generation: %v", err)
	}
	if string(meta) != string(oldMeta) || string(content) != "old" || fetchedDigest != oldDigest {
		t.Errorf("old generation = meta %q, content %q, digest %q", meta, content, fetchedDigest)
	}

	fake.setPutFailures(newExportKey, 0)
	newDigest, err := st.PublishDigested(t.Context(), newStaging, id)
	if err != nil {
		t.Fatalf("retry PublishDigested: %v", err)
	}
	dir, meta, fetchedDigest, err = st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch replacement: %v", err)
	}
	content, err = os.ReadFile(filepath.Join(dir, "disk.img"))
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if string(meta) != string(newMeta) || string(content) != "replacement" || fetchedDigest != newDigest {
		t.Errorf("replacement = meta %q, content %q, digest %q", meta, content, fetchedDigest)
	}
}

func TestRepublishRetainsGenerationSelectedByAnotherStore(t *testing.T) {
	const id = "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{}}
	reader := newTestStore(t, fake)
	writer := newTestStore(t, fake)
	publishRecord(t, writer, id, []byte(`{"id":"`+id+`","gen":1}`), "first")
	selected, err := reader.ReadMeta(t.Context(), id)
	if err != nil {
		t.Fatalf("select first generation: %v", err)
	}
	publishRecord(t, writer, id, []byte(`{"id":"`+id+`","gen":2}`), "second")

	if err = writer.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete the record: %v", err)
	}
	if _, ok := fake.get("ck/" + id + "/" + store.MetaFile); ok {
		t.Error("meta survived Delete")
	}
	gen := filepath.Join(reader.staging, "selected-first")
	if err = reader.populate(t.Context(), id, selected, gen); err != nil {
		t.Fatalf("fetch of the selected generation after the record was deleted: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(gen, store.ExportDir, "disk.img"))
	if err != nil || string(got) != "first" {
		t.Fatalf("selected generation bytes: %q, %v, want first", got, err)
	}
	fake.age("", 2*store.GenerationGrace)
	if err = writer.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if keys := fake.keys(); len(keys) != 0 {
		t.Errorf("objects left after the sweep: %v", keys)
	}
}

func TestSweepKeepsChunksOfARecentlyDeletedRecord(t *testing.T) {
	const id = "ck_00000000000000a8"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	publishRecord(t, st, id, []byte(`{"id":"`+id+`"}`), "payload")
	fake.age("", 2*store.GenerationGrace)
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if keys := fake.keys(); len(keys) != 2 {
		t.Errorf("objects after a sweep right after Delete = %v, want the manifest and its chunk", keys)
	}
	fake.age("", 2*store.GenerationGrace)
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if keys := fake.keys(); len(keys) != 0 {
		t.Errorf("objects left after the grace: %v", keys)
	}
}

func TestFetchPrunesSupersededGenerationsIdlePastTheGrace(t *testing.T) {
	const id = "ck_00000000000000aa"
	st := newTestStore(t, &fakeS3{objects: map[string][]byte{}})
	fetchGen := func(gen int) string {
		t.Helper()
		publishRecord(t, st, id, fmt.Appendf(nil, `{"id":"%s","gen":%d}`, id, gen), fmt.Sprint(gen))
		export, _, _, err := st.Fetch(t.Context(), id)
		if err != nil {
			t.Fatalf("fetch generation %d: %v", gen, err)
		}
		return filepath.Dir(export)
	}
	first := fetchGen(1)
	second := fetchGen(2)
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("a generation used within the grace was pruned: %v", err)
	}
	old := time.Now().Add(-store.GenerationGrace - time.Minute)
	if err := os.Chtimes(first, old, old); err != nil {
		t.Fatal(err)
	}
	fetchGen(3)
	if _, err := os.Stat(first); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("an idle superseded generation survived the next fetch: %v", err)
	}
	if _, err := os.Stat(second); err != nil {
		t.Errorf("a generation used within the grace was pruned: %v", err)
	}
}

func TestFetchHitRefreshesTheGenerationsUse(t *testing.T) {
	const id = "ck_00000000000000aa"
	st := newTestStore(t, &fakeS3{objects: map[string][]byte{}})
	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":1}`), "first")
	export, _, _, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	old := time.Now().Add(-2 * store.GenerationGrace)
	if err = os.Chtimes(filepath.Dir(export), old, old); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = st.Fetch(t.Context(), id); err != nil {
		t.Fatalf("fetch hit: %v", err)
	}
	info, err := os.Stat(filepath.Dir(export))
	if err != nil || time.Since(info.ModTime()) > time.Minute {
		t.Errorf("a cache hit left the generation looking idle: %v, %v", info, err)
	}
}

func TestS3BackendContractRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("SANDBOX_S3_E2E")
	if endpoint == "" {
		t.Skip("SANDBOX_S3_E2E not set (export it to a MinIO endpoint to run)")
	}
	st, err := New(t.Context(), store.S3Config{
		Bucket:         cmp.Or(os.Getenv("SANDBOX_S3_E2E_BUCKET"), "sbx-checkpoints"),
		Prefix:         "contract/",
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}

func TestSparseExportStaysSparseThroughS3(t *testing.T) {
	const id = "ck_00000000000000a1"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	size := 12*store.DigestChunkSize + 4096
	meta := []byte(`{"id":"` + id + `"}`)
	staging, src := stageSparseRecord(t, st, id, meta, size, map[int64]string{0: "head", 3*store.DigestChunkSize + 100: "mid", 11*store.DigestChunkSize + 10: "tail"})
	srcSum := fileSum(t, src)
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	count, uploaded := fake.chunkPuts("ck/chunks/" + id + "/")
	if want := 3 * store.DigestChunkSize; count != 3 || uploaded != want {
		t.Fatalf("chunk PUTs = %d carrying %d bytes, want 3 carrying %d", count, uploaded, want)
	}

	dir, _, _, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got := filepath.Join(dir, "disk.img")
	info, err := os.Stat(got)
	if err != nil || info.Size() != size {
		t.Fatalf("fetched size = %v, %v, want %d", info, err, size)
	}
	if fileSum(t, got) != srcSum {
		t.Errorf("fetched content differs from the source")
	}
	if blocks := allocatedBytes(t, got); blocks > uploaded+(1<<20) {
		t.Errorf("fetched file allocates %d bytes, want at most the %d uploaded", blocks, uploaded)
	}
}

func TestAllZeroDataChunkIsNotUploaded(t *testing.T) {
	const id = "ck_00000000000000a2"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	meta := []byte(`{"id":"` + id + `"}`)
	staging := stageRecord(t, st, id, meta, "")
	src := filepath.Join(staging, store.ExportDir, "disk.img")
	dense := make([]byte, 2*store.DigestChunkSize+10)
	for i := range int(store.DigestChunkSize) {
		dense[int(store.DigestChunkSize)+i] = 'x'
	}
	if err := os.WriteFile(src, dense, 0o600); err != nil {
		t.Fatalf("write dense export: %v", err)
	}
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if count, _ := fake.chunkPuts("ck/chunks/" + id + "/"); count != 1 {
		t.Errorf("chunk PUTs = %d, want only the non-zero chunk", count)
	}
	dir, _, _, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "disk.img")); err != nil || !bytes.Equal(got, dense) {
		t.Errorf("fetched content differs from the source: %v", err)
	}
}

func TestFetchFailsOnDamagedChunks(t *testing.T) {
	const id = "ck_00000000000000a3"
	meta := []byte(`{"id":"` + id + `"}`)
	sum := sha256.Sum256([]byte("payload"))
	chunkKey := "ck/chunks/" + id + "/" + hex.EncodeToString(sum[:])
	for _, tc := range []struct {
		name   string
		damage func(*fakeS3)
		want   string
	}{
		{"flipped byte", func(f *fakeS3) { f.objects[chunkKey][0] ^= 0xff }, "checksum"},
		{"missing chunk", func(f *fakeS3) { delete(f.objects, chunkKey) }, "is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeS3{objects: map[string][]byte{}}
			st := newTestStore(t, fake)
			publishRecord(t, st, id, meta, "payload")
			tc.damage(fake)
			_, _, _, err := st.Fetch(t.Context(), id)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Fetch = %v, want an error containing %q", err, tc.want)
			}
			if errors.Is(err, store.ErrNotFound) {
				t.Errorf("a damaged record must not read as absent: %v", err)
			}
		})
	}
}

func TestPublishDigestedHashesHolesLikeZeros(t *testing.T) {
	const id = "ck_00000000000000a4"
	st := newTestStore(t, &fakeS3{objects: map[string][]byte{}})
	size := 3*store.DigestChunkSize + 7
	staging, src := stageSparseRecord(t, st, id, []byte(`{"id":"`+id+`"}`), size, map[int64]string{store.DigestChunkSize + 5: "data", size - 1: "z"})
	sums := denseChunkSums(t, src)
	digest, err := st.PublishDigested(t.Context(), staging, id)
	if err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	want, err := store.AssembleDigest([]store.DigestFile{{Path: "disk.img", Size: size, Chunks: sums}})
	if err != nil {
		t.Fatalf("assemble reference digest: %v", err)
	}
	if digest != want {
		t.Errorf("digest = %q, want the dense-read digest %q", digest, want)
	}
}

func TestPublishDigestedSplitsChunksAtTheFixedBoundary(t *testing.T) {
	const id = "ck_00000000000000a5"
	st := newTestStore(t, &fakeS3{objects: map[string][]byte{}})
	content := string(bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize))) + "y"
	staging := stageRecord(t, st, id, []byte(`{"id":"`+id+`"}`), content)
	digest, err := st.PublishDigested(t.Context(), staging, id)
	if err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	want, err := store.AssembleDigest([]store.DigestFile{{
		Path: "disk.img", Size: store.DigestChunkSize + 1,
		Chunks: [][sha256.Size]byte{sha256.Sum256([]byte(content[:store.DigestChunkSize])), sha256.Sum256([]byte("y"))},
	}})
	if err != nil {
		t.Fatalf("assemble reference digest: %v", err)
	}
	if digest != want {
		t.Errorf("digest = %q, want %q", digest, want)
	}
}

func TestLineageDedupUploadsOnlyChangedChunks(t *testing.T) {
	const lineage = "sbx_00000000000000a1"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	cs := store.DigestChunkSize
	base := map[int64]string{0: "c0", 2 * cs: "c2", 4 * cs: "c4", 6 * cs: "c6"}
	changed := maps.Clone(base)
	changed[4*cs] = "changed"
	publish := func(id string, gen int, writes map[int64]string, opts ...store.PublishOption) (puts int, srcSum [sha256.Size]byte) {
		t.Helper()
		before, _ := fake.chunkPuts("ck/chunks/")
		staging, src := stageSparseRecord(t, st, id, fmt.Appendf(nil, `{"id":"%s","gen":%d}`, id, gen), 8*cs, writes)
		srcSum = fileSum(t, src)
		if err := st.Publish(t.Context(), staging, id, opts...); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
		after, _ := fake.chunkPuts("ck/chunks/")
		return after - before, srcSum
	}

	if puts, _ := publish("ck_00000000000000b1", 1, base, store.WithLineage(lineage)); puts != 4 {
		t.Fatalf("first publish uploaded %d chunks, want 4", puts)
	}
	puts, srcSum := publish("ck_00000000000000b2", 1, changed, store.WithLineage(lineage))
	if puts != 1 {
		t.Errorf("second record of the lineage uploaded %d chunks, want only the changed one", puts)
	}
	if puts, _ = publish("ck_00000000000000b1", 2, base, store.WithLineage(lineage)); puts != 0 {
		t.Errorf("re-publish of identical content uploaded %d chunks, want 0", puts)
	}
	if puts, _ = publish("ck_00000000000000b3", 1, base); puts != 4 {
		t.Errorf("a record outside the lineage uploaded %d chunks, want all 4", puts)
	}

	dir, _, _, err := st.Fetch(t.Context(), "ck_00000000000000b2")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if fileSum(t, filepath.Join(dir, "disk.img")) != srcSum {
		t.Errorf("fetched content differs from the source")
	}
}

func TestSweepKeepsReferencedChunksAndRemovesUnreferencedPastTheGrace(t *testing.T) {
	const lineage = "sbx_00000000000000a2"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	publish := func(id, only string) {
		t.Helper()
		staging := stageRecord(t, st, id, []byte(`{"id":"`+id+`"}`), "shared")
		if err := os.WriteFile(filepath.Join(staging, store.ExportDir, "extra.bin"), []byte(only), 0o600); err != nil {
			t.Fatalf("write extra: %v", err)
		}
		if err := st.Publish(t.Context(), staging, id, store.WithLineage(lineage)); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
	}
	publish("ck_00000000000000b1", "only-a")
	publish("ck_00000000000000b2", "only-b")
	if err := st.Delete(t.Context(), "ck_00000000000000b1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	fake.age("", 2*store.GenerationGrace)
	fresh := "ck/chunks/" + lineage + "/" + strings.Repeat("0", 64)
	fake.put(fresh, []byte("in-flight"))

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	chunk := func(content string) string {
		sum := sha256.Sum256([]byte(content))
		return "ck/chunks/" + lineage + "/" + hex.EncodeToString(sum[:])
	}
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{chunk("shared"), true},
		{chunk("only-b"), true},
		{chunk("only-a"), false},
		{fresh, true},
	} {
		if _, ok := fake.get(tc.key); ok != tc.want {
			t.Errorf("chunk %s present = %v, want %v", tc.key, ok, tc.want)
		}
	}
	if _, _, _, err := st.Fetch(t.Context(), "ck_00000000000000b2"); err != nil {
		t.Errorf("Fetch of the surviving record: %v", err)
	}
}

func TestSweepRemovesSupersededGenerationsPastTheGrace(t *testing.T) {
	const id = "ck_00000000000000a6"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	first := []byte(`{"id":"` + id + `","gen":1}`)
	second := []byte(`{"id":"` + id + `","gen":2}`)
	publishRecord(t, st, id, first, "first")
	publishRecord(t, st, id, second, "second")
	firstManifest := "ck/" + id + "/" + store.ExportGen(first) + "/" + manifestName
	secondManifest := "ck/" + id + "/" + store.ExportGen(second) + "/" + manifestName

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, ok := fake.get(firstManifest); !ok {
		t.Fatal("a generation superseded within the grace was swept")
	}

	fake.age(firstManifest, 2*store.GenerationGrace)
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, ok := fake.get(firstManifest); !ok {
		t.Fatal("a generation was swept although the current meta is younger than the grace")
	}

	fake.age("", 2*store.GenerationGrace)
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, ok := fake.get(firstManifest); ok {
		t.Error("a generation superseded past the grace survived the sweep")
	}
	if _, ok := fake.get(secondManifest); !ok {
		t.Error("the current generation was swept")
	}
	secondChunk := sha256.Sum256([]byte("second"))
	want := []string{"ck/chunks/" + id + "/" + hex.EncodeToString(secondChunk[:]), secondManifest, "ck/" + id + "/" + store.MetaFile}
	if keys := fake.keys(); !slices.Equal(keys, want) {
		t.Errorf("objects after the sweep = %v, want %v", keys, want)
	}
	if _, _, _, err := st.Fetch(t.Context(), id); err != nil {
		t.Errorf("Fetch after the sweep: %v", err)
	}
}

func TestSweepSparesAnInFlightGeneration(t *testing.T) {
	const id = "ck_00000000000000a7"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":1}`), "first")
	fake.age("", 2*store.GenerationGrace)
	inFlight := "ck/" + id + "/" + store.ExportDir + "-0000000000000000/" + manifestName
	fake.put(inFlight, []byte(`{"lineage":"`+id+`","files":[]}`))

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, ok := fake.get(inFlight); !ok {
		t.Error("the sweep removed a manifest uploaded moments ago")
	}
}

func TestPublishUploadsAnUnreferencedChunkAgain(t *testing.T) {
	const lineage = "sbx_00000000000000a3"
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	publishIn := func(id, content string, opts ...store.PublishOption) int {
		t.Helper()
		before, _ := fake.chunkPuts("ck/chunks/")
		staging := stageRecord(t, st, id, []byte(`{"id":"`+id+`"}`), content)
		if err := st.Publish(t.Context(), staging, id, opts...); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
		after, _ := fake.chunkPuts("ck/chunks/")
		return after - before
	}
	publish := func(id string) int { return publishIn(id, "orphaned", store.WithLineage(lineage)) }

	if puts := publish("ck_00000000000000c1"); puts != 1 {
		t.Fatalf("first publish uploaded %d chunks, want 1", puts)
	}
	if err := st.Delete(t.Context(), "ck_00000000000000c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if puts := publish("ck_00000000000000c2"); puts != 1 {
		t.Errorf("a listed chunk no manifest references was reused: %d uploads, want 1", puts)
	}
	if puts := publish("ck_00000000000000c3"); puts != 0 {
		t.Errorf("a chunk a committed manifest references was uploaded again: %d uploads, want 0", puts)
	}
	publishIn("ck_00000000000000c4", "elsewhere")
	if puts := publishIn("ck_00000000000000c5", "elsewhere", store.WithLineage(lineage)); puts != 1 {
		t.Errorf("a chunk another lineage's manifest references was reused: %d uploads, want 1", puts)
	}
}

func publishRecord(t *testing.T, st *Store, id string, meta []byte, content string) {
	t.Helper()
	staging := stageRecord(t, st, id, meta, content)
	if err := st.Publish(t.Context(), staging, id); err != nil {
		t.Fatalf("publish record: %v", err)
	}
}

func stageRecord(t *testing.T, st *Store, id string, meta []byte, content string) string {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("stage record: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if err := os.MkdirAll(export, 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(export, "disk.img"), []byte(content), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return staging
}

func stageSparseRecord(t *testing.T, st *Store, id string, meta []byte, size int64, writes map[int64]string) (staging, path string) {
	t.Helper()
	staging = stageRecord(t, st, id, meta, "")
	path = filepath.Join(staging, store.ExportDir, "disk.img")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open export: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err = f.Truncate(size); err != nil {
		t.Fatalf("truncate export: %v", err)
	}
	for off, data := range writes {
		if _, err = f.WriteAt([]byte(data), off); err != nil {
			t.Fatalf("write export at %d: %v", off, err)
		}
	}
	return staging, path
}

func fileSum(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	return [sha256.Size]byte(h.Sum(nil))
}

func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Sys().(*syscall.Stat_t).Blocks * 512
}

func denseChunkSums(t *testing.T, path string) [][sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	var sums [][sha256.Size]byte
	buf := make([]byte, store.DigestChunkSize)
	for {
		n, err := io.ReadFull(f, buf)
		if n > 0 {
			sums = append(sums, sha256.Sum256(buf[:n]))
		}
		if err != nil {
			return sums
		}
	}
}

func newTestStore(t *testing.T, fake *fakeS3) *Store {
	t.Helper()
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
	st, err := New(t.Context(), store.S3Config{
		Bucket: "testbucket", Prefix: "ck/", Endpoint: ts.URL,
		Region: "us-east-1", ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return st
}

func assertSingleMetaGet(t *testing.T, fake *fakeS3, metaKey string) {
	t.Helper()
	if got := fake.requestCount(http.MethodGet, metaKey); got != 1 {
		t.Errorf("meta GET requests = %d, want 1", got)
	}
	if got := fake.requestCount(http.MethodHead, metaKey); got != 0 {
		t.Errorf("meta HEAD requests = %d, want 0", got)
	}
}

type fakeRequest struct {
	method string
	key    string
}

type fakeS3 struct {
	mu            sync.Mutex
	objects       map[string][]byte
	metadata      map[string]map[string]string
	failPuts      map[string]int
	requests      map[fakeRequest]int
	putLengths    map[string][]int64
	modified      map[string]time.Time
	deleteBatches [][]string
	failList      bool
	failDelete    bool
	exportGate    chan struct{}
	exportEntered chan struct{}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
	if f.exportGate != nil && r.Method == http.MethodGet && strings.Contains(key, "/"+store.ExportDir) {
		select {
		case f.exportEntered <- struct{}{}:
		default:
		}
		<-f.exportGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.requests == nil {
		f.requests = map[fakeRequest]int{}
	}
	f.requests[fakeRequest{method: r.Method, key: key}]++
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && query.Get("list-type") == "2":
		f.handleListObjects(w, r)
	case r.Method == http.MethodPost && query.Has("delete"):
		f.handleDeleteObjects(w, r)
	case r.Method == http.MethodPut:
		f.handlePutObject(w, r, key)
	case r.Method == http.MethodHead:
		f.handleHeadObject(w, key)
	case r.Method == http.MethodGet:
		f.handleGetObject(w, r, key)
	case r.Method == http.MethodDelete:
		f.handleDeleteObject(w, key)
	default:
		http.Error(w, "unsupported", http.StatusNotImplemented)
	}
}

func (f *fakeS3) setPutFailures(key string, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPuts == nil {
		f.failPuts = map[string]int{}
	}
	f.failPuts[key] = count
}

func (f *fakeS3) requestCount(method, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[fakeRequest{method: method, key: key}]
}

func (f *fakeS3) contentLengths(key string) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.putLengths[key])
}

func (f *fakeS3) objectMetadata(key string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.metadata[key])
}

func (f *fakeS3) chunkPuts(prefix string) (count int, bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, lengths := range f.putLengths {
		if strings.HasPrefix(key, prefix) {
			count += len(lengths)
			for _, n := range lengths {
				bytes += n
			}
		}
	}
	return count, bytes
}

func (f *fakeS3) put(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = body
	if f.modified == nil {
		f.modified = map[string]time.Time{}
	}
	f.modified[key] = time.Now()
}

func (f *fakeS3) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	return body, ok
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects))
}

func (f *fakeS3) age(prefix string, by time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			f.modified[key] = time.Now().Add(-by)
		}
	}
}

func (f *fakeS3) resetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.requests)
}

func (f *fakeS3) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if f.putLengths == nil {
		f.putLengths = map[string][]int64{}
	}
	f.putLengths[key] = append(f.putLengths[key], r.ContentLength)
	if f.failPuts[key] > 0 {
		f.failPuts[key]--
		writeFakeError(w, http.StatusInternalServerError, "InternalError", "injected put failure")
		return
	}
	if f.objects == nil {
		f.objects = map[string][]byte{}
	}
	if f.metadata == nil {
		f.metadata = map[string]map[string]string{}
	}
	f.objects[key] = bytes.Clone(body)
	if f.modified == nil {
		f.modified = map[string]time.Time{}
	}
	f.modified[key] = time.Now()
	metadata := requestMetadata(r.Header)
	if len(metadata) == 0 {
		delete(f.metadata, key)
	} else {
		f.metadata[key] = metadata
	}
	w.Header().Set("ETag", `"put"`)
}

func (f *fakeS3) handleListObjects(w http.ResponseWriter, r *http.Request) {
	if f.failList {
		http.Error(w, "injected list failure", http.StatusBadRequest)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delim := r.URL.Query().Get("delimiter")
	type object struct {
		Key          string `xml:"Key"`
		Size         int    `xml:"Size"`
		LastModified string `xml:"LastModified"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	var result struct {
		XMLName        xml.Name `xml:"ListBucketResult"`
		IsTruncated    bool     `xml:"IsTruncated"`
		Contents       []object
		CommonPrefixes []commonPrefix
	}
	seen := map[string]bool{}
	for key, body := range f.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(key[len(prefix):], delim); i >= 0 {
				common := key[:len(prefix)+i+1]
				if !seen[common] {
					seen[common] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: common})
				}
				continue
			}
		}
		result.Contents = append(result.Contents, object{
			Key: key, Size: len(body), LastModified: f.modified[key].UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}
	slices.SortFunc(result.Contents, func(a, b object) int { return cmp.Compare(a.Key, b.Key) })
	slices.SortFunc(result.CommonPrefixes, func(a, b commonPrefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) handleHeadObject(w http.ResponseWriter, key string) {
	body, ok := f.objects[key]
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	writeMetadata(w.Header(), f.metadata[key])
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
}

func (f *fakeS3) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	body, ok := f.objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		return
	}
	writeMetadata(w.Header(), f.metadata[key])
	if rng := r.Header.Get("Range"); rng != "" {
		var start, end int
		if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil && start < len(body) {
			end = min(end, len(body)-1)
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[start : end+1])
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

func (f *fakeS3) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil || xml.Unmarshal(body, &req) != nil {
		http.Error(w, "bad delete xml", http.StatusBadRequest)
		return
	}
	batch := make([]string, len(req.Objects))
	for i := range req.Objects {
		batch[i] = req.Objects[i].Key
	}
	f.deleteBatches = append(f.deleteBatches, batch)
	type deleteError struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message,omitempty"`
	}
	var result struct {
		XMLName xml.Name      `xml:"DeleteResult"`
		Errors  []deleteError `xml:"Error"`
	}
	if f.failDelete {
		result.Errors = append(result.Errors, deleteError{
			Key: req.Objects[0].Key, Code: "AccessDenied", Message: "injected delete failure",
		})
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
		return
	}
	for _, object := range req.Objects {
		if _, ok := f.objects[object.Key]; !ok {
			result.Errors = append(result.Errors, deleteError{Key: object.Key, Code: "NoSuchKey"})
			continue
		}
		delete(f.objects, object.Key)
		delete(f.metadata, object.Key)
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) handleDeleteObject(w http.ResponseWriter, key string) {
	delete(f.objects, key)
	delete(f.metadata, key)
	w.WriteHeader(http.StatusNoContent)
}

func requestMetadata(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		key = strings.ToLower(key)
		if name, ok := strings.CutPrefix(key, "x-amz-meta-"); ok && len(values) > 0 {
			metadata[name] = values[0]
		}
	}
	return metadata
}

func writeMetadata(header http.Header, metadata map[string]string) {
	for key, value := range metadata {
		header.Set("x-amz-meta-"+key, value)
	}
}

func writeFakeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}
