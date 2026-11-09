package s3

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
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
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

const _ = uint(store.GenerationGrace - transferBudget - 1)

func TestS3BackendContract(t *testing.T) {
	_, st := newFakeStore(t)
	storetest.RunContract(t, st)
}

func TestDeleteRetryConverges(t *testing.T) {
	const id = "ck_00000000000000bb"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                            metaJSON(id),
		"ck/manifests/" + id + "/" + store.ExportDir + "-first.json": []byte(`{"lineage":"` + id + `","files":[]}`),
	}}
	st := newTestStore(t, fake)
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete retry after keys already gone: %v", err)
	}
	if keys := fake.keys(); len(keys) != 1 || !strings.HasPrefix(keys[0], "ck/manifests/") {
		t.Errorf("objects after Delete = %v, want only the manifest", keys)
	}
}

func TestDeleteRetainsMetaUntilManifestsAreRefreshed(t *testing.T) {
	const id = "ck_00000000000000bb"
	metaKey := "ck/" + id + "/" + store.MetaFile
	manifestKeys := []string{
		"ck/manifests/" + id + "/export-first.json",
		"ck/manifests/" + id + "/export-second.json",
	}
	for _, tt := range []struct {
		name       string
		failList   bool
		failDelete bool
	}{
		{name: "list failure", failList: true},
		{name: "delete failure", failDelete: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeS3{
				objects: map[string][]byte{
					metaKey:         metaJSON(id),
					manifestKeys[0]: []byte(`{"lineage":"` + id + `","files":[]}`),
					manifestKeys[1]: []byte(`{"lineage":"` + id + `","files":[]}`),
				},
				failList:   tt.failList,
				failDelete: tt.failDelete,
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
		"ck/" + id + "/" + store.MetaFile: metaJSON(id),
	}}
	st := newTestStore(t, fake)
	if _, _, _, err := st.Fetch(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch = %v, want store.ErrNotFound", err)
	}
}

func TestFetchOutlivesTheCallerThatStartedIt(t *testing.T) {
	const id = "ck_00000000000000dd"
	fake, st, release := newGatedStore(t, "")
	publishRecord(t, st, id, metaJSON(id), "payload")
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
	storetest.WaitStacks(t, 2, func(stack string) bool {
		return strings.Contains(stack, "s3.(*Store).Fetch(") && !strings.Contains(stack, "readMeta")
	})
	hangUp()
	select {
	case err := <-started:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first caller's Fetch = %v, want its own cancellation", err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("first caller's Fetch still blocked on the stalled download after it hung up")
	}
	release()
	if err := <-joined; err != nil {
		t.Fatalf("second caller's Fetch = %v after the first caller hung up, want the shared download", err)
	}
}

func TestPublishDigestedRetryAndMetaRequestAccounting(t *testing.T) {
	const id = "ck_00000000000000dd"
	fake, st := newFakeStore(t)
	meta := []byte(`{"id":"` + id + `","gen":1}`)
	content := "retry-body"
	chunk := sha256.Sum256([]byte(content))
	exportKey := "ck/chunks/" + id + "/" + hex.EncodeToString(chunk[:])
	metaKey := "ck/" + id + "/" + store.MetaFile
	fake.setPutFailures(exportKey, 1)

	staging := stageRecord(t, st, id, meta, content)
	digest, err := storetest.PublishDigested(t.Context(), st, staging, id)
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
	fake, st := newFakeStore(t)
	oldMeta := []byte(`{"id":"` + id + `","gen":1}`)
	oldStaging := stageRecord(t, st, id, oldMeta, "old")
	oldDigest, err := storetest.PublishDigested(t.Context(), st, oldStaging, id)
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
	digest, err := storetest.PublishDigested(t.Context(), st, newStaging, id)
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
	newDigest, err := storetest.PublishDigested(t.Context(), st, newStaging, id)
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
	if fake.has("ck/" + id + "/" + store.MetaFile) {
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
}

func TestFetchPrunesSupersededGenerationsIdlePastTheGrace(t *testing.T) {
	const id = "ck_00000000000000aa"
	_, st := newFakeStore(t)
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
	_, st := newFakeStore(t)
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

func TestAcceptEncodingIsUnsignedOnlyWhenConfigured(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsigned=%t", unsigned), func(t *testing.T) {
			fake := &fakeS3{objects: map[string][]byte{}}
			var mu sync.Mutex
			var headers []http.Header
			st := newTestStoreAt(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				headers = append(headers, r.Header.Clone())
				mu.Unlock()
				fake.ServeHTTP(w, r)
			}), store.S3Config{UnsignedAcceptEncoding: unsigned})
			if _, err := st.ReadMeta(t.Context(), "ck_0123456789abcdef"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("ReadMeta: %v, want ErrNotFound", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(headers) == 0 {
				t.Fatal("no request reached the endpoint")
			}
			for _, h := range headers {
				if got := h.Get("Accept-Encoding"); got != "identity" {
					t.Errorf("Accept-Encoding = %q, want identity", got)
				}
				if signed := strings.Contains(strings.ToLower(h.Get("Authorization")), "accept-encoding"); signed == unsigned {
					t.Errorf("Accept-Encoding signed = %t, want %t", signed, !unsigned)
				}
			}
		})
	}
}

func TestS3BackendContractRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("SANDBOX_S3_E2E")
	if endpoint == "" {
		t.Skip("SANDBOX_S3_E2E not set (export it to a MinIO endpoint to run)")
	}
	st, err := New(t.Context(), store.S3Config{
		Bucket:                 cmp.Or(os.Getenv("SANDBOX_S3_E2E_BUCKET"), "sbx-checkpoints"),
		Prefix:                 "contract/",
		Endpoint:               endpoint,
		Region:                 "us-east-1",
		ForcePathStyle:         true,
		UnsignedAcceptEncoding: os.Getenv("SANDBOX_S3_E2E_UNSIGNED_ACCEPT_ENCODING") != "",
	}, t.TempDir(), store.CheckpointIDRe, true)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}

func TestSparseExportStaysSparseThroughS3(t *testing.T) {
	const id = "ck_00000000000000a1"
	fake, st := newFakeStore(t)
	size := 12*store.DigestChunkSize + 4096
	meta := metaJSON(id)
	staging, src := stageSparseRecord(t, st, id, meta, size, map[int64][]byte{0: []byte("head"), 3*store.DigestChunkSize + 100: []byte("mid"), 11*store.DigestChunkSize + 10: []byte("tail")})
	srcSum := storetest.FileSum(t, src)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
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
	if storetest.FileSum(t, got) != srcSum {
		t.Error("fetched content differs from the source")
	}
	if blocks := storetest.AllocatedBytes(t, got); blocks > uploaded+(1<<20) {
		t.Errorf("fetched file allocates %d bytes, want at most the %d uploaded", blocks, uploaded)
	}
}

func TestAllZeroDataChunkIsNotUploaded(t *testing.T) {
	const id = "ck_00000000000000a2"
	fake, st := newFakeStore(t)
	meta := metaJSON(id)
	staging := stageRecord(t, st, id, meta, "")
	src := filepath.Join(staging, store.ExportDir, "disk.img")
	dense := make([]byte, 2*store.DigestChunkSize+10)
	for i := range int(store.DigestChunkSize) {
		dense[int(store.DigestChunkSize)+i] = 'x'
	}
	if err := os.WriteFile(src, dense, 0o600); err != nil {
		t.Fatalf("write dense export: %v", err)
	}
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
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
	meta := metaJSON(id)
	sum := sha256.Sum256([]byte("payload"))
	chunkKey := "ck/chunks/" + id + "/" + hex.EncodeToString(sum[:])
	for _, tt := range []struct {
		name   string
		damage func(*fakeS3)
		want   string
	}{
		{"flipped byte", func(f *fakeS3) { f.objects[chunkKey][0] ^= 0xff }, "checksum"},
		{"missing chunk", func(f *fakeS3) { delete(f.objects, chunkKey) }, "is missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake, st := newFakeStore(t)
			publishRecord(t, st, id, meta, "payload")
			tt.damage(fake)
			_, _, _, err := st.Fetch(t.Context(), id)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Fetch = %v, want an error containing %q", err, tt.want)
			}
			if errors.Is(err, store.ErrNotFound) {
				t.Errorf("a damaged record must not read as absent: %v", err)
			}
		})
	}
}

func TestPublishDigestedHashesHolesLikeZeros(t *testing.T) {
	const id = "ck_00000000000000a4"
	_, st := newFakeStore(t)
	size := 3*store.DigestChunkSize + 7
	staging, src := stageSparseRecord(t, st, id, metaJSON(id), size, map[int64][]byte{store.DigestChunkSize + 5: []byte("data"), size - 1: []byte("z")})
	sums := denseChunkSums(t, src)
	digest, err := storetest.PublishDigested(t.Context(), st, staging, id)
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
	_, st := newFakeStore(t)
	content := string(bytes.Repeat([]byte{'x'}, int(store.DigestChunkSize))) + "y"
	staging := stageRecord(t, st, id, metaJSON(id), content)
	digest, err := storetest.PublishDigested(t.Context(), st, staging, id)
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
	fake, st := newFakeStore(t)
	cs := store.DigestChunkSize
	base := map[int64][]byte{0: []byte("c0"), 2 * cs: []byte("c2"), 4 * cs: []byte("c4"), 6 * cs: []byte("c6")}
	changed := maps.Clone(base)
	changed[4*cs] = []byte("changed")
	publish := func(id string, gen int, writes map[int64][]byte, scope string) (puts int, srcSum [sha256.Size]byte) {
		t.Helper()
		before, _ := fake.chunkPuts("ck/chunks/")
		staging, src := stageSparseRecord(t, st, id, fmt.Appendf(nil, `{"id":"%s","gen":%d}`, id, gen), 8*cs, writes)
		srcSum = storetest.FileSum(t, src)
		if err := st.Publish(t.Context(), staging, id, scope); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
		after, _ := fake.chunkPuts("ck/chunks/")
		return after - before, srcSum
	}

	if puts, _ := publish("ck_00000000000000b1", 1, base, lineage); puts != 4 {
		t.Fatalf("first publish uploaded %d chunks, want 4", puts)
	}
	puts, srcSum := publish("ck_00000000000000b2", 1, changed, lineage)
	if puts != 1 {
		t.Errorf("second record of the lineage uploaded %d chunks, want only the changed one", puts)
	}
	if puts, _ = publish("ck_00000000000000b1", 2, base, lineage); puts != 0 {
		t.Errorf("re-publish of identical content uploaded %d chunks, want 0", puts)
	}
	if puts, _ = publish("ck_00000000000000b3", 1, base, ""); puts != 4 {
		t.Errorf("a record outside the lineage uploaded %d chunks, want all 4", puts)
	}

	dir, _, _, err := st.Fetch(t.Context(), "ck_00000000000000b2")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if storetest.FileSum(t, filepath.Join(dir, "disk.img")) != srcSum {
		t.Error("fetched content differs from the source")
	}
}

func TestPublishReadsOnlyItsLineage(t *testing.T) {
	const lineage = "sbx_00000000000000a4"
	fake, st := newFakeStore(t)
	for _, id := range []string{"ck_00000000000000d1", "ck_00000000000000d2"} {
		publishRecord(t, st, id, metaJSON(id), "unrelated")
	}
	publishIn := func(id string) {
		t.Helper()
		staging := stageRecord(t, st, id, metaJSON(id), "claim")
		if err := st.Publish(t.Context(), staging, id, lineage); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
	}
	publishIn("ck_00000000000000e1")
	publishIn("ck_00000000000000e2")
	fake.resetRequests()
	publishIn("ck_00000000000000e3")

	meta := func(id string) int { return fake.requestCount(http.MethodGet, "ck/"+id+"/"+store.MetaFile) }
	if got := meta("ck_00000000000000d1") + meta("ck_00000000000000d2"); got != 0 {
		t.Errorf("a publish read %d metas of records outside its lineage", got)
	}
	if got := meta("ck_00000000000000e1") + meta("ck_00000000000000e2"); got != 2 {
		t.Errorf("a publish read %d metas of its lineage's records, want 2", got)
	}
}

func TestPublishUploadsAnUnreferencedChunkAgain(t *testing.T) {
	const lineage = "sbx_00000000000000a3"
	fake, st := newFakeStore(t)
	publishIn := func(id, content, scope string) int {
		t.Helper()
		before, _ := fake.chunkPuts("ck/chunks/")
		staging := stageRecord(t, st, id, metaJSON(id), content)
		if err := st.Publish(t.Context(), staging, id, scope); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
		after, _ := fake.chunkPuts("ck/chunks/")
		return after - before
	}
	publish := func(id string) int { return publishIn(id, "orphaned", lineage) }

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
	publishIn("ck_00000000000000c4", "elsewhere", "")
	if puts := publishIn("ck_00000000000000c5", "elsewhere", lineage); puts != 1 {
		t.Errorf("a chunk another lineage's manifest references was reused: %d uploads, want 1", puts)
	}
}

func TestDeletedRecordLeavesNoPrefixToList(t *testing.T) {
	const (
		gone = "ck_00000000000000b1"
		live = "ck_00000000000000b2"
	)
	fake, st := newFakeStore(t)
	publishRecord(t, st, gone, metaJSON(gone), "gone-payload")
	publishRecord(t, st, live, metaJSON(live), "live-payload")
	if err := st.Delete(t.Context(), gone); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, key := range fake.keys() {
		if strings.HasPrefix(key, "ck/"+gone+"/") {
			t.Errorf("Delete left %s under the record prefix", key)
		}
	}

	fake.resetRequests()
	recs, err := st.Metas(t.Context(), "")
	if err != nil || len(recs) != 1 {
		t.Fatalf("Metas = %d records, %v, want only the live one", len(recs), err)
	}
	if got := fake.requestCount(http.MethodGet, "ck/"+gone+"/"+store.MetaFile); got != 0 {
		t.Errorf("Metas read the meta of the deleted record %d times, want 0", got)
	}

	sum := sha256.Sum256([]byte("gone-payload"))
	if !fake.has("ck/chunks/" + gone + "/" + hex.EncodeToString(sum[:])) {
		t.Error("Delete freed the chunk of the deleted record")
	}
}

func TestCheckpointMetasNeverReadAnArchiveImage(t *testing.T) {
	const (
		ckpt    = "ck_00000000000000b3"
		archive = "ar_00000000000000b3"
	)
	fake, st := newFakeStore(t)
	publishRecord(t, st, ckpt, metaJSON(ckpt), "checkpoint")
	publishRecord(t, st, archive, metaJSON(archive), "archive")
	fake.resetRequests()
	recs, err := st.Metas(t.Context(), store.CheckpointPrefix)
	if err != nil || len(recs) != 1 || string(recs[0].Meta) != string(metaJSON(ckpt)) {
		t.Fatalf("Metas = %d records, %v, want only the checkpoint", len(recs), err)
	}
	if got := fake.requestCount(http.MethodGet, "ck/"+archive+"/"+store.MetaFile); got != 0 {
		t.Errorf("listing checkpoints read the archive image's meta %d times, want 0", got)
	}
}

func TestSweepGenerationsLeavesTheBucketAlone(t *testing.T) {
	const id = "ck_00000000000000a8"
	fake, st := newFakeStore(t)
	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":1}`), "first")
	publishRecord(t, st, id, []byte(`{"id":"`+id+`","gen":2}`), "second")
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	fake.age(2 * store.GenerationGrace)
	want := fake.keys()
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if keys := fake.keys(); !slices.Equal(keys, want) {
		t.Errorf("objects after SweepGenerations = %v, want %v", keys, want)
	}
}

func TestSweepGenerationsDropsTheCacheOfARecordAPeerDeleted(t *testing.T) {
	const deleted, live = "ck_00000000000000a9", "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{}}
	owner, fetcher := newTestStore(t, fake), newTestStore(t, fake)
	for _, id := range []string{deleted, live} {
		publishRecord(t, owner, id, metaJSON(id), id)
		if _, _, _, err := fetcher.Fetch(t.Context(), id); err != nil {
			t.Fatalf("fetch %s: %v", id, err)
		}
		ageCache(t, filepath.Join(fetcher.staging, cacheDir, id))
	}
	if err := owner.Delete(t.Context(), deleted); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := fetcher.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}

	if _, err := os.Stat(filepath.Join(fetcher.staging, cacheDir, deleted)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the cache of a record a peer deleted survived the sweep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(fetcher.staging, cacheDir, live)); err != nil {
		t.Errorf("the cache of a live record was swept: %v", err)
	}
}

func TestSweepGenerationsDropsACacheACanceledFetchInstalledAfterDelete(t *testing.T) {
	const id = "ck_00000000000000ab"
	fake, st, release := newGatedStore(t, "/chunks/")
	publishRecord(t, st, id, metaJSON(id), "payload")
	ctx, cancel := context.WithCancel(t.Context())
	fetched := make(chan error, 1)
	go func() {
		_, _, _, err := st.Fetch(ctx, id)
		fetched <- err
	}()
	<-fake.exportEntered
	cancel()
	if err := <-fetched; !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch = %v, want its cancellation", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	release()
	cached := filepath.Join(st.staging, cacheDir, id)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if entries, _ := os.ReadDir(cached); len(entries) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the detached download never installed its cache")
		}
	}

	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, err := os.Stat(cached); err != nil {
		t.Fatalf("a cache used within the grace was swept: %v", err)
	}
	ageCache(t, cached)
	if err := st.SweepGenerations(t.Context()); err != nil {
		t.Fatalf("SweepGenerations: %v", err)
	}
	if _, err := os.Stat(cached); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the cache a canceled fetch installed after the delete survived the sweep: %v", err)
	}
}

func TestDeleteRemovesTheLineageMarker(t *testing.T) {
	const lineage = "sbx_00000000000000a5"
	fake, st := newFakeStore(t)
	for _, id := range []string{"ck_00000000000000f1", "ck_00000000000000f2"} {
		staging := stageRecord(t, st, id, metaJSON(id), "claim")
		if err := st.Publish(t.Context(), staging, id, lineage); err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
	}
	if err := st.Delete(t.Context(), "ck_00000000000000f1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if fake.has("ck/lineages/" + lineage + "/ck_00000000000000f1") {
		t.Error("Delete left the lineage marker of the deleted record")
	}
	if !fake.has("ck/lineages/" + lineage + "/ck_00000000000000f2") {
		t.Error("Delete removed the marker of another record")
	}
}

func TestFetchFileDownloadsOnlyTheRequestedFile(t *testing.T) {
	const id = "ck_00000000000000aa"
	meta := metaJSON(id)
	fake, st := newFakeStore(t)
	staging := stageRecord(t, st, id, meta, "disk-bytes")
	if err := os.WriteFile(filepath.Join(staging, store.ExportDir, "memory.bin"), []byte("memory-bytes"), 0o600); err != nil {
		t.Fatalf("write memory: %v", err)
	}
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	fake.resetRequests()

	dst := filepath.Join(t.TempDir(), "disk.img")
	if err := st.FetchFile(t.Context(), id, "disk.img", dst); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if got, readErr := os.ReadFile(dst); readErr != nil || string(got) != "disk-bytes" {
		t.Fatalf("fetched file: %q, %v", got, readErr)
	}
	memory := sha256.Sum256([]byte("memory-bytes"))
	memoryKey := "ck/chunks/" + id + "/" + hex.EncodeToString(memory[:])
	if n := fake.requestCount(http.MethodGet, memoryKey); n != 0 {
		t.Errorf("GET %s = %d requests, want 0", memoryKey, n)
	}
	if _, err := os.Stat(filepath.Join(st.staging, cacheDir)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("FetchFile touched the shared fetch cache: %v", err)
	}
}

func TestFetchFileInstallsOnlyACompleteDownload(t *testing.T) {
	const id = "ck_00000000000000aa"
	meta := metaJSON(id)
	fake, st, release := newGatedStore(t, "/chunks/")
	publishRecord(t, st, id, meta, "disk-bytes")
	dst := filepath.Join(t.TempDir(), "disk.img")

	fetched := make(chan error, 1)
	go func() {
		fetched <- st.FetchFile(t.Context(), id, "disk.img", dst)
	}()
	<-fake.exportEntered
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a download in flight is already visible at the destination: %v", err)
	}
	release()
	if err := <-fetched; err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != "disk-bytes" {
		t.Errorf("destination %q, %v, want the object's bytes", got, err)
	}
	if _, err := os.Stat(dst + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the partial download survived: %v", err)
	}
}

func TestFetchFileKeepsHoles(t *testing.T) {
	const id = "ck_00000000000000a2"
	_, st := newFakeStore(t)
	size := 12*store.DigestChunkSize + 4096
	staging, src := stageSparseRecord(t, st, id, metaJSON(id), size, map[int64][]byte{0: []byte("head"), 11*store.DigestChunkSize + 10: []byte("tail")})
	srcSum := storetest.FileSum(t, src)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "disk.img")
	if err := st.FetchFile(t.Context(), id, "disk.img", dst); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if info, err := os.Stat(dst); err != nil || info.Size() != size {
		t.Fatalf("fetched size = %v, %v, want %d", info, err, size)
	}
	if storetest.FileSum(t, dst) != srcSum {
		t.Error("fetched content differs from the source")
	}
	if blocks, limit := storetest.AllocatedBytes(t, dst), 2*store.DigestChunkSize+1<<20; blocks > limit {
		t.Errorf("fetched file allocates %d bytes, want at most %d for two data chunks", blocks, limit)
	}
}

func TestOnlyALabeledStoreReadsLabelsWhenListing(t *testing.T) {
	ids := []string{"ck_00000000000000d1", "ck_00000000000000d2"}
	for _, labeled := range []bool{true, false} {
		fake := &fakeS3{objects: map[string][]byte{}}
		for _, id := range ids {
			fake.objects["ck/"+id+"/"+store.MetaFile] = metaJSON(id)
		}
		st := newTestStore(t, fake)
		st.labeled = labeled
		recs, err := st.Metas(t.Context(), "")
		if err != nil || len(recs) != len(ids) {
			t.Fatalf("labeled=%v: Metas = %d records, %v", labeled, len(recs), err)
		}
		gets := 0
		for _, id := range ids {
			gets += fake.requestCount(http.MethodGet, "ck/"+id+"/"+store.LabelsFile)
		}
		if want := map[bool]int{true: len(ids), false: 0}[labeled]; gets != want {
			t.Errorf("labeled=%v: %d label GETs, want %d", labeled, gets, want)
		}
	}
}

func TestRaisedConcurrencyReachesItsLimitAndKeepsItsConnections(t *testing.T) {
	const (
		id     = "ck_00000000000000b1"
		chunks = 12
	)
	gate := &chunkGate{fake: &fakeS3{objects: map[string][]byte{}}, conns: map[string]struct{}{}}
	st := newTestStoreAt(t, gate, store.S3Config{UploadConcurrency: chunks, FetchConcurrency: chunks})
	writes := map[int64][]byte{}
	for i := range int64(chunks) {
		writes[i*store.DigestChunkSize] = []byte{byte(i + 1)}
	}
	staging, src := stageSparseRecord(t, st, id, metaJSON(id), chunks*store.DigestChunkSize, writes)
	srcSum := storetest.FileSum(t, src)

	gate.arm(http.MethodPut, chunks)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if peak, _ := gate.result(); peak != chunks {
		t.Errorf("chunk PUTs in flight = %d, want %d", peak, chunks)
	}
	for round := range 2 {
		gate.arm(http.MethodGet, chunks)
		dst := filepath.Join(t.TempDir(), "disk.img")
		if err := st.FetchFile(t.Context(), id, "disk.img", dst); err != nil {
			t.Fatalf("FetchFile round %d: %v", round, err)
		}
		if storetest.FileSum(t, dst) != srcSum {
			t.Fatalf("FetchFile round %d: content differs from the source", round)
		}
		peak, opened := gate.result()
		if peak != chunks {
			t.Errorf("round %d: chunk GETs in flight = %d, want %d", round, peak, chunks)
		}
		if round == 1 && opened != 0 {
			t.Errorf("second fetch opened %d connections, want every one reused from the idle pool", opened)
		}
	}
}

func TestFetchRejectsAnInvalidManifestBeforeWritingAFile(t *testing.T) {
	const id = "ck_00000000000000a4"
	appendFile := func(path string, size int64) func(*manifest) {
		return func(man *manifest) { man.Files = append(man.Files, manifestFile{Path: path, Size: size}) }
	}
	for _, tt := range []struct {
		name   string
		damage func(*manifest)
		want   string
	}{
		{"parent traversal", appendFile("../../escaped", 1), "not a path inside the record"},
		{"absolute path", appendFile("/escaped", 1), "not a path inside the record"},
		{"backslash", appendFile(`..\escaped`, 1), "not a path inside the record"},
		{"empty path", appendFile("", 1), "not a path inside the record"},
		{"duplicate path", appendFile("./disk.img", 1), "twice"},
		{"negative size", func(man *manifest) { man.Files[0].Size = -1 }, "outside [0,"},
		{"record over the bound", appendFile("big.img", store.MaxRecordBytes), "outside [0,"},
		{"too many files", func(man *manifest) {
			for i := range maxManifestFiles {
				appendFile(fmt.Sprintf("f%d", i), 0)(man)
			}
		}, "more than"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake, st := newFakeStore(t)
			publishRecord(t, st, id, metaJSON(id), "payload")
			rewriteManifest(t, fake, id, tt.damage)
			_, _, _, err := st.Fetch(t.Context(), id)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Fetch = %v, want an error containing %q", err, tt.want)
			}
			if errors.Is(err, store.ErrNotFound) {
				t.Errorf("an invalid manifest must not read as absent: %v", err)
			}
			if _, statErr := os.Lstat(filepath.Join(st.staging, "escaped")); !errors.Is(statErr, os.ErrNotExist) {
				t.Errorf("a manifest path escaped the fetch dir: stat = %v", statErr)
			}
		})
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
	gateOn        string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
	if f.exportGate != nil && r.Method == http.MethodGet && strings.Contains(key, cmp.Or(f.gateOn, "/"+store.ExportDir)) {
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
	case r.Method == http.MethodGet:
		f.handleGetObject(w, key)
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

func (f *fakeS3) chunkPuts(prefix string) (count int, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, lengths := range f.putLengths {
		if strings.HasPrefix(key, prefix) {
			count += len(lengths)
			for _, n := range lengths {
				size += n
			}
		}
	}
	return count, size
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects))
}

func (f *fakeS3) age(by time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.objects {
		f.modified[key] = time.Now().Add(-by)
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
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `<Error><Code>InternalError</Code><Message>injected put failure</Message></Error>`)
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
			if before, _, ok := strings.Cut(key[len(prefix):], delim); ok {
				common := prefix + before + delim
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

func (f *fakeS3) handleGetObject(w http.ResponseWriter, key string) {
	body, ok := f.objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		return
	}
	writeMetadata(w.Header(), f.metadata[key])
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

func ageCache(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cache %s: %v", dir, err)
	}
	old := time.Now().Add(-2 * store.GenerationGrace)
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
			t.Fatalf("age cache: %v", err)
		}
	}
}

func publishRecord(t *testing.T, st *Store, id string, meta []byte, content string) {
	t.Helper()
	staging := stageRecord(t, st, id, meta, content)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("publish record: %v", err)
	}
}

func rewriteManifest(t *testing.T, fake *fakeS3, id string, change func(*manifest)) {
	t.Helper()
	prefix := "ck/" + manifestsDir + "/" + id + "/"
	keys := slices.DeleteFunc(fake.keys(), func(key string) bool { return !strings.HasPrefix(key, prefix) })
	if len(keys) != 1 {
		t.Fatalf("manifests of %s = %v, want one", id, keys)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	var man manifest
	if err := json.Unmarshal(fake.objects[keys[0]], &man); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	change(&man)
	raw, err := json.Marshal(man)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	fake.objects[keys[0]] = raw
}

func stageRecord(t *testing.T, st *Store, id string, meta []byte, content string) string {
	t.Helper()
	return storetest.StageRecord(t, st, id, string(meta), "disk.img", content)
}

func stageSparseRecord(t *testing.T, st *Store, id string, meta []byte, size int64, writes map[int64][]byte) (staging, path string) {
	t.Helper()
	staging = stageRecord(t, st, id, meta, "")
	return staging, storetest.WriteSparse(t, filepath.Join(staging, store.ExportDir, "disk.img"), size, writes)
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

func newFakeStore(t *testing.T) (*fakeS3, *Store) {
	t.Helper()
	fake := &fakeS3{objects: map[string][]byte{}}
	return fake, newTestStore(t, fake)
}

func newTestStore(t *testing.T, fake *fakeS3) *Store {
	t.Helper()
	return newTestStoreAt(t, fake, store.S3Config{})
}

func metaJSON(id string) []byte {
	return []byte(storetest.MetaJSON(id))
}

func newGatedStore(t *testing.T, gateOn string) (*fakeS3, *Store, func()) {
	t.Helper()
	fake := &fakeS3{objects: map[string][]byte{}, exportGate: make(chan struct{}), exportEntered: make(chan struct{}, 1), gateOn: gateOn}
	st := newTestStore(t, fake)
	release := sync.OnceFunc(func() { close(fake.exportGate) })
	t.Cleanup(release)
	return fake, st, release
}

func newTestStoreAt(t *testing.T, endpoint http.Handler, cfg store.S3Config) *Store {
	t.Helper()
	ts := httptest.NewServer(endpoint)
	t.Cleanup(ts.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
	cfg.Bucket, cfg.Prefix, cfg.Endpoint, cfg.Region, cfg.ForcePathStyle = "testbucket", "ck/", ts.URL, "us-east-1", true
	st, err := New(t.Context(), cfg, t.TempDir(), store.CheckpointIDRe, true)
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

type chunkGate struct {
	fake *fakeS3

	mu       sync.Mutex
	method   string
	want     int
	full     chan struct{}
	inflight int
	peak     int
	conns    map[string]struct{}
	opened   int
}

func (g *chunkGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if g.enter(r) {
		defer g.leave()
	}
	g.fake.ServeHTTP(w, r)
}

func (g *chunkGate) arm(method string, want int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.method, g.want, g.full, g.peak, g.opened = method, want, make(chan struct{}), 0, 0
}

func (g *chunkGate) result() (peak, opened int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak, g.opened
}

func (g *chunkGate) enter(r *http.Request) bool {
	g.mu.Lock()
	if _, ok := g.conns[r.RemoteAddr]; !ok {
		g.conns[r.RemoteAddr] = struct{}{}
		g.opened++
	}
	if r.Method != g.method || !strings.Contains(r.URL.Path, "/"+chunksDir+"/") {
		g.mu.Unlock()
		return false
	}
	g.inflight++
	g.peak = max(g.peak, g.inflight)
	full := g.full
	if g.inflight == g.want {
		select {
		case <-full:
		default:
			close(full)
		}
	}
	g.mu.Unlock()
	select {
	case <-full:
	case <-time.After(5 * time.Second):
	}
	return true
}

func (g *chunkGate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
}
