// Package storetest is the backend contract every store.Store implementation must pass.
package storetest

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// RunContract drives one backend through plain and digested record lifecycles.
func RunContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000aa"

	publishRecord(t, st, id, MetaJSON(id), "disk.img", "snapshot-bytes")

	raw, err := st.ReadMeta(ctx, id)
	if err != nil || string(raw) != MetaJSON(id) {
		t.Fatalf("ReadMeta: %q, %v", raw, err)
	}

	dir, meta, digest, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(meta) != MetaJSON(id) {
		t.Fatalf("Fetch meta: %q, want the published record", meta)
	}
	if digest != "" {
		t.Fatalf("Fetch digest after plain Publish: %q, want empty", digest)
	}
	got, err := os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil || string(got) != "snapshot-bytes" {
		t.Fatalf("fetched export: %q, %v", got, err)
	}

	orphan, err := st.Stage("ck_00000000000000bb")
	if err != nil {
		t.Fatalf("Stage orphan: %v", err)
	}
	writeExport(t, orphan, "disk.img", "torn")

	metas, err := st.Metas(ctx, "")
	if err != nil {
		t.Fatalf("Metas: %v", err)
	}
	if !slices.ContainsFunc(metas, func(r store.Record) bool { return string(r.Meta) == MetaJSON(id) }) || len(metas) != 1 {
		t.Fatalf("Metas: %d records %q, want exactly the published one", len(metas), metas)
	}

	if err = st.SweepStaging(); err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}

	publishRecord(t, st, id, `{"id":"`+id+`","gen":2}`, "disk2.img", "second-gen")
	dir, meta, digest, err = st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if string(meta) != `{"id":"`+id+`","gen":2}` {
		t.Fatalf("Fetch meta after re-publish: %q, want the second generation", meta)
	}
	if digest != "" {
		t.Fatalf("Fetch digest after plain re-publish: %q, want empty", digest)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "disk.img")); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("first-generation file survived re-publish: %v", statErr)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk2.img")); readErr != nil || string(got) != "second-gen" { //nolint:gosec // test path
		t.Errorf("second-generation export: %q, %v", got, readErr)
	}

	runFetchFileContract(t, st, id)

	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err = st.FetchFile(ctx, id, "disk2.img", filepath.Join(t.TempDir(), "deleted")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("FetchFile after Delete: %v, want store.ErrNotFound", err)
	}
	if _, err = st.ReadMeta(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadMeta after Delete: %v, want store.ErrNotFound", err)
	}
	if _, _, _, err = st.Fetch(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch after Delete: %v, want store.ErrNotFound", err)
	}
	if metas, err = st.Metas(ctx, ""); err != nil || len(metas) != 0 {
		t.Fatalf("Metas after Delete: %d, %v", len(metas), err)
	}
	runDigestContract(t, st)
	runLabelsContract(t, st)
	runPrefixContract(t, st)
}

// RunFallbackContract drives wrapped, a store over primary and fallback, through records only the fallback holds, records it writes, and an id both hold; every store must be labeled and empty.
func RunFallbackContract(t *testing.T, primary, fallback, wrapped store.Store) {
	t.Helper()
	runFallbackOnlyContract(t, fallback, wrapped)
	written := runFallbackWriteContract(t, primary, fallback, wrapped)
	runFallbackShadowContract(t, primary, fallback, wrapped, written)
}

// runFallbackOnlyContract reads, labels, lists and deletes a record only the fallback holds.
func runFallbackOnlyContract(t *testing.T, fallback, wrapped store.Store) {
	t.Helper()
	ctx := t.Context()
	const old = "ck_00000000000000f1"
	publishRecord(t, fallback, old, MetaJSON(old), "disk.img", "from-fallback")
	if raw, err := wrapped.ReadMeta(ctx, old); err != nil || string(raw) != MetaJSON(old) {
		t.Fatalf("ReadMeta of a fallback record: %q, %v", raw, err)
	}
	if digest, err := wrapped.ReadDigest(ctx, old); err != nil || digest != "" {
		t.Fatalf("ReadDigest of a fallback record: %q, %v; want its empty digest", digest, err)
	}
	dir, meta, _, err := wrapped.Fetch(ctx, old)
	if err != nil || string(meta) != MetaJSON(old) {
		t.Fatalf("Fetch of a fallback record: %q, %v", meta, err)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "from-fallback" { //nolint:gosec // test path
		t.Fatalf("fetched fallback export: %q, %v", got, readErr)
	}
	dst := filepath.Join(t.TempDir(), "file")
	if err = wrapped.FetchFile(ctx, old, "disk.img", dst); err != nil {
		t.Fatalf("FetchFile of a fallback record: %v", err)
	}
	if got, readErr := os.ReadFile(dst); readErr != nil || string(got) != "from-fallback" { //nolint:gosec // test path
		t.Fatalf("fetched fallback file: %q, %v", got, readErr)
	}
	if err = wrapped.SetLabels(ctx, old, []byte(`{"a":"1"}`)); err != nil {
		t.Fatalf("SetLabels of a fallback record: %v", err)
	}
	for which, st := range map[string]store.Store{"fallback": fallback, "wrapped": wrapped} {
		recs, metasErr := st.Metas(ctx, "")
		if metasErr != nil || len(recs) != 1 || recs[0].ID != old || string(recs[0].Labels) != `{"a":"1"}` {
			t.Fatalf("%s Metas: %+v, %v; want %s once, labeled beside its meta", which, recs, metasErr, old)
		}
	}
	if err = wrapped.Delete(ctx, old); err != nil {
		t.Fatalf("Delete of a fallback record: %v", err)
	}
	requireAbsent(t, fallback, "fallback", old)
	if _, _, _, err = wrapped.Fetch(ctx, old); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch after Delete of a fallback record: %v, want store.ErrNotFound", err)
	}
}

// runFallbackWriteContract publishes through wrapped and returns the ids it wrote, which must land in the primary only.
func runFallbackWriteContract(t *testing.T, primary, fallback, wrapped store.Store) []string {
	t.Helper()
	ctx := t.Context()
	const fresh, digested = "ck_00000000000000f2", "ck_00000000000000f4"
	publishRecord(t, wrapped, fresh, MetaJSON(fresh), "disk.img", "new")
	if _, err := PublishDigested(ctx, wrapped, StageRecord(t, wrapped, digested, MetaJSON(digested), "disk.img", "new"), digested); err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	for _, id := range []string{fresh, digested} {
		if _, err := primary.ReadMeta(ctx, id); err != nil {
			t.Fatalf("primary ReadMeta of %s written through the wrapper: %v", id, err)
		}
		requireAbsent(t, fallback, "fallback", id)
	}
	return []string{fresh, digested}
}

// runFallbackShadowContract checks an id both stores hold, as a template promoted again after the move, reads as the primary's copy; it then deletes that id and written from both.
func runFallbackShadowContract(t *testing.T, primary, fallback, wrapped store.Store, written []string) {
	t.Helper()
	ctx := t.Context()
	const both = "ck_00000000000000f3"
	staging := StageRecord(t, fallback, both, `{"id":"`+both+`","v":"old"}`, "disk.img", "older")
	writeExport(t, staging, "old-only.img", "stale")
	if err := fallback.Publish(ctx, staging, both, ""); err != nil {
		t.Fatalf("publish the fallback copy: %v", err)
	}
	if err := fallback.SetLabels(ctx, both, []byte(`{"from":"fallback"}`)); err != nil {
		t.Fatalf("label the fallback copy: %v", err)
	}
	publishRecord(t, primary, both, `{"id":"`+both+`","v":"new"}`, "disk.img", "newer")
	if raw, readErr := wrapped.ReadMeta(ctx, both); readErr != nil || string(raw) != `{"id":"`+both+`","v":"new"}` {
		t.Fatalf("ReadMeta of an id both hold: %q, %v; want the primary's", raw, readErr)
	}
	dir, _, _, err := wrapped.Fetch(ctx, both)
	if err != nil {
		t.Fatalf("Fetch of an id both hold: %v", err)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk.img")); readErr != nil || string(got) != "newer" { //nolint:gosec // test path
		t.Fatalf("fetched export of an id both hold: %q, %v; want the primary's", got, readErr)
	}
	if err = wrapped.FetchFile(ctx, both, "old-only.img", filepath.Join(t.TempDir(), "old")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("FetchFile of a file only the fallback copy has: %v, want store.ErrNotFound from the primary's record", err)
	}
	recs, err := wrapped.Metas(ctx, "")
	if err != nil || len(recs) != 1+len(written) {
		t.Fatalf("Metas with an id in both: %+v, %v; want %d records", recs, err, 1+len(written))
	}
	for _, r := range recs {
		if r.ID == both && (string(r.Meta) != `{"id":"`+both+`","v":"new"}` || r.Labels != nil) {
			t.Fatalf("Metas record of an id both hold: %q labels %q, want the primary's, unlabeled", r.Meta, r.Labels)
		}
	}
	for _, id := range append([]string{both}, written...) {
		if err = wrapped.Delete(ctx, id); err != nil {
			t.Fatalf("Delete %s: %v", id, err)
		}
		requireAbsent(t, primary, "primary", id)
		requireAbsent(t, fallback, "fallback", id)
	}
	if recs, err = wrapped.Metas(ctx, ""); err != nil || len(recs) != 0 {
		t.Fatalf("Metas after every Delete: %+v, %v", recs, err)
	}
}

func requireAbsent(t *testing.T, st store.Store, which, id string) {
	t.Helper()
	if _, err := st.ReadMeta(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%s ReadMeta %s: %v, want store.ErrNotFound", which, id, err)
	}
}

// PublishDigested prepares and commits a digested record in one call.
func PublishDigested(ctx context.Context, st store.Store, staging, id string) (string, error) {
	digest, commit, err := st.PrepareDigested(ctx, staging, id, "")
	if err != nil {
		return "", err
	}
	if err = commit(ctx); err != nil {
		return "", err
	}
	return digest, nil
}

// MetaJSON is the bare record meta the contract and its callers publish.
func MetaJSON(id string) string {
	return `{"id":"` + id + `"}`
}

// StageRecord stages one export file and its meta, ready to publish.
func StageRecord(t *testing.T, st store.Store, id, meta, file, content string) string {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage %s: %v", id, err)
	}
	writeExport(t, staging, file, content)
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(meta), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return staging
}

// WriteSparse creates path at size with only writes allocated.
func WriteSparse(t testing.TB, path string, size int64, writes map[int64][]byte) string {
	t.Helper()
	f, err := os.Create(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if err = f.Truncate(size); err != nil {
		t.Fatalf("size %s: %v", path, err)
	}
	for off, data := range writes {
		if _, err = f.WriteAt(data, off); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return path
}

func FileSum(t testing.TB, path string) [sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test path
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

func AllocatedBytes(t testing.TB, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Sys().(*syscall.Stat_t).Blocks * 512
}

func WaitStacks(t testing.TB, want int, match func(stack string) bool) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for range 5000 {
		matched := 0
		for stack := range strings.SplitSeq(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if match(stack) {
				matched++
			}
		}
		if matched >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fewer than %d goroutines reached the expected state", want)
}

func runFetchFileContract(t *testing.T, st store.Store, id string) {
	t.Helper()
	ctx := t.Context()
	dst := filepath.Join(t.TempDir(), "fetched")
	if err := st.FetchFile(ctx, id, "disk2.img", dst); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if got, readErr := os.ReadFile(dst); readErr != nil || string(got) != "second-gen" { //nolint:gosec // test path
		t.Errorf("fetched file: %q, %v", got, readErr)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if err := st.FetchFile(ctx, id, "absent.img", missing); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("FetchFile of an absent file: %v, want store.ErrNotFound", err)
	}
	for _, leftover := range []string{missing, missing + ".part"} {
		if _, statErr := os.Stat(leftover); !errors.Is(statErr, os.ErrNotExist) {
			t.Errorf("a failed FetchFile left %s behind: %v", leftover, statErr)
		}
	}
}

func runDigestContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000cc"
	staging := StageRecord(t, st, id, MetaJSON(id), "z.bin", "z")
	writeExport(t, staging, "nested/a.bin", "a")
	digest, commit, err := st.PrepareDigested(ctx, staging, id, "")
	if err != nil {
		t.Fatalf("PrepareDigested: %v", err)
	}
	const want = "sha256:ad65b315de70e494c767969e65fc5de65c73846c4ebcdc7051abe08b056637ad"
	if digest != want {
		t.Fatalf("PrepareDigested digest %q, want %q", digest, want)
	}
	if _, err = st.ReadMeta(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadMeta of a prepared record: %v, want store.ErrNotFound until its commit", err)
	}
	if err = commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	_, _, fetchedDigest, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch digested: %v", err)
	}
	if fetchedDigest != want {
		t.Errorf("Fetch digest %q, want %q", fetchedDigest, want)
	}
	recs, err := st.Metas(ctx, "")
	if err != nil || len(recs) != 1 || recs[0].Digest != want {
		t.Errorf("Metas %+v, %v; want the one record with digest %q", recs, err, want)
	}
	rejectNonRegularReplacement(t, st, id, want)
	staging = StageRecord(t, st, id, `{"id":"`+id+`","gen":3}`, "z.bin", "zz")
	next, commit, err := st.PrepareDigested(ctx, staging, id, "")
	if err != nil {
		t.Fatalf("PrepareDigested replacement: %v", err)
	}
	if held, readErr := st.ReadDigest(ctx, id); readErr != nil || held != want {
		t.Fatalf("ReadDigest while a replacement is prepared: %q, %v, want the committed %q", held, readErr, want)
	}
	if err = commit(ctx); err != nil {
		t.Fatalf("commit replacement: %v", err)
	}
	if held, readErr := st.ReadDigest(ctx, id); readErr != nil || held != next {
		t.Fatalf("ReadDigest after the replacement commit: %q, %v, want %q", held, readErr, next)
	}
	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete digested: %v", err)
	}
}

func runLabelsContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000dd"
	publishRecord(t, st, id, MetaJSON(id), "disk.img", "labeled")
	labelsOf := func() []byte {
		t.Helper()
		recs, metasErr := st.Metas(ctx, "")
		if metasErr != nil || len(recs) != 1 {
			t.Fatalf("Metas: %d records, %v; want the labeled one", len(recs), metasErr)
		}
		return recs[0].Labels
	}
	if got := labelsOf(); got != nil {
		t.Fatalf("labels before any set: %q, want none", got)
	}
	for _, want := range []string{`{"a":"1"}`, `{"b":"2"}`} {
		if err := st.SetLabels(ctx, id, []byte(want)); err != nil {
			t.Fatalf("SetLabels %s: %v", want, err)
		}
		if got := labelsOf(); string(got) != want {
			t.Fatalf("labels %q, want %s replaced whole", got, want)
		}
	}
	if _, _, _, err := st.Fetch(ctx, id); err != nil {
		t.Fatalf("Fetch after SetLabels: %v, want the export untouched", err)
	}
	if err := st.SetLabels(ctx, id, nil); err != nil {
		t.Fatalf("clear labels: %v", err)
	}
	if got := labelsOf(); got != nil {
		t.Fatalf("labels after clear: %q, want none", got)
	}
	if err := st.SetLabels(ctx, id, []byte(`{"c":"3"}`)); err != nil {
		t.Fatalf("SetLabels before delete: %v", err)
	}
	if err := st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete labeled: %v", err)
	}
	if recs, metasErr := st.Metas(ctx, ""); metasErr != nil || len(recs) != 0 {
		t.Fatalf("Metas after Delete: %d records, %v; want the labels gone with the record", len(recs), metasErr)
	}
	publishRecord(t, st, id, `{"id":"`+id+`","v":2}`, "disk.img", "again")
	if got := labelsOf(); got != nil {
		t.Fatalf("labels %q resurfaced on a record published after Delete", got)
	}
	if err := st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete republished: %v", err)
	}
}

func runPrefixContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	ckpt, archive := store.CheckpointID("00000000000000ee"), store.ArchiveID("00000000000000ee")
	publishRecord(t, st, ckpt, MetaJSON(ckpt), "disk.img", "checkpoint")
	publishRecord(t, st, archive, MetaJSON(archive), "disk.img", "archive")
	for prefix, want := range map[string]string{store.CheckpointPrefix: ckpt, store.ArchivePrefix: archive} {
		recs, err := st.Metas(ctx, prefix)
		if err != nil || len(recs) != 1 || string(recs[0].Meta) != MetaJSON(want) || recs[0].ID != want {
			t.Fatalf("Metas %s: %+v, %v; want only %s, with its id", prefix, recs, err, want)
		}
	}
	for _, id := range []string{ckpt, archive} {
		if err := st.Delete(ctx, id); err != nil {
			t.Fatalf("Delete %s: %v", id, err)
		}
	}
}

func rejectNonRegularReplacement(t *testing.T, st store.Store, id, wantDigest string) {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage non-regular replacement: %v", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	export := filepath.Join(staging, store.ExportDir)
	if err = os.MkdirAll(export, 0o750); err != nil {
		t.Fatalf("mkdir non-regular export: %v", err)
	}
	if err = os.Symlink("target", filepath.Join(export, "link")); err != nil {
		t.Fatalf("symlink export: %v", err)
	}
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(`{"id":"`+id+`","gen":2}`), 0o600); err != nil {
		t.Fatalf("write non-regular meta: %v", err)
	}
	if _, _, err = st.PrepareDigested(t.Context(), staging, id, ""); err == nil {
		t.Fatal("PrepareDigested accepted a non-regular export entry")
	}
	dir, meta, digest, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch after rejected replacement: %v", err)
	}
	if string(meta) != MetaJSON(id) || digest != wantDigest {
		t.Errorf("committed meta/digest after rejection = %q/%q, want original/%q", meta, digest, wantDigest)
	}
	content, err := os.ReadFile(filepath.Join(dir, "nested", "a.bin")) //nolint:gosec // test path
	if err != nil || string(content) != "a" {
		t.Errorf("committed export after rejection = %q, %v, want a", content, err)
	}
}

func publishRecord(t *testing.T, st store.Store, id, meta, file, content string) {
	t.Helper()
	staging := StageRecord(t, st, id, meta, file, content)
	if err := st.Publish(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("Publish %s: %v", id, err)
	}
}

func writeExport(t *testing.T, staging, name, content string) {
	t.Helper()
	path := filepath.Join(staging, store.ExportDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
}
