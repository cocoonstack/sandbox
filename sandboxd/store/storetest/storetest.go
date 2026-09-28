// Package storetest is the backend contract: every store.Store
// implementation must pass RunContract, so the pool sees identical
// semantics regardless of what sits underneath.
package storetest

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// RunContract drives one backend through plain and digested record lifecycles.
func RunContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000aa"

	publishRecord(t, st, id, `{"id":"`+id+`"}`, "disk.img", "snapshot-bytes")

	raw, err := st.ReadMeta(ctx, id)
	if err != nil || string(raw) != `{"id":"`+id+`"}` {
		t.Fatalf("ReadMeta: %q, %v", raw, err)
	}

	dir, meta, digest, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(meta) != `{"id":"`+id+`"}` {
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

	metas, err := st.Metas(ctx)
	if err != nil {
		t.Fatalf("Metas: %v", err)
	}
	if !slices.ContainsFunc(metas, func(r store.Record) bool { return string(r.Meta) == `{"id":"`+id+`"}` }) || len(metas) != 1 {
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
	if _, statErr := os.Stat(filepath.Join(dir, "disk.img")); !os.IsNotExist(statErr) {
		t.Errorf("first-generation file survived re-publish: %v", statErr)
	}
	if got, readErr := os.ReadFile(filepath.Join(dir, "disk2.img")); readErr != nil || string(got) != "second-gen" { //nolint:gosec // test path
		t.Errorf("second-generation export: %q, %v", got, readErr)
	}

	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err = st.ReadMeta(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadMeta after Delete: %v, want store.ErrNotFound", err)
	}
	if _, _, _, err = st.Fetch(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch after Delete: %v, want store.ErrNotFound", err)
	}
	if metas, err = st.Metas(ctx); err != nil || len(metas) != 0 {
		t.Fatalf("Metas after Delete: %d, %v", len(metas), err)
	}
	runDigestContract(t, st)
	runLabelsContract(t, st)
}

func runDigestContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000cc"
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage digested: %v", err)
	}
	writeExport(t, staging, "z.bin", "z")
	writeExport(t, staging, "nested/a.bin", "a")
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(`{"id":"`+id+`"}`), 0o600); err != nil {
		t.Fatalf("write digested meta: %v", err)
	}
	digest, err := st.PublishDigested(ctx, staging, id)
	if err != nil {
		t.Fatalf("PublishDigested: %v", err)
	}
	const want = "sha256:ad65b315de70e494c767969e65fc5de65c73846c4ebcdc7051abe08b056637ad"
	if digest != want {
		t.Fatalf("PublishDigested digest %q, want %q", digest, want)
	}
	_, _, fetchedDigest, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch digested: %v", err)
	}
	if fetchedDigest != want {
		t.Errorf("Fetch digest %q, want %q", fetchedDigest, want)
	}
	recs, err := st.Metas(ctx)
	if err != nil || len(recs) != 1 || recs[0].Digest != want {
		t.Errorf("Metas %+v, %v; want the one record with digest %q", recs, err, want)
	}
	rejectNonRegularReplacement(t, st, id, want)
	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete digested: %v", err)
	}
}

func runLabelsContract(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	const id = "ck_00000000000000dd"
	publishRecord(t, st, id, `{"id":"`+id+`"}`, "disk.img", "labeled")
	labelsOf := func() []byte {
		t.Helper()
		recs, metasErr := st.Metas(ctx)
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
	if recs, metasErr := st.Metas(ctx); metasErr != nil || len(recs) != 0 {
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
	if _, err = st.PublishDigested(t.Context(), staging, id); err == nil {
		t.Fatal("PublishDigested accepted a non-regular export entry")
	}
	dir, meta, digest, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch after rejected replacement: %v", err)
	}
	if string(meta) != `{"id":"`+id+`"}` || digest != wantDigest {
		t.Errorf("committed meta/digest after rejection = %q/%q, want original/%q", meta, digest, wantDigest)
	}
	content, err := os.ReadFile(filepath.Join(dir, "nested", "a.bin")) //nolint:gosec // test path
	if err != nil || string(content) != "a" {
		t.Errorf("committed export after rejection = %q, %v, want a", content, err)
	}
}

func publishRecord(t *testing.T, st store.Store, id, meta, file, content string) {
	t.Helper()
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage %s: %v", id, err)
	}
	writeExport(t, staging, file, content)
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(meta), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err = st.Publish(t.Context(), staging, id); err != nil {
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
