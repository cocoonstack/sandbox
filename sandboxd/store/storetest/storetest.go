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

	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	writeExport(t, staging, "disk.img", "snapshot-bytes")
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(`{"id":"`+id+`"}`), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err = st.Publish(ctx, staging, id); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	raw, err := st.ReadMeta(ctx, id)
	if err != nil || string(raw) != `{"id":"`+id+`"}` {
		t.Fatalf("ReadMeta: %q, %v", raw, err)
	}

	dir, meta, digest, release, err := st.Fetch(ctx, id)
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
	release()

	// A half-published checkpoint (no meta) is invisible to Metas.
	orphan, err := st.Stage("ck_00000000000000bb")
	if err != nil {
		t.Fatalf("Stage orphan: %v", err)
	}
	writeExport(t, orphan, "disk.img", "torn")

	metas, err := st.Metas(ctx)
	if err != nil {
		t.Fatalf("Metas: %v", err)
	}
	if !slices.ContainsFunc(metas, func(m []byte) bool { return string(m) == `{"id":"`+id+`"}` }) || len(metas) != 1 {
		t.Fatalf("Metas: %d records %q, want exactly the published one", len(metas), metas)
	}

	if err = st.SweepStaging(); err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}

	// Re-publish replaces: the second generation's export fully supersedes
	// the first (a leftover first-generation file would corrupt a clone).
	second, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage second: %v", err)
	}
	writeExport(t, second, "disk2.img", "second-gen")
	if err = os.WriteFile(filepath.Join(second, store.MetaFile), []byte(`{"id":"`+id+`","gen":2}`), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err = st.Publish(ctx, second, id); err != nil {
		t.Fatalf("Publish second: %v", err)
	}
	dir, meta, digest, release, err = st.Fetch(ctx, id)
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
	release()

	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err = st.ReadMeta(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadMeta after Delete: %v, want store.ErrNotFound", err)
	}
	if _, _, _, _, err = st.Fetch(ctx, id); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Fetch after Delete: %v, want store.ErrNotFound", err)
	}
	if metas, err = st.Metas(ctx); err != nil || len(metas) != 0 {
		t.Fatalf("Metas after Delete: %d, %v", len(metas), err)
	}
	runDigestContract(t, st)
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
	_, _, fetchedDigest, release, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch digested: %v", err)
	}
	if fetchedDigest != want {
		t.Errorf("Fetch digest %q, want %q", fetchedDigest, want)
	}
	release()
	rejectNonRegularReplacement(t, st, id, want)
	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete digested: %v", err)
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
	dir, meta, digest, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch after rejected replacement: %v", err)
	}
	defer release()
	if string(meta) != `{"id":"`+id+`"}` || digest != wantDigest {
		t.Errorf("committed meta/digest after rejection = %q/%q, want original/%q", meta, digest, wantDigest)
	}
	content, err := os.ReadFile(filepath.Join(dir, "nested", "a.bin")) //nolint:gosec // test path
	if err != nil || string(content) != "a" {
		t.Errorf("committed export after rejection = %q, %v, want a", content, err)
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
