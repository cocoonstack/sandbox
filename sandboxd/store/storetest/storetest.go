// Package storetest is the backend contract: every store.Store
// implementation must pass RunContract, so the pool sees identical
// semantics regardless of what sits underneath.
package storetest

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

// RunContract drives one backend through the full checkpoint lifecycle.
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

	dir, release, err := st.Fetch(ctx, id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
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
	if err = st.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err = st.ReadMeta(ctx, id); err == nil {
		t.Fatal("ReadMeta after Delete: want an error")
	}
	if metas, err = st.Metas(ctx); err != nil || len(metas) != 0 {
		t.Fatalf("Metas after Delete: %d, %v", len(metas), err)
	}
}

func writeExport(t *testing.T, staging, name, content string) {
	t.Helper()
	dir := filepath.Join(staging, store.ExportDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("write export: %v", err)
	}
}
