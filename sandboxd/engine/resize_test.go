package engine

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestResizePassesTheSizeAndAddsStorageOnlyWhenSet(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "trace")
	t.Setenv("COCOON_TEST_TRACE", trace)
	fakeCocoon(t, "#!/bin/sh\necho \"$*\" >> \"$COCOON_TEST_TRACE\"\n")
	e := newTestEngine()
	spec, _ := types.SizeXLarge.Spec()

	if err := e.Resize(t.Context(), "sbx-1", spec, ""); err != nil {
		t.Fatalf("resize without storage: %v", err)
	}
	if err := e.Resize(t.Context(), "sbx-1", spec, "40G"); err != nil {
		t.Fatalf("resize with storage: %v", err)
	}

	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"vm resize --cpu 4 --memory 8G sbx-1",
		"vm resize --cpu 4 --memory 8G --storage 40G sbx-1",
	}
	if calls := strings.Split(strings.TrimSpace(string(data)), "\n"); !slices.Equal(calls, want) {
		t.Errorf("cocoon calls\n%q\nwant\n%q", calls, want)
	}
}

func TestResizeSurfacesCocoonsRefusal(t *testing.T) {
	fakeCocoon(t, "#!/bin/sh\necho 'Error: vm resize: vm sbx-1 is not stopped; run vm stop first' >&2\nexit 1\n")
	spec, _ := types.SizeXLarge.Spec()

	err := newTestEngine().Resize(t.Context(), "sbx-1", spec, "")

	if err == nil || !strings.Contains(err.Error(), "is not stopped") {
		t.Errorf("resize error = %v, want cocoon's refusal", err)
	}
}
