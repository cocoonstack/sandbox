package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCapsCocoonsGoMaxProcs(t *testing.T) {
	t.Setenv("GOMAXPROCS", "384")
	t.Setenv("SANDBOXD_RUN_TEST", "kept")
	bin := filepath.Join(t.TempDir(), "cocoon")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho \"$GOMAXPROCS $SANDBOXD_RUN_TEST\"\n"), 0o700); err != nil {
		t.Fatalf("write fake cocoon: %v", err)
	}
	out, err := New(bin, nil, nil, false, false, "").run(t.Context(), "version")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := strings.TrimPrefix(cocoonGoMaxProcs, "GOMAXPROCS=") + " kept"
	if got := strings.TrimSpace(string(out)); got != want {
		t.Errorf("cocoon env %q, want %q", got, want)
	}
}
