package engine

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunCapsCocoonsGoMaxProcs(t *testing.T) {
	t.Setenv("SANDBOXD_RUN_TEST", "kept")
	bin := filepath.Join(t.TempDir(), "cocoon")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho \"$GOMAXPROCS $SANDBOXD_RUN_TEST\"\n"), 0o700); err != nil {
		t.Fatalf("write fake cocoon: %v", err)
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	for _, tt := range []struct {
		procs int
		want  string
	}{
		{384, "4 kept"},
		{4, "4 kept"},
		{2, "2 kept"},
		{1, "1 kept"},
	} {
		runtime.GOMAXPROCS(tt.procs)
		out, err := New(bin, nil, nil, false, false, "").run(t.Context(), "version")
		if err != nil {
			t.Fatalf("run at %d Ps: %v", tt.procs, err)
		}
		if got := strings.TrimSpace(string(out)); got != tt.want {
			t.Errorf("at %d Ps cocoon env %q, want %q", tt.procs, got, tt.want)
		}
	}
}
