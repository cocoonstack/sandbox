package e2e

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

func TestReaderServesAHibernatedClaimUntilAWriteWakesIt(t *testing.T) {
	st := startConfigStack(t, "node-token", &config.Config{
		Pools: []config.PoolSpec{{PoolKey: testKey, Warm: 2, Reader: true}},
	})
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "work"), 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "work", "out.txt"), []byte("result"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	st.eng.mu.Lock()
	st.eng.readerRoot = root
	st.eng.mu.Unlock()
	sb, err := st.client.New(t.Context(), testKey.Template)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = sb.Close() }()
	if err = sb.Hibernate(t.Context()); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	waitFor(t, func() bool {
		pools, _ := st.mgr.Info()
		return slices.ContainsFunc(pools, func(p pool.PoolInfo) bool { return p.Key == testKey && p.Warm >= 1 })
	})

	got, err := sb.ReadFile(t.Context(), "/work/out.txt")
	if err != nil || string(got) != "result" {
		t.Fatalf("ReadFile of a hibernated claim = %q, %v, want the reader's file", got, err)
	}
	if !hibernated(t, st, sb.ID) {
		t.Fatal("reading a file woke the claim")
	}
	st.eng.mu.Lock()
	started, attached := len(st.eng.readerStarts), len(st.eng.readerAttaches)
	st.eng.mu.Unlock()
	if started != 1 || attached != 1 {
		t.Errorf("%d readers started over %d snapshot copies, want one of each", started, attached)
	}

	if out, execErr := sb.Exec(t.Context(), "echo", "woke"); execErr != nil || out != "woke\n" {
		t.Fatalf("Exec on the sleeping claim = %q, %v, want it to wake and answer", out, execErr)
	}
	if hibernated(t, st, sb.ID) {
		t.Error("an exec left the claim hibernated")
	}
}

func hibernated(t *testing.T, st *stack, id string) bool {
	t.Helper()
	for _, s := range st.mgr.Sandboxes("", "", nil) {
		if s.ID == id {
			return s.Hibernated
		}
	}
	t.Fatalf("claim %s not listed", id)
	return false
}
