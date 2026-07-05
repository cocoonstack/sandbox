package pool

import (
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestStoreRoundTrip(t *testing.T) {
	s := newStore(t.TempDir())
	claims := map[string]*types.Sandbox{
		"sb_a": {ID: "sb_a", VMName: "sbx-1", Key: testKey, Token: "t1", Deadline: time.Now().Add(time.Minute).UTC(), VsockSocket: "/v/1"},
		"sb_b": {ID: "sb_b", VMName: "sbx-2", Key: testKey, Token: "t2"},
	}

	if err := s.save(claims); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !maps.EqualFunc(got, claims, func(a, b *types.Sandbox) bool { return *a == *b }) {
		t.Errorf("got %+v, want %+v", got, claims)
	}
}

func TestStoreLoadMissingFile(t *testing.T) {
	got, err := newStore(t.TempDir()).load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d entries, want 0", len(got))
	}
}

func TestStoreLoadCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "claims.json"), []byte("{oops"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if _, err := newStore(dir).load(); err == nil {
		t.Error("load succeeded on corrupt file")
	}
}
