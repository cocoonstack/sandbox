package mesh

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/hashicorp/memberlist"
)

func newBoundMesh(t *testing.T, dataDir string) *Mesh {
	t.Helper()
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindPort = 0
	cfg.Logger = discardLogger()
	m, err := New(cfg, "self", "self:7777", nil, dataDir)
	if err != nil {
		t.Fatalf("new mesh: %v", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return m
}

func TestEpochRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "e")
	if loadEpoch(p) != 0 {
		t.Error("missing epoch file must read 0")
	}
	if err := storeEpoch(p, 42); err != nil {
		t.Fatalf("store: %v", err)
	}
	if got := loadEpoch(p); got != 42 {
		t.Errorf("loadEpoch = %d, want 42", got)
	}
}

func TestEpochSeededFromPersistedFloor(t *testing.T) {
	dir := t.TempDir()
	huge := uint64(1) << 62 // above any plausible wall-clock nanos
	if err := storeEpoch(filepath.Join(dir, "mesh-epoch"), huge); err != nil {
		t.Fatalf("store: %v", err)
	}
	m := newBoundMesh(t, dir)
	if m.self.Epoch != huge {
		t.Errorf("epoch seeded %d, want persisted floor %d (a backwards clock must not regress it)", m.self.Epoch, huge)
	}
}

func TestUpdateSelfPersistsEpoch(t *testing.T) {
	dir := t.TempDir()
	m := newBoundMesh(t, dir)
	before := m.self.Epoch
	m.UpdateSelf(map[string]int{"k": 1}, nil)
	if got := loadEpoch(filepath.Join(dir, "mesh-epoch")); got <= before {
		t.Errorf("persisted epoch %d did not advance past the seed %d", got, before)
	}
}

func TestPersistEpochMonotonic(t *testing.T) {
	dir := t.TempDir()
	m := newBoundMesh(t, dir)
	base := m.self.Epoch // New already seeded a wall-clock floor
	high := base + 1000
	if err := m.persistEpoch(high); err != nil {
		t.Fatalf("persist high: %v", err)
	}
	// Concurrent lower values must never regress the on-disk floor below high.
	var wg sync.WaitGroup
	for e := base + 1; e < high; e++ {
		wg.Go(func() {
			if err := m.persistEpoch(e); err != nil {
				t.Errorf("persist %d: %v", e, err)
			}
		})
	}
	wg.Wait()
	if got := loadEpoch(filepath.Join(dir, "mesh-epoch")); got != high {
		t.Errorf("persisted epoch %d, want the %d floor held against lower concurrent writes", got, high)
	}
}

func TestConfigDigestMismatch(t *testing.T) {
	m := newTestMesh(t, "self")
	m.SetSelfDigest("self-digest")
	m.merge([]NodeState{{NodeID: "peerA", Addr: "a:1", Epoch: 1, Digest: "self-digest"}})
	m.merge([]NodeState{{NodeID: "peerB", Addr: "b:1", Epoch: 1, Digest: "other-digest"}})
	if n := m.ConfigMismatches(); n != 1 {
		t.Errorf("ConfigMismatches = %d, want 1 (only peerB diverges)", n)
	}
}
