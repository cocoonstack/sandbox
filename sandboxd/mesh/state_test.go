package mesh

import (
	"os"
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

func TestLoadEpochRejectsImplausibleValue(t *testing.T) {
	p := filepath.Join(t.TempDir(), "e")
	// A saturated value must not seed a counter that +1 can't climb past.
	if err := os.WriteFile(p, []byte("18446744073709551615"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := loadEpoch(p); got != 0 {
		t.Errorf("loadEpoch of a MaxUint64 file = %d, want 0 (treated as corrupt)", got)
	}
}

func TestEpochSeededAbovePersistedFloor(t *testing.T) {
	dir := t.TempDir()
	huge := uint64(1) << 62 // above any plausible wall-clock nanos
	if err := storeEpoch(filepath.Join(dir, "mesh-epoch"), huge); err != nil {
		t.Fatalf("store: %v", err)
	}
	m := newBoundMesh(t, dir)
	// Strictly above: seeding at the persisted value would tie peers' stale copy.
	if m.self.Epoch != huge+1 {
		t.Errorf("epoch seeded %d, want persisted floor + 1 = %d (a backwards clock must land above peers' last-seen epoch)", m.self.Epoch, huge+1)
	}
}

func TestUpdateSelfPersistFailKeepsOldState(t *testing.T) {
	dir := t.TempDir()
	m := newBoundMesh(t, dir)
	before := m.self.Epoch
	// A non-existent dir makes the durable write fail: the epoch must not publish.
	m.epochPath = filepath.Join(dir, "gone", "mesh-epoch")
	m.UpdateSelf(map[string]int{"k": 1}, nil)
	if m.self.Epoch != before {
		t.Errorf("self epoch advanced to %d on a failed persist, want %d held", m.self.Epoch, before)
	}
	if len(m.self.Pools) != 0 {
		t.Errorf("self pools published %v despite a failed persist", m.self.Pools)
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
