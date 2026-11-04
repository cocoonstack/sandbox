package mesh

import (
	"sync"
	"testing"

	"github.com/hashicorp/memberlist"
)

func TestUpdateSelfConcurrentDropsNothing(t *testing.T) {
	m := newBoundMesh(t)
	base := m.self.Epoch
	for i := range 100 {
		a := map[string]int{"a": i + 1}
		b := map[string]int{"b": i + 1}
		var wg sync.WaitGroup
		wg.Go(func() { m.UpdateSelf(a, nil, nil) })
		wg.Go(func() { m.UpdateSelf(b, nil, nil) })
		wg.Wait()

		want := base + uint64(2*(i+1))
		if m.self.Epoch != want {
			t.Fatalf("round %d: epoch %d, want %d (an update was dropped)", i, m.self.Epoch, want)
		}
	}
}

func TestUpdateSelfBumpsOnlyWhenVolumesChange(t *testing.T) {
	m := newBoundMesh(t)
	base := m.self.Epoch
	m.UpdateSelf(nil, nil, []string{"dataset"})
	if m.self.Epoch != base+1 {
		t.Fatalf("first volume publish epoch=%d, want %d", m.self.Epoch, base+1)
	}
	m.UpdateSelf(nil, nil, []string{"dataset"})
	if m.self.Epoch != base+1 {
		t.Errorf("unchanged volume republished at epoch=%d", m.self.Epoch)
	}
	m.UpdateSelf(nil, nil, []string{"dataset", "weights"})
	if m.self.Epoch != base+2 {
		t.Errorf("changed volume epoch=%d, want %d", m.self.Epoch, base+2)
	}
}

func TestConfigDigestMismatch(t *testing.T) {
	m := newTestMesh(t, "self")
	mergeStates(t, m, []NodeState{{NodeID: "peerA", Addr: "a:1", Epoch: 1, Digest: "self-digest"}})
	mergeStates(t, m, []NodeState{{NodeID: "peerB", Addr: "b:1", Epoch: 1, Digest: "other-digest"}})
	if n := m.ConfigMismatches(); n != 1 {
		t.Errorf("ConfigMismatches = %d, want 1 (only peerB diverges)", n)
	}
}

func TestUpdateDigestRepublishesUnderAFreshEpoch(t *testing.T) {
	m := newBoundMesh(t)
	base := m.self.Epoch
	m.UpdateDigest("d1")
	if m.self.Epoch != base {
		t.Errorf("an unchanged digest republished at epoch=%d", m.self.Epoch)
	}
	m.UpdateDigest("d2")
	if m.self.Epoch != base+1 || m.view["self"].Digest != "d2" {
		t.Errorf("epoch=%d view digest %q, want epoch %d and d2", m.self.Epoch, m.view["self"].Digest, base+1)
	}
}

func newBoundMesh(t *testing.T) *Mesh {
	t.Helper()
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindPort = 0
	cfg.Logger = discardLogger()
	m, err := New(t.Context(), cfg, NodeState{NodeID: "self", Addr: "self:7777", ClientAddr: "https://self.example", Digest: "d1"}, nil)
	if err != nil {
		t.Fatalf("new mesh: %v", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return m
}
