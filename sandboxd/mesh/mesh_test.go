package mesh

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestMergeKeepsHigherEpoch(t *testing.T) {
	m := newTestMesh(t, "a")
	m.merge([]NodeState{{NodeID: "b", Addr: "b:7777", Epoch: 1, Pools: map[string]int{"k": 2}}})
	m.merge([]NodeState{{NodeID: "b", Addr: "b:7777", Epoch: 3, Pools: map[string]int{"k": 5}}})
	m.merge([]NodeState{{NodeID: "b", Addr: "b:7777", Epoch: 2, Pools: map[string]int{"k": 9}}}) // stale

	got := 0
	for _, st := range m.Members() {
		if st.NodeID == "b" {
			got = st.Pools["k"]
		}
	}
	if got != 5 {
		t.Errorf("warm count %d, want 5 (epoch 3 wins)", got)
	}
}

func TestMergeNeverOverwritesSelf(t *testing.T) {
	m := newTestMesh(t, "a")
	m.UpdateSelf(map[string]int{"k": 3}, nil)
	// A peer claiming to be "a" must not clobber our authoritative self entry.
	m.merge([]NodeState{{NodeID: "a", Addr: "evil:9999", Epoch: 999, Pools: map[string]int{"k": 0}}})

	for _, st := range m.Members() {
		if st.NodeID == "a" && (st.Addr != "a:7777" || st.Pools["k"] != 3) {
			t.Errorf("self overwritten: %+v", st)
		}
	}
}

func TestCandidatesExcludeSelfAndEmpty(t *testing.T) {
	m := newTestMesh(t, "a")
	m.UpdateSelf(map[string]int{"k": 5}, nil) // self has warm, but is never a candidate
	m.merge([]NodeState{
		{NodeID: "b", Addr: "b:7777", Epoch: 1, Pools: map[string]int{"k": 2}},
		{NodeID: "c", Addr: "c:7777", Epoch: 1, Pools: map[string]int{"k": 0}}, // no warm
		{NodeID: "d", Addr: "d:7777", Epoch: 1, Pools: map[string]int{"other": 4}},
	})

	cands := m.Candidates("k")
	if len(cands) != 1 || cands[0] != "b:7777" {
		t.Errorf("candidates %v, want [b:7777]", cands)
	}
	if got := m.Candidates("missing"); got != nil {
		t.Errorf("candidates for absent key %v, want nil", got)
	}
}

func TestTemplateOwnersExcludeSelfAndUnknown(t *testing.T) {
	m := newTestMesh(t, "a")
	m.UpdateSelf(nil, []string{"tpl"}) // self holds it, but is never an owner candidate
	m.merge([]NodeState{
		{NodeID: "b", Addr: "b:7777", Epoch: 1, Templates: []string{"tpl", "other"}},
		{NodeID: "c", Addr: "c:7777", Epoch: 1, Templates: []string{"other"}},
	})

	if owners := m.TemplateOwners("tpl"); len(owners) != 1 || owners[0] != "b:7777" {
		t.Errorf("owners %v, want [b:7777]", owners)
	}
	if owners := m.TemplateOwners("absent"); owners != nil {
		t.Errorf("owners for absent hash %v, want nil", owners)
	}
}

func TestForgetPrunesDeadNode(t *testing.T) {
	m := newTestMesh(t, "a")
	m.merge([]NodeState{{NodeID: "b", Addr: "b:7777", Epoch: 1, Pools: map[string]int{"k": 3}}})
	if len(m.Candidates("k")) != 1 {
		t.Fatal("setup: b should be a candidate")
	}
	m.forget("b")
	if got := m.Candidates("k"); got != nil {
		t.Errorf("candidates after forget %v, want nil (b pruned)", got)
	}
	// forgetting self is a no-op.
	m.UpdateSelf(map[string]int{"k": 1}, nil)
	m.forget("a")
	if len(m.Members()) != 1 {
		t.Error("forget removed self")
	}
}

func TestCandidatesPowerOfTwo(t *testing.T) {
	m := newTestMesh(t, "a")
	m.merge([]NodeState{
		{NodeID: "b", Addr: "b:7777", Epoch: 1, Pools: map[string]int{"k": 1}},
		{NodeID: "c", Addr: "c:7777", Epoch: 1, Pools: map[string]int{"k": 1}},
		{NodeID: "d", Addr: "d:7777", Epoch: 1, Pools: map[string]int{"k": 1}},
	})
	// With ≥2 warm peers, exactly two distinct candidates come back.
	for range 20 {
		cands := m.Candidates("k")
		if len(cands) != 2 || cands[0] == cands[1] {
			t.Fatalf("candidates %v, want two distinct", cands)
		}
		for _, a := range cands {
			if !slices.Contains([]string{"b:7777", "c:7777", "d:7777"}, a) {
				t.Errorf("unexpected candidate %q", a)
			}
		}
	}
}

func TestTwoNodeClusterGossipsPools(t *testing.T) {
	a := startNode(t, "127.0.0.1", 0, "node-a")
	b := startNode(t, "127.0.0.1", 0, "node-b")
	if err := b.mesh.Join([]string{a.addr}); err != nil {
		t.Fatalf("join: %v", err)
	}
	a.mesh.UpdateSelf(map[string]int{"kk": 4}, []string{"tpl-hash"})

	// Push/pull sync propagates a's warm counts and template set to b within
	// a few intervals.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cands := b.mesh.Candidates("kk")
		owners := b.mesh.TemplateOwners("tpl-hash")
		if len(cands) == 1 && cands[0] == "node-a:7777" &&
			len(owners) == 1 && owners[0] == "node-a:7777" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node-b never learned node-a's state: view=%+v", b.mesh.Members())
}

func newTestMesh(t *testing.T, id string) *Mesh {
	t.Helper()
	return &Mesh{
		epochPath: filepath.Join(t.TempDir(), "mesh-epoch"),
		self:      NodeState{NodeID: id, Addr: id + ":7777", Pools: map[string]int{}},
		view:      map[string]NodeState{id: {NodeID: id, Addr: id + ":7777"}},
	}
}

func discardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

type node struct {
	mesh *Mesh
	addr string
}

func startNode(t *testing.T, host string, port int, id string) *node {
	t.Helper()
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.AdvertiseAddr = host
	cfg.PushPullInterval = 200 * time.Millisecond
	cfg.Logger = discardLogger()
	m, err := New(cfg, id, id+":7777", nil, t.TempDir())
	if err != nil {
		t.Fatalf("new mesh %s: %v", id, err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return &node{mesh: m, addr: fmt.Sprintf("%s:%d", host, m.ml.LocalNode().Port)}
}
