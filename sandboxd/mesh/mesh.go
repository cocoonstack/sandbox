// Package mesh gossips per-node warm-pool counts over a hashicorp/memberlist
// SWIM cluster, so any node can redirect a claim to a peer that already holds
// a warm sandbox for the requested pool key. Gossip carries only placement
// hints — per-sandbox state stays node-local — so a stale view costs at most
// one extra redirect, never correctness. A single node with no seeds is a
// valid mesh of one.
package mesh

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// leaveTimeout bounds the graceful-leave broadcast on shutdown so a wedged
// network can't hang the exit path.
const leaveTimeout = time.Second

// NodeState is one node's gossiped placement view. Epoch resolves merges: the
// higher epoch for a given node wins.
type NodeState struct {
	NodeID    string         `json:"node_id"`
	Addr      string         `json:"addr"` // data-plane advertise address
	Epoch     uint64         `json:"epoch"`
	Pools     map[string]int `json:"pools"`               // PoolKey hash → warm count
	Templates []string       `json:"templates,omitempty"` // promoted-template key hashes on disk
}

// Mesh is the node's view of the cluster and its own gossiped state.
type Mesh struct {
	ml *memberlist.Memberlist

	mu   sync.Mutex
	self NodeState
	view map[string]NodeState // node_id → latest known state (includes self)
}

// New starts a mesh member listening per cfg. selfAddr is the data-plane
// address peers should dial for a redirect; secretKey (16/24/32 bytes) enables
// gossip encryption when non-empty.
func New(cfg *memberlist.Config, nodeID, selfAddr string, secretKey []byte) (*Mesh, error) {
	m := &Mesh{
		self: NodeState{
			NodeID: nodeID,
			Addr:   selfAddr,
			// Seed the epoch from wall-clock so a restarted node's fresh counts
			// aren't rejected by peers still holding its pre-restart (higher)
			// epoch; Epoch++ keeps intra-process monotonicity above that base.
			Epoch: uint64(time.Now().UnixNano()), //nolint:gosec // UnixNano is positive for current times
			Pools: map[string]int{},
		},
		view: map[string]NodeState{},
	}
	m.view[nodeID] = m.self

	cfg.Name = nodeID
	cfg.Delegate = (*delegate)(m)
	cfg.Events = (*eventDelegate)(m)
	if len(secretKey) > 0 {
		cfg.SecretKey = secretKey
	}
	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}
	m.ml = ml
	return m, nil
}

// Join contacts seed members; an empty list leaves a mesh of one.
func (m *Mesh) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil
	}
	if _, err := m.ml.Join(seeds); err != nil {
		return fmt.Errorf("join mesh: %w", err)
	}
	return nil
}

// UpdateSelf republishes this node's warm-pool counts and promoted-template
// set, bumping the epoch so peers adopt the new view.
func (m *Mesh) UpdateSelf(pools map[string]int, templates []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.self.Epoch++
	m.self.Pools = pools
	m.self.Templates = templates
	m.view[m.self.NodeID] = m.self
}

// Candidates returns up to two peer addresses that report warm(keyHash) > 0,
// chosen power-of-two-choices to avoid herding every waiter onto one node.
// Self is never a candidate — the caller has already missed locally.
func (m *Mesh) Candidates(keyHash string) []string {
	m.mu.Lock()
	type cand struct {
		addr string
		warm int
	}
	var pool []cand
	for id, st := range m.view {
		if id == m.self.NodeID {
			continue
		}
		if st.Pools[keyHash] > 0 {
			pool = append(pool, cand{st.Addr, st.Pools[keyHash]})
		}
	}
	m.mu.Unlock()

	switch len(pool) {
	case 0:
		return nil
	case 1:
		return []string{pool[0].addr}
	}
	// Power-of-two-choices: sample two, order by warmer. This is load
	// spreading, not security — a weak PRNG is the right tool.
	i := rand.IntN(len(pool))     //nolint:gosec // placement jitter, not crypto
	j := rand.IntN(len(pool) - 1) //nolint:gosec // placement jitter, not crypto
	if j >= i {
		j++
	}
	a, b := pool[i], pool[j]
	if b.warm > a.warm {
		a, b = b, a
	}
	return []string{a.addr, b.addr}
}

// TemplateOwners returns up to two peer addresses whose gossiped template
// set contains keyHash — the redirect targets for a name-based claim or
// delete of a template this node does not hold. Self is excluded: the caller
// has already checked its own disk.
func (m *Mesh) TemplateOwners(keyHash string) []string {
	m.mu.Lock()
	var owners []string
	for id, st := range m.view {
		if id != m.self.NodeID && slices.Contains(st.Templates, keyHash) {
			owners = append(owners, st.Addr)
		}
	}
	m.mu.Unlock()
	// Which two survive truncation is already jittered by map iteration
	// order; unlike Candidates there is no warmth to rank by.
	if len(owners) > 2 {
		owners = owners[:2]
	}
	return owners
}

// Members returns the current cluster view (self included).
func (m *Mesh) Members() []NodeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Collect(maps.Values(m.view))
}

// PeerAddrs returns the data-plane addresses of the other nodes, for a
// client-side Lookup scatter.
func (m *Mesh) PeerAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	addrs := make([]string, 0, len(m.view))
	for id, st := range m.view {
		if id != m.self.NodeID {
			addrs = append(addrs, st.Addr)
		}
	}
	return addrs
}

// Shutdown leaves the mesh and stops the member.
func (m *Mesh) Shutdown() error {
	_ = m.ml.Leave(leaveTimeout)
	return m.ml.Shutdown()
}

// forget drops a departed node from the placement view so redirects stop
// targeting a dead peer; SWIM detected the death, the view must follow.
func (m *Mesh) forget(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID != m.self.NodeID {
		delete(m.view, nodeID)
	}
}

// merge absorbs a peer's view, keeping the higher epoch per node and never
// letting a peer overwrite this node's own authoritative self entry.
func (m *Mesh) merge(states []NodeState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, st := range states {
		if st.NodeID == m.self.NodeID {
			continue
		}
		if cur, ok := m.view[st.NodeID]; !ok || st.Epoch > cur.Epoch {
			m.view[st.NodeID] = st
		}
	}
}

var _ memberlist.Delegate = (*delegate)(nil)

// delegate carries this node's full view on each memberlist push/pull sync, so
// state propagates transitively across the cluster.
type delegate Mesh

func (d *delegate) NodeMeta(int) []byte             { return nil }
func (d *delegate) NotifyMsg([]byte)                {}
func (d *delegate) GetBroadcasts(int, int) [][]byte { return nil }
func (d *delegate) LocalState(bool) []byte {
	buf, _ := json.Marshal((*Mesh)(d).Members())
	return buf
}

func (d *delegate) MergeRemoteState(buf []byte, _ bool) {
	var states []NodeState
	if err := json.Unmarshal(buf, &states); err != nil {
		return
	}
	(*Mesh)(d).merge(states)
}

var _ memberlist.EventDelegate = (*eventDelegate)(nil)

// eventDelegate prunes the placement view when SWIM reports a node gone, so a
// dead peer stops attracting redirects.
type eventDelegate Mesh

func (e *eventDelegate) NotifyJoin(*memberlist.Node)   {}
func (e *eventDelegate) NotifyUpdate(*memberlist.Node) {}
func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	(*Mesh)(e).forget(n.Name)
}
