// Package mesh gossips per-node warm counts, promoted templates, and available
// volume names over a hashicorp/memberlist SWIM cluster. Gossip carries only
// placement hints — per-sandbox state stays node-local — so a stale view costs
// at most one failed redirect, never correctness. A single node with no seeds
// is a valid mesh of one.
package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/projecteru2/core/log"
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
	Volumes   []string       `json:"volumes,omitempty"`   // locally available dataset names
	Digest    string         `json:"digest,omitempty"`    // cluster-invariant config digest
}

// Mesh is the node's view of the cluster and its own gossiped state.
type Mesh struct {
	ml        *memberlist.Memberlist
	epochPath string
	// ctx is the daemon's, for logging inside memberlist callbacks — the
	// delegate interfaces carry no context of their own.
	ctx context.Context

	// updateMu serializes UpdateSelf end-to-end: two updates reading one epoch
	// would both bump to E+1 and the loser's payload would be dropped.
	updateMu sync.Mutex

	mu   sync.Mutex
	self NodeState
	view map[string]NodeState // node_id → latest known state (includes self)
	live map[string]struct{}  // members SWIM reports; gossip about anyone else is ignored
}

// New starts a mesh member listening per cfg. selfAddr is the data-plane
// address peers should dial for a redirect; secretKey (16/24/32 bytes) enables
// gossip encryption when non-empty; dataDir holds the persisted epoch.
func New(ctx context.Context, cfg *memberlist.Config, nodeID, selfAddr string, secretKey []byte, dataDir string) (*Mesh, error) {
	epochPath := filepath.Join(dataDir, "mesh-epoch")
	// Seed strictly above the persisted floor (the last epoch peers saw):
	// seeding at it ties their stale copy and merge's `>` rejects the restart's
	// fresh state. loadEpoch caps the floor so the +1 cannot wrap.
	epoch := max(uint64(time.Now().UnixNano()), loadEpoch(epochPath)+1) //nolint:gosec // UnixNano is positive for current times
	m := &Mesh{
		ctx:       ctx,
		epochPath: epochPath,
		self: NodeState{
			NodeID: nodeID,
			Addr:   selfAddr,
			Epoch:  epoch,
			Pools:  map[string]int{},
		},
		view: map[string]NodeState{},
		live: map[string]struct{}{},
	}
	if err := m.persistEpoch(epoch); err != nil {
		return nil, fmt.Errorf("persist mesh epoch: %w", err)
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

// UpdateSelf republishes this node's warm-pool counts, promoted-template set,
// and locally available volumes. An unchanged view does not bump the epoch.
// templates and volumes must arrive sorted: the compare is order-sensitive.
func (m *Mesh) UpdateSelf(ctx context.Context, pools map[string]int, templates, volumes []string) {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()
	m.mu.Lock()
	if maps.Equal(m.self.Pools, pools) && slices.Equal(m.self.Templates, templates) && slices.Equal(m.self.Volumes, volumes) {
		m.mu.Unlock()
		return
	}
	epoch := m.self.Epoch + 1
	m.mu.Unlock()
	// Persist the candidate before publishing it: memberlist gossips self the
	// instant it enters the view, so a crash before the write would strand peers
	// on an epoch a backwards-clock restart can't beat.
	if err := m.persistEpoch(epoch); err != nil {
		log.WithFunc("mesh.UpdateSelf").Warnf(ctx, "persist epoch: %v", err)
		return
	}
	m.mu.Lock()
	m.self.Epoch = epoch
	m.self.Pools = pools
	m.self.Templates = templates
	m.self.Volumes = volumes
	m.view[m.self.NodeID] = m.self
	m.mu.Unlock()
}

// SetSelfDigest records this node's cluster-invariant config digest so peers can
// detect divergence; call it once before Join, before any gossip ships.
func (m *Mesh) SetSelfDigest(digest string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.self.Digest = digest
	m.view[m.self.NodeID] = m.self
}

// ConfigMismatches counts peers whose config digest differs from this node's —
// the gauge for alerting on a divergent cluster, recomputed per read.
func (m *Mesh) ConfigMismatches() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.self.Digest == "" {
		return 0
	}
	n := 0
	for id, st := range m.view {
		if id != m.self.NodeID && st.Digest != "" && st.Digest != m.self.Digest {
			n++
		}
	}
	return n
}

// Candidates returns up to two peer addresses that report warm(keyHash) > 0,
// chosen power-of-two-choices to avoid herding every waiter onto one node.
// Self is never a candidate — the caller has already missed locally.
func (m *Mesh) Candidates(keyHash string) []string {
	return m.warmCandidates(keyHash, func(NodeState) bool { return true })
}

// VolumeCandidates returns warm peers that advertise every requested volume.
func (m *Mesh) VolumeCandidates(keyHash string, names []string) []string {
	return m.warmCandidates(keyHash, func(st NodeState) bool { return containsAll(st.Volumes, names) })
}

// TemplateOwners returns up to two peer addresses whose gossiped template
// set contains keyHash — the redirect targets for a name-based claim or
// delete of a template this node does not hold. Self is excluded: the caller
// has already checked its own disk.
func (m *Mesh) TemplateOwners(keyHash string) []string {
	return m.owners(func(st NodeState) bool { return slices.Contains(st.Templates, keyHash) })
}

// VolumeOwners returns peers that currently advertise every requested volume.
// Self is excluded because the caller checks local availability first.
func (m *Mesh) VolumeOwners(names []string) []string {
	return m.owners(func(st NodeState) bool { return containsAll(st.Volumes, names) })
}

// TemplateVolumeOwners returns peers that hold both the promoted template and
// every requested volume, avoiding an incorrect intersection after truncation.
func (m *Mesh) TemplateVolumeOwners(keyHash string, names []string) []string {
	return m.owners(func(st NodeState) bool {
		return slices.Contains(st.Templates, keyHash) && containsAll(st.Volumes, names)
	})
}

// VolumeHolders counts every member advertising each volume, including self.
func (m *Mesh) VolumeHolders() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	holders := map[string]int{}
	for _, st := range m.view {
		for _, name := range st.Volumes {
			holders[name]++
		}
	}
	return holders
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

func (m *Mesh) warmCandidates(keyHash string, match func(NodeState) bool) []string {
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
		if st.Pools[keyHash] > 0 && match(st) {
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

func (m *Mesh) persistEpoch(epoch uint64) error {
	return storeEpoch(m.epochPath, epoch)
}

func (m *Mesh) owners(match func(NodeState) bool) []string {
	m.mu.Lock()
	var owners []string
	for id, st := range m.view {
		if id != m.self.NodeID && match(st) {
			owners = append(owners, st.Addr)
		}
	}
	m.mu.Unlock()
	return owners[:min(len(owners), 2)]
}

func (m *Mesh) admit(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.live[nodeID] = struct{}{}
}

// forget drops a departed node from the placement view so redirects stop
// targeting a dead peer; SWIM detected the death, the view must follow.
func (m *Mesh) forget(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID != m.self.NodeID {
		delete(m.live, nodeID)
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
		if _, member := m.live[st.NodeID]; !member {
			continue
		}
		cur, ok := m.view[st.NodeID]
		if ok && st.Epoch <= cur.Epoch {
			continue
		}
		// Warn on each distinct divergent digest (not once per lifetime): a
		// mismatched api_token/tenants/preview_secret/CA root 401s cross-node
		// redirects and fails interception. Warn-only — refusing would partition
		// a rolling credential rotation.
		if m.self.Digest != "" && st.Digest != "" && st.Digest != m.self.Digest && (!ok || cur.Digest != st.Digest) {
			log.WithFunc("mesh.merge").Warnf(m.ctx,
				"peer %s config digest %s differs from this node's %s: cluster-invariant config diverges (redirects may 401, interception may fail)",
				st.NodeID, short(st.Digest), short(m.self.Digest))
		}
		m.view[st.NodeID] = st
	}
}

func short(digest string) string {
	return digest[:min(len(digest), 12)]
}

func containsAll(have, need []string) bool {
	return len(need) > 0 && !slices.ContainsFunc(need, func(name string) bool {
		return !slices.Contains(have, name)
	})
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

// eventDelegate tracks SWIM membership: admit on join, prune the view on leave.
type eventDelegate Mesh

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) { (*Mesh)(e).admit(n.Name) }
func (e *eventDelegate) NotifyUpdate(*memberlist.Node) {}
func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	(*Mesh)(e).forget(n.Name)
}
