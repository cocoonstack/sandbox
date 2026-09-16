package mesh

import (
	"slices"
	"testing"
)

func TestClientAddressesDoNotReplaceInternalAddresses(t *testing.T) {
	m := newBoundMesh(t, t.TempDir())
	m.SetSelfClientAddr("https://self.example")
	if m.Members()[0].ClientAddr != "https://self.example" {
		t.Fatal("self client address was not published")
	}
	peer := NodeState{NodeID: "peer", Addr: "peer:7777", ClientAddr: "https://peer.example", Epoch: 1, Pools: map[string]int{"key": 1}}
	mergeStates(t, m, []NodeState{peer})
	if got := m.ClientAddr(peer.Addr); got != peer.ClientAddr {
		t.Fatalf("client address %q, want %q", got, peer.ClientAddr)
	}
	if got := m.Candidates("key"); !slices.Equal(got, []string{peer.Addr}) {
		t.Fatalf("placement addresses %v, want internal address", got)
	}
	if got := m.PeerAddrs(); !slices.Equal(got, []string{peer.Addr}) {
		t.Fatalf("peer addresses %v, want internal address", got)
	}
	if got := m.ClientAddr("unknown:7777"); got != "unknown:7777" {
		t.Fatalf("fallback address = %q", got)
	}
	peer.Epoch++
	peer.ClientAddr = ""
	mergeStates(t, m, []NodeState{peer})
	if got := m.ClientAddr(peer.Addr); got != peer.Addr {
		t.Fatalf("direct address = %q", got)
	}
}
