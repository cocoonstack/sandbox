package sandbox

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestSetPools(t *testing.T) {
	var got poolUpdate
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/pools" {
			t.Errorf("got %s %s, want PUT /v1/pools", r.Method, r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer root" {
			t.Errorf("auth = %q, want Bearer root", auth)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(NodeInfo{Claimed: 2})
	}))
	defer ts.Close()

	c := testClient(t, ts, WithAPIToken("root"))
	info, err := c.SetPools(t.Context(), []PoolSpec{{Template: "rt:24.04", Net: NetNone, Size: Small, Warm: 3}})
	if err != nil {
		t.Fatalf("SetPools: %v", err)
	}
	if info.Claimed != 2 {
		t.Errorf("info = %+v, want Claimed 2", info)
	}
	if len(got.Pools) != 1 || got.Pools[0].Warm != 3 || got.Pools[0].Template != "rt:24.04" {
		t.Errorf("server received %+v", got.Pools)
	}
}

func TestSetPoolsClusterFansOut(t *testing.T) {
	var mu sync.Mutex
	applied := map[string]int{}
	pools := func(w http.ResponseWriter, r *http.Request) {
		var body poolUpdate
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		applied[r.Host] = body.Pools[0].Warm
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(NodeInfo{})
	}
	peer := httptest.NewServer(http.HandlerFunc(pools))
	defer peer.Close()
	peerAddr := strings.TrimPrefix(peer.URL, "http://")

	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peers" {
			_ = json.NewEncoder(w).Encode(map[string][]string{"peers": {peerAddr}})
			return
		}
		pools(w, r)
	}))
	defer entry.Close()

	c := testClient(t, entry, WithAPIToken("root"))
	results, err := c.SetPoolsCluster(t.Context(), []PoolSpec{{Template: "rt:24.04", Net: NetNone, Size: Small, Warm: 5}})
	if err != nil {
		t.Fatalf("SetPoolsCluster: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2 (entry + one peer)", len(results))
	}
	for _, res := range results {
		if res.Err != nil {
			t.Errorf("node %s: %v", res.Addr, res.Err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != 2 {
		t.Fatalf("applied to %d nodes, want 2: %v", len(applied), applied)
	}
	for addr, warm := range applied {
		if warm != 5 {
			t.Errorf("node %s saw warm %d, want 5", addr, warm)
		}
	}
}

func TestSetPoolsClusterPeerDiscoveryFails(t *testing.T) {
	var applied int
	entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peers" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		applied++
		_ = json.NewEncoder(w).Encode(NodeInfo{})
	}))
	defer entry.Close()

	c := testClient(t, entry, WithAPIToken("root"))
	results, err := c.SetPoolsCluster(t.Context(), []PoolSpec{{Template: "rt:24.04", Net: NetNone, Size: Small, Warm: 5}})
	if err == nil {
		t.Fatal("SetPoolsCluster returned nil error despite peer-discovery failure")
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("results = %+v, want the entry node applied cleanly", results)
	}
	if applied != 1 {
		t.Errorf("applied to %d nodes, want 1 (entry only)", applied)
	}
}
