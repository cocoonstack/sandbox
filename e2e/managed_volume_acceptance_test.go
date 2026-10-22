package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/server"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func warmTotal(st *stack) int {
	total := 0
	for _, n := range st.mgr.WarmCounts() {
		total += n
	}
	return total
}

// TestManagedVolumeIssue300Acceptance walks the acceptance line of issue #300 over HTTP.
func TestManagedVolumeIssue300Acceptance(t *testing.T) {
	other := types.PoolKey{Template: "rt:22.04", Net: types.NetNone, Size: types.SizeSmall}
	cfg := &config.Config{
		ManagedVolumeDir: filepath.Join(t.TempDir(), "managed"),
		Pools:            []config.PoolSpec{{PoolKey: testKey, Warm: 1}, {PoolKey: other, Warm: 1}},
	}
	st := startConfigStack(t, "root-token", cfg)
	if err := st.mgr.SetTenants(t.Context(), []tenants.TenantSpec{{Name: "t1", Token: "t1-token"}, {Name: "t2", Token: "t2-token"}}); err != nil {
		t.Fatal(err)
	}
	if c := requestManagedVolume(t, st, http.MethodPut, "/v1/volumes/work", "t1-token", `{"size_gib":20}`, ""); c.status != http.StatusCreated {
		t.Fatalf("create 20 GiB for t1: %d %s", c.status, c.body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for warmTotal(st) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if warmTotal(st) < 2 {
		t.Fatalf("warm pools never filled: %v", st.mgr.WarmCounts())
	}
	claimBody := func(template string) string {
		return fmt.Sprintf(`{"template":%q,"net":"none","size":"small","volumes":[{"name":"work","mode":"rw","attach_only":true}]}`, template)
	}
	warmBefore := st.mgr.Counters().ClaimsWarm
	first := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t1-token", claimBody("rt:24.04"), "")
	var sb server.ClaimResponse
	if err := json.Unmarshal(first.body, &sb); first.status != http.StatusOK || err != nil || sb.ID == "" {
		t.Fatalf("warm rw claim: %d %s", first.status, first.body)
	}
	if st.mgr.Counters().ClaimsWarm != warmBefore+1 {
		t.Fatalf("rw claim with a managed volume was not served from the warm pool: counters=%+v", st.mgr.Counters())
	}
	second := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t1-token", claimBody("rt:22.04"), "")
	if second.status != http.StatusConflict || second.retryAfter != "" || !strings.Contains(string(second.body), `"sandbox_id":"`+sb.ID+`"`) {
		t.Fatalf("second rw claim: %d Retry-After=%q %s, want 409 naming %s without Retry-After", second.status, second.retryAfter, second.body, sb.ID)
	}
	if r := requestManagedVolume(t, st, http.MethodPost, "/v1/sandboxes/"+sb.ID+"/release", sb.Token, "", ""); r.status != http.StatusOK && r.status != http.StatusNoContent {
		t.Fatalf("release: %d %s", r.status, r.body)
	}
	if g := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/work", "t1-token", "", ""); g.status != http.StatusOK {
		t.Fatalf("volume after release: %d %s, want it kept", g.status, g.body)
	}
	again := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t1-token", claimBody("rt:22.04"), "")
	if again.status != http.StatusOK {
		t.Fatalf("claim of another template with the same volume: %d %s", again.status, again.body)
	}
	foreignGet := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/work", "t2-token", "", "")
	unknownGet := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes/nope", "t2-token", "", "")
	if foreignGet.status != http.StatusNotFound || string(foreignGet.body) != string(unknownGet.body) {
		t.Fatalf("t2 GET: %d %s; unknown: %d %s; want identical 404", foreignGet.status, foreignGet.body, unknownGet.status, unknownGet.body)
	}
	if list := requestManagedVolume(t, st, http.MethodGet, "/v1/volumes", "t2-token", "", ""); strings.Contains(string(list.body), `"work"`) {
		t.Fatalf("t2 list shows t1's volume: %s", list.body)
	}
	foreignClaim := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", claimBody("rt:24.04"), "")
	unknownClaim := requestManagedVolume(t, st, http.MethodPost, "/v1/claim", "t2-token", strings.Replace(claimBody("rt:24.04"), `"work"`, `"nope"`, 1), "")
	if foreignClaim.status != http.StatusBadRequest || string(foreignClaim.body) != string(unknownClaim.body) {
		t.Fatalf("t2 claim: %d %s; unknown: %d %s; want identical 400", foreignClaim.status, foreignClaim.body, unknownClaim.status, unknownClaim.body)
	}
}
