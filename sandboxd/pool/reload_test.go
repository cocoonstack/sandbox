package pool

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

func TestALiveClaimsEgressFollowsTheView(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(origin.Close)
	host := mustHostname(t, origin.URL)
	m := egressManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "other.test"}}}})
	m.dial = (&net.Dialer{}).DialContext
	sb := vsockSandbox(t, "sb_live")
	if err := m.armEgress(t.Context(), sb); err != nil {
		t.Fatalf("arm egress: %v", err)
	}
	client := egressClient(engine.EgressSocketPath(sb.VsockSocket))
	get := func() int {
		t.Helper()
		resp, err := client.Get(origin.URL + "/")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(); code != http.StatusForbidden {
		t.Fatalf("before the change: %d, want 403", code)
	}
	editView(m, func(v *configView) { v.poolEgress[testKey] = &egress.Policy{Allow: []egress.Rule{{Host: host}}} })
	if code := get(); code != http.StatusOK {
		t.Errorf("after the allow-list admits the host: %d, want 200 on the same armed claim", code)
	}
	editView(m, func(v *configView) { delete(v.poolEgress, testKey) })
	if code := get(); code != http.StatusForbidden {
		t.Errorf("after the pool loses its policy: %d, want 403", code)
	}
}
