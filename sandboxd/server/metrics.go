package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

// SandboxListResponse is the wire reply of GET /v1/sandboxes.
type SandboxListResponse struct {
	Sandboxes []pool.SandboxSummary `json:"sandboxes"`
}

// handleMetrics renders Prometheus text format by hand — counters and
// gauges only, no client library. Latency rides as *_seconds_total next to
// its count, so dashboards derive averages without histogram machinery.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	pools, claimed, hibernated := s.mgr.Info()
	c := s.mgr.Counters()

	var b strings.Builder
	metric := func(name, kind, help string) {
		fmt.Fprintf(&b, "# HELP sandboxd_%s %s\n# TYPE sandboxd_%s %s\n", name, help, name, kind)
	}
	metric("claimed", "gauge", "live claims on this node")
	fmt.Fprintf(&b, "sandboxd_claimed %d\n", claimed)
	metric("hibernated", "gauge", "claims currently hibernated")
	fmt.Fprintf(&b, "sandboxd_hibernated %d\n", hibernated)

	metric("pool_warm", "gauge", "claim-ready VMs per pool")
	for _, p := range pools {
		fmt.Fprintf(&b, "sandboxd_pool_warm{template=%q,net=%q,size=%q} %d\n", p.Key.Template, p.Key.Net, p.Key.Size, p.Warm)
	}
	metric("pool_target", "gauge", "warm watermark per pool")
	for _, p := range pools {
		fmt.Fprintf(&b, "sandboxd_pool_target{template=%q,net=%q,size=%q} %d\n", p.Key.Template, p.Key.Net, p.Key.Size, p.Target)
	}

	metric("claims_total", "counter", "claims served, by provisioning tier")
	fmt.Fprintf(&b, "sandboxd_claims_total{tier=\"warm\"} %d\n", c.ClaimsWarm)
	fmt.Fprintf(&b, "sandboxd_claims_total{tier=\"clone\"} %d\n", c.ClaimsClone)
	fmt.Fprintf(&b, "sandboxd_claims_total{tier=\"cold\"} %d\n", c.ClaimsCold)
	metric("claim_seconds_total", "counter", "time spent serving claims")
	fmt.Fprintf(&b, "sandboxd_claim_seconds_total %.6f\n", float64(c.ClaimNanos)/1e9)
	metric("wakes_total", "counter", "transparent wakes")
	fmt.Fprintf(&b, "sandboxd_wakes_total %d\n", c.Wakes)
	metric("wake_seconds_total", "counter", "time spent waking")
	fmt.Fprintf(&b, "sandboxd_wake_seconds_total %.6f\n", float64(c.WakeNanos)/1e9)
	for _, row := range []struct {
		name, help string
		value      uint64
	}{
		{"hibernates_total", "hibernate transitions", c.Hibernates},
		{"forks_total", "fork operations", c.Forks},
		{"checkpoints_total", "checkpoints captured", c.Checkpoints},
		{"promotes_total", "templates promoted", c.Promotes},
		{"releases_total", "claims released", c.Releases},
		{"reaps_total", "claims reaped at deadline", c.Reaps},
	} {
		metric(row.name, "counter", row.help)
		fmt.Fprintf(&b, "sandboxd_%s %d\n", row.name, row.value)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}

// handleSandboxes lists live claims for operator tooling — never tokens.
func (s *Server) handleSandboxes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, SandboxListResponse{Sandboxes: s.mgr.Sandboxes()})
}
