package server

import (
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

// SandboxListResponse is the wire reply of GET /v1/sandboxes.
type SandboxListResponse struct {
	Sandboxes []pool.SandboxSummary `json:"sandboxes"`
}

// handleMetrics renders Prometheus text format by hand, counters and gauges only.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	pools, g := s.mgr.Info()
	c := s.mgr.Counters()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	metric := func(name, kind, help string) {
		_, _ = fmt.Fprintf(w, "# HELP sandboxd_%s %s\n# TYPE sandboxd_%s %s\n", name, help, name, kind)
	}
	scalar := func(name, help string, value int) {
		metric(name, "gauge", help)
		_, _ = fmt.Fprintf(w, "sandboxd_%s %d\n", name, value)
	}
	draining := 0
	if g.Draining {
		draining = 1
	}
	scalar("claimed", "live claims on this node", g.Claimed)
	scalar("hibernated", "claims currently hibernated", g.Hibernated)
	scalar("archived", "claims archived to the checkpoint store", g.Archived)
	scalar("draining", "1 while the node is cordoned for maintenance", draining)

	if tenants := s.mgr.TenantClaims(); len(tenants) > 0 {
		metric("tenant_claims", "gauge", "live claims per configured tenant")
		for _, name := range slices.Sorted(maps.Keys(tenants)) {
			_, _ = fmt.Fprintf(w, "sandboxd_tenant_claims{tenant=%q} %d\n", name, tenants[name])
		}
	}

	metric("pool_warm", "gauge", "claim-ready VMs per pool")
	for _, p := range pools {
		_, _ = fmt.Fprintf(w, "sandboxd_pool_warm{template=%q,net=%q,size=%q} %d\n", p.Key.Template, p.Key.Net, p.Key.Size, p.Warm)
	}
	metric("pool_target", "gauge", "warm watermark per pool")
	for _, p := range pools {
		_, _ = fmt.Fprintf(w, "sandboxd_pool_target{template=%q,net=%q,size=%q} %d\n", p.Key.Template, p.Key.Net, p.Key.Size, p.Target)
	}

	if s.placer != nil {
		metric("config_digest_mismatch", "gauge", "peers whose cluster-invariant config digest differs from this node")
		_, _ = fmt.Fprintf(w, "sandboxd_config_digest_mismatch %d\n", s.placer.ConfigMismatches())
	}

	metric("claims_total", "counter", "claims served, by provisioning tier")
	_, _ = fmt.Fprintf(w, "sandboxd_claims_total{tier=\"warm\"} %d\n", c.ClaimsWarm)
	_, _ = fmt.Fprintf(w, "sandboxd_claims_total{tier=\"clone\"} %d\n", c.ClaimsClone)
	_, _ = fmt.Fprintf(w, "sandboxd_claims_total{tier=\"cold\"} %d\n", c.ClaimsCold)
	metric("claim_seconds_total", "counter", "time spent serving claims")
	_, _ = fmt.Fprintf(w, "sandboxd_claim_seconds_total %.6f\n", float64(c.ClaimNanos)/1e9)
	metric("wake_seconds_total", "counter", "time spent waking")
	_, _ = fmt.Fprintf(w, "sandboxd_wake_seconds_total %.6f\n", float64(c.WakeNanos)/1e9)
	for _, row := range []struct {
		name, help string
		value      uint64
	}{
		{"wakes_total", "transparent wakes", c.Wakes},
		{"hibernates_total", "hibernate transitions", c.Hibernates},
		{"forks_total", "fork operations", c.Forks},
		{"checkpoints_total", "checkpoints captured", c.Checkpoints},
		{"promotes_total", "templates promoted", c.Promotes},
		{"releases_total", "claims released", c.Releases},
		{"reaps_total", "claims reaped at deadline", c.Reaps},
		{"archives_total", "claims archived to the store", c.Archives},
		{"unarchives_total", "archived claims restored", c.Unarchives},
		{"archive_deletes_total", "archived checkpoints purged at retention", c.ArchiveDeletes},
	} {
		metric(row.name, "counter", row.help)
		_, _ = fmt.Fprintf(w, "sandboxd_%s %d\n", row.name, row.value)
	}
}

// handleSandboxes lists the live claims visible to the caller — never tokens.
func (s *Server) handleSandboxes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, SandboxListResponse{Sandboxes: s.mgr.Sandboxes(tenantFrom(r.Context()))})
}
