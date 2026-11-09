package server

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
)

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
	flag := func(name, help string, on bool) {
		value := 0
		if on {
			value = 1
		}
		scalar(name, help, value)
	}
	scalar("claimed", "live claims on this node", g.Claimed)
	scalar("running", "claims holding a VM, including wakes in flight", g.Running)
	scalar("max_running", "cap on running claims, 0 when off", g.MaxRunning)
	scalar("hibernated", "claims currently hibernated", g.Hibernated)
	scalar("archived", "claims archived to the checkpoint store", g.Archived)
	scalar("failed", "claims whose VMM is down and not restarted", g.Failed)
	flag("draining", "1 while the node is draining", g.Draining)
	flag("cordoned", "1 while the node is cordoned", g.Cordoned)
	scalar("evacuating", "claims an evacuation has yet to move to a peer", g.Evacuating)
	flag("memory_low", "1 while memory_limit refuses claims and refills", g.MemoryLow)
	scalar("anon_bytes", "node anon memory, 0 when memory_limit has no anon limit", g.AnonBytes)

	if tenants := s.mgr.TenantClaims(); len(tenants) > 0 {
		metric("tenant_claims", "gauge", "live claims per tenant holding any")
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
	metric("claim_duration_seconds", "histogram", "time to serve a pool claim, by provisioning tier")
	writeClaimLatency(w, "warm", c.ClaimWarmLatency)
	writeClaimLatency(w, "clone", c.ClaimCloneLatency)
	writeClaimLatency(w, "cold", c.ClaimColdLatency)
	metric("claim_redirects_total", "counter", "claims handed to a peer instead of served here")
	_, _ = fmt.Fprintf(w, "sandboxd_claim_redirects_total %d\n", s.claimRedirects.Load())
	metric("wake_seconds_total", "counter", "time spent waking")
	_, _ = fmt.Fprintf(w, "sandboxd_wake_seconds_total %.6f\n", float64(c.WakeNanos)/1e9)
	metric("wakes_refused_total", "counter", "wakes refused for capacity, by the limit that refused them")
	_, _ = fmt.Fprintf(w, "sandboxd_wakes_refused_total{reason=\"max_running\"} %d\n", c.WakesRefusedRunning)
	_, _ = fmt.Fprintf(w, "sandboxd_wakes_refused_total{reason=\"disk\"} %d\n", c.WakesRefusedDisk)
	_, _ = fmt.Fprintf(w, "sandboxd_wakes_refused_total{reason=\"memory\"} %d\n", c.WakesRefusedMemory)
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
		{"archive_deletes_total", "archived checkpoints deleted at release or retention", c.ArchiveDeletes},
		{"wake_handoffs_total", "archived claims handed to a peer with room instead of a refused wake", c.WakeHandoffs},
		{"wake_memory_fallbacks_total", "archived wakes over memory_limit woken here because no hand-off ran or no peer adopted", c.WakeMemoryFallbacks},
		{"dns_malformed_total", "guest DNS queries dropped as malformed", c.DNSMalformed},
	} {
		metric(row.name, "counter", row.help)
		_, _ = fmt.Fprintf(w, "sandboxd_%s %d\n", row.name, row.value)
	}
	if st, ok := s.mgr.TunStats(); ok {
		writeTunStats(w, metric, st)
	}
}

func writeTunStats(w io.Writer, metric func(name, kind, help string), st tunnet.Stats) {
	metric("tun_frames_total", "counter", "packet frames on the claims' vsock streams, by direction from the guest")
	_, _ = fmt.Fprintf(w, "sandboxd_tun_frames_total{dir=\"out\"} %d\n", st.FramesIn)
	_, _ = fmt.Fprintf(w, "sandboxd_tun_frames_total{dir=\"in\"} %d\n", st.FramesOut)
	metric("tun_datagrams_total", "counter", "VXLAN datagrams on the underlay, by direction from the guest")
	_, _ = fmt.Fprintf(w, "sandboxd_tun_datagrams_total{dir=\"out\"} %d\n", st.DgramsOut)
	_, _ = fmt.Fprintf(w, "sandboxd_tun_datagrams_total{dir=\"in\"} %d\n", st.DgramsIn)
	metric("tun_arp_total", "counter", "ARP replies to exits and gratuitous ARPs sent")
	_, _ = fmt.Fprintf(w, "sandboxd_tun_arp_total{kind=\"answered\"} %d\n", st.ARPAnswered)
	_, _ = fmt.Fprintf(w, "sandboxd_tun_arp_total{kind=\"announced\"} %d\n", st.ARPSent)
	metric("tun_drops_total", "counter", "packets the tun path did not carry, by reason")
	for i, n := range st.Drops {
		if reason := tunnet.Drop(i).String(); reason != "" {
			_, _ = fmt.Fprintf(w, "sandboxd_tun_drops_total{reason=%q} %d\n", reason, n)
		}
	}
}

func writeClaimLatency(w io.Writer, tier string, latency pool.ClaimLatency) {
	var total uint64
	for i, bound := range pool.ClaimBuckets {
		total += latency.Counts[i]
		_, _ = fmt.Fprintf(w, "sandboxd_claim_duration_seconds_bucket{tier=%q,le=\"%g\"} %d\n", tier, bound.Seconds(), total)
	}
	total += latency.Counts[len(pool.ClaimBuckets)]
	_, _ = fmt.Fprintf(w, "sandboxd_claim_duration_seconds_bucket{tier=%q,le=\"+Inf\"} %d\n", tier, total)
	_, _ = fmt.Fprintf(w, "sandboxd_claim_duration_seconds_sum{tier=%q} %.6f\n", tier, float64(latency.Nanos)/1e9)
	_, _ = fmt.Fprintf(w, "sandboxd_claim_duration_seconds_count{tier=%q} %d\n", tier, total)
}
