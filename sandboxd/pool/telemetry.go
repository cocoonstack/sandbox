package pool

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	// AuditLineCap bounds how much of a relayed request frame the audit tap captures and records.
	AuditLineCap = 4096

	egressEvent  = "egress"
	dnsEvent     = "dns"
	reapEvent    = "reap"
	tunFlowEvent = "tun_flow"
	tunFlowPath  = "tun"
)

// ClaimBuckets are the upper bounds of the claim latency histogram.
var ClaimBuckets = [...]time.Duration{
	500 * time.Microsecond, time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2500 * time.Millisecond,
}

// ClaimLatency is one tier's histogram: one count per ClaimBuckets bound, then the overflow.
type ClaimLatency struct {
	Counts [len(ClaimBuckets) + 1]uint64
	Nanos  uint64
}

// Counters is a monotonic snapshot for /metrics; gauges come from Info.
type Counters struct {
	ClaimsWarm  uint64
	ClaimsClone uint64
	ClaimsCold  uint64
	Wakes       uint64
	Hibernates  uint64
	Forks       uint64
	Checkpoints uint64
	Promotes    uint64
	Releases    uint64
	Reaps       uint64

	Archives       uint64
	Unarchives     uint64
	ArchiveDeletes uint64

	WakesRefusedRunning uint64
	WakesRefusedDisk    uint64
	WakeHandoffs        uint64

	ClaimWarmLatency, ClaimCloneLatency, ClaimColdLatency ClaimLatency

	ClaimNanos uint64
	WakeNanos  uint64

	DNSMalformed uint64
}

type counters struct {
	claimsWarm, claimsClone, claimsCold   atomic.Uint64
	wakes, hibernates                     atomic.Uint64
	forks, checkpoints, promotes          atomic.Uint64
	releases, reaps                       atomic.Uint64
	archives, unarchives, archiveDeletes  atomic.Uint64
	wakesRefusedRunning, wakesRefusedDisk atomic.Uint64
	wakeHandoffs                          atomic.Uint64
	wakeNanos                             atomic.Uint64

	claimWarmLatency, claimCloneLatency, claimColdLatency claimHistogram
}

type claimHistogram struct {
	counts [len(ClaimBuckets) + 1]atomic.Uint64
	nanos  atomic.Uint64
}

func (h *claimHistogram) observe(d time.Duration) {
	i, _ := slices.BinarySearch(ClaimBuckets[:], d)
	h.counts[i].Add(1)
	h.nanos.Add(uint64(d)) //nolint:gosec // durations are positive
}

func (h *claimHistogram) snapshot() ClaimLatency {
	out := ClaimLatency{Nanos: h.nanos.Load()}
	for i := range h.counts {
		out.Counts[i] = h.counts[i].Load()
	}
	return out
}

// requestFrame is the addressing slice of a relayed request frame worth auditing.
type requestFrame struct {
	Op      string   `json:"op"`
	Argv    []string `json:"argv,omitempty"`
	Path    string   `json:"path,omitempty"`
	Dest    string   `json:"dest,omitempty"`
	From    string   `json:"from,omitempty"`
	To      string   `json:"to,omitempty"`
	URL     string   `json:"url,omitempty"`
	Session string   `json:"session,omitempty"`
	Port    uint16   `json:"port,omitzero"`
}

// auditFrame keeps Caller out of requestFrame, so a guest-bound frame cannot forge it.
type auditFrame struct {
	requestFrame
	Caller  string   `json:"caller,omitempty"`
	Added   []string `json:"added,omitempty"`
	Changed []string `json:"changed,omitempty"`
	Removed []string `json:"removed,omitempty"`
}

type egressFrame struct {
	Op       string         `json:"op"`
	Dest     string         `json:"dest,omitempty"`
	Method   string         `json:"method,omitempty"`
	Port     uint16         `json:"port,omitzero"`
	Decision string         `json:"decision,omitempty"`
	Secret   string         `json:"secret,omitempty"`
	Upstream string         `json:"upstream,omitempty"`
	Tenant   string         `json:"tenant,omitempty"`
	Src      netip.AddrPort `json:"src_addr,omitzero"`
	Dst      netip.AddrPort `json:"dst_addr,omitzero"`
}

type dnsFrame struct {
	Op       string `json:"op"`
	Name     string `json:"name"`
	Type     string `json:"qtype"`
	Answers  int    `json:"answers"`
	Decision string `json:"decision"`
	Tenant   string `json:"tenant,omitempty"`
}

type tunFlowFrame struct {
	Op       string         `json:"op"`
	Path     string         `json:"path"`
	Tenant   string         `json:"tenant,omitempty"`
	Proto    string         `json:"proto"`
	Dst      netip.AddrPort `json:"dst_addr"`
	Decision string         `json:"decision"`
	Sent     int64          `json:"sent"`
	Received int64          `json:"received"`
	Start    time.Time      `json:"start"`
	End      time.Time      `json:"end"`
	Profile  string         `json:"profile"`
}

type stamped[F any] struct {
	Time  time.Time `json:"t"`
	ID    string    `json:"id"`
	Frame F         `json:",embed"`
}

func (m *Manager) Counters() Counters {
	c := &m.counters
	warm, clone, cold := c.claimWarmLatency.snapshot(), c.claimCloneLatency.snapshot(), c.claimColdLatency.snapshot()
	return Counters{
		ClaimsWarm:     c.claimsWarm.Load(),
		ClaimsClone:    c.claimsClone.Load(),
		ClaimsCold:     c.claimsCold.Load(),
		Wakes:          c.wakes.Load(),
		Hibernates:     c.hibernates.Load(),
		Forks:          c.forks.Load(),
		Checkpoints:    c.checkpoints.Load(),
		Promotes:       c.promotes.Load(),
		Releases:       c.releases.Load(),
		Reaps:          c.reaps.Load(),
		Archives:       c.archives.Load(),
		Unarchives:     c.unarchives.Load(),
		ArchiveDeletes: c.archiveDeletes.Load(),

		WakesRefusedRunning: c.wakesRefusedRunning.Load(),
		WakesRefusedDisk:    c.wakesRefusedDisk.Load(),
		WakeHandoffs:        c.wakeHandoffs.Load(),

		ClaimNanos:   warm.Nanos + clone.Nanos + cold.Nanos,
		WakeNanos:    c.wakeNanos.Load(),
		DNSMalformed: m.out.DNSMalformed(),

		ClaimWarmLatency:  warm,
		ClaimCloneLatency: clone,
		ClaimColdLatency:  cold,
	}
}

// TenantClaims counts live claims per tenant that holds any, so label cardinality stays bounded by the node's claims.
func (m *Manager) TenantClaims() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.tenantLive)
}

// Audit records one relayed request frame against a sandbox: the op and addressing fields only.
func (m *Manager) Audit(ctx context.Context, id string, line []byte) {
	if len(line) > AuditLineCap {
		// The addressing fields stay unread at this size, but a padded frame must not escape the journal
		m.recordAudit(ctx, id, auditFrame{Op: "oversized"})
		return
	}
	var frame requestFrame
	if json.Unmarshal(line, &frame) != nil {
		frame = memberwiseFrame(line)
	}
	if frame.Op == "" {
		return // torn cap boundary or a non-frame line: nothing to record
	}
	m.recordAudit(ctx, id, auditFrame{requestFrame: frame})
}

// AuditEnabled reports whether the relay should tap request frames at all.
func (m *Manager) AuditEnabled() bool {
	return m.audit != nil
}

// AuditTokenRead records that the caller on ctx read claim id's token.
func (m *Manager) AuditTokenRead(ctx context.Context, id string) {
	m.recordAudit(ctx, id, auditFrame{Op: "token"})
}

func (m *Manager) recordAudit(ctx context.Context, id string, frame auditFrame) {
	if m.audit == nil {
		return
	}
	frame.Caller = CallerOf(ctx)
	m.appendAudit(ctx, stamped[auditFrame]{time.Now(), id, frame})
}

func (m *Manager) appendAudit(ctx context.Context, event any) {
	if err := m.audit.append(event); err != nil {
		log.WithFunc("pool.appendAudit").Error(ctx, err, "append audit event")
	}
}

func (m *Manager) recordEgress(ctx context.Context, id, tenant string, ev egress.Event) {
	decision := "deny"
	switch ev.Decision {
	case egress.DecisionAllow:
		decision = "allow"
	case egress.DecisionRateLimited:
		decision = "rate_limited"
	}
	if m.audit != nil {
		m.appendAudit(ctx, stamped[egressFrame]{time.Now(), id, egressFrame{
			Op: egressEvent, Dest: ev.Host, Method: ev.Method, Port: ev.Port, Decision: decision, Secret: ev.Injected, Upstream: ev.Upstream,
			Tenant: tenant, Src: ev.Local, Dst: ev.Remote,
		}})
	}
	m.recordUsage(ctx, usage.Event{Event: egressEvent, ID: id, Tenant: tenant, Reference: ev.Host, Upstream: ev.Upstream, Secret: ev.Injected})
}

func (m *Manager) recordDNS(ctx context.Context, id, tenant string, ev outbound.DNSEvent) {
	if m.audit == nil {
		return
	}
	m.appendAudit(ctx, stamped[dnsFrame]{time.Now(), id, dnsFrame{Op: dnsEvent, Name: ev.Name, Type: ev.Type, Answers: ev.Answers, Decision: ev.Decision, Tenant: tenant}})
}

func (m *Manager) recordTunFlow(ctx context.Context, id, tenant, profile string, r tunnet.FlowRecord) {
	if m.audit == nil {
		return
	}
	decision := "deny"
	if r.Allowed {
		decision = "allow"
	}
	m.appendAudit(ctx, stamped[tunFlowFrame]{r.End, id, tunFlowFrame{
		Op: tunFlowEvent, Path: tunFlowPath, Tenant: tenant, Proto: protoName(r.Proto), Dst: netip.AddrPortFrom(r.Dst, r.Port),
		Decision: decision, Sent: r.Up, Received: r.Down, Start: r.Start, End: r.End, Profile: cmp.Or(profile, "default"),
	}})
}

func (m *Manager) recordUsage(ctx context.Context, ev usage.Event) {
	ev.Time = cmp.Or(ev.Time, time.Now())
	if err := m.usage.append(ev); err != nil {
		log.WithFunc("pool.recordUsage").Errorf(ctx, err, "append usage event %s %s", ev.Event, ev.ID)
	}
}

type callerKey struct{}

// WithCaller tags ctx with the credential kind behind a request, for the audit records it causes.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// CallerOf is the credential kind WithCaller set on ctx, or "".
func CallerOf(ctx context.Context) string {
	caller, _ := ctx.Value(callerKey{}).(string)
	return caller
}

func memberwiseFrame(line []byte) requestFrame {
	var frame requestFrame
	var members map[string]jsontext.Value
	if json.Unmarshal(line, &members, jsontext.AllowDuplicateNames(true)) != nil {
		return frame
	}
	for name, v := range members {
		member, _ := jsontext.AppendQuote([]byte("{"), name)
		_ = json.Unmarshal(append(append(append(member, ':'), v...), '}'), &frame)
	}
	return frame
}

func protoName(proto uint8) string {
	switch proto {
	case tunnet.ProtoTCP:
		return "tcp"
	case tunnet.ProtoUDP:
		return "udp"
	case tunnet.ProtoICMP:
		return "icmp"
	default:
		return strconv.Itoa(int(proto))
	}
}
