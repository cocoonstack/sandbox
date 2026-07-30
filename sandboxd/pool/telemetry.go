package pool

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
)

// AuditLineCap bounds how much of a relayed request frame the audit tap
// captures and records: enough for the op and its addressing fields, never
// file payloads. The relay's tee and Audit share it, so a captured line is
// always recordable.
const AuditLineCap = 4096

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

	// Nanosecond totals paired with the counters above: avg latency for
	// dashboards without histogram machinery.
	ClaimNanos uint64
	WakeNanos  uint64
}

type counters struct {
	claimsWarm, claimsClone, claimsCold  atomic.Uint64
	wakes, hibernates                    atomic.Uint64
	forks, checkpoints, promotes         atomic.Uint64
	releases, reaps                      atomic.Uint64
	archives, unarchives, archiveDeletes atomic.Uint64
	claimNanos, wakeNanos                atomic.Uint64
}

// auditFrame is the addressing slice of a request frame worth auditing.
type auditFrame struct {
	Op       string   `json:"op"`
	Argv     []string `json:"argv,omitempty"`
	Path     string   `json:"path,omitempty"`
	Dest     string   `json:"dest,omitempty"`
	From     string   `json:"from,omitempty"`
	To       string   `json:"to,omitempty"`
	URL      string   `json:"url,omitempty"`
	Session  string   `json:"session,omitempty"`
	Port     uint16   `json:"port,omitempty"`
	Decision string   `json:"decision,omitempty"` // egress: allow|deny
	Secret   string   `json:"secret,omitempty"`   //nolint:gosec // the secret's ref name, never its value
}

// Counters snapshots the monotonic telemetry counters.
func (m *Manager) Counters() Counters {
	c := &m.counters
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
		ClaimNanos:     c.claimNanos.Load(),
		WakeNanos:      c.wakeNanos.Load(),
	}
}

// TenantClaims counts live claims per configured tenant for /metrics —
// configured tenants only, so the label cardinality stays bounded.
func (m *Manager) TenantClaims() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.tenantMax))
	for name := range m.tenantMax {
		counts[name] = m.tenantLive[name]
	}
	return counts
}

// Audit records one relayed request frame against a sandbox when the audit
// journal is enabled: the op plus its addressing fields, payloads dropped.
// It is the byte-level adapter over recordAudit for the relay's tee;
// non-relay sources (preview) call recordAudit with the frame directly.
func (m *Manager) Audit(ctx context.Context, id string, line []byte) {
	if m.audit == nil || len(line) > AuditLineCap {
		return
	}
	var frame auditFrame
	if err := json.Unmarshal(line, &frame); err != nil || frame.Op == "" {
		return // torn cap boundary or a non-frame first line: nothing to record
	}
	m.recordAudit(ctx, id, frame)
}

// AuditEnabled reports whether the relay should tap request frames at all.
func (m *Manager) AuditEnabled() bool {
	return m.audit != nil
}

func (m *Manager) recordAudit(ctx context.Context, id string, frame auditFrame) {
	if m.audit == nil {
		return
	}
	event := struct {
		Time time.Time `json:"t"`
		ID   string    `json:"id"`
		auditFrame
	}{time.Now(), id, frame}
	if err := m.audit.append(event); err != nil {
		log.WithFunc("pool.recordAudit").Error(ctx, err, "append audit event")
	}
}

// recordEgress logs one guarded-egress decision to the audit tap (secret name
// only, never its value) and meters it as an always-on usage event.
func (m *Manager) recordEgress(ctx context.Context, id, tenant string, ev egress.Event) {
	decision := "deny"
	if ev.Decision == egress.DecisionAllow {
		decision = "allow"
	}
	m.recordAudit(ctx, id, auditFrame{Op: "egress", Dest: ev.Host, Decision: decision, Secret: ev.Injected})
	m.recordUsage(ctx, usageEvent{Event: "egress", ID: id, Tenant: tenant, Reference: ev.Host})
}

// recordUsage appends one billing event; failures are logged, never
// propagated — a claim must not fail because the meter hiccuped (cocoon's
// machine-level ledger remains the audit backstop).
func (m *Manager) recordUsage(ctx context.Context, ev usageEvent) {
	ev.Time = time.Now()
	if err := m.usage.append(ev); err != nil {
		log.WithFunc("pool.recordUsage").Errorf(ctx, err, "append usage event %s %s", ev.Event, ev.ID)
	}
}
