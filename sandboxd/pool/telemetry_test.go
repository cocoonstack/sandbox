package pool

import (
	"encoding/json/v2"
	"errors"
	"io/fs"
	"maps"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

func TestUsageJournalRecordsLifecycle(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if got, want := usageEventsOf(t, m, sb.ID), "claim,hibernate,wake,release"; got != want {
		t.Errorf("events %s, want %s", got, want)
	}

	c := m.Counters()
	if c.ClaimsWarm+c.ClaimsClone+c.ClaimsCold != 1 || c.Hibernates != 1 || c.Wakes != 1 || c.Releases != 1 {
		t.Errorf("counters %+v", c)
	}
	if c.ClaimNanos == 0 || c.WakeNanos == 0 {
		t.Errorf("latency totals missing: %+v", c)
	}
}

func TestClaimLatencyFillsTheBucketThatBoundsIt(t *testing.T) {
	var h claimHistogram
	last := len(ClaimBuckets)
	for _, tt := range []struct {
		d    time.Duration
		want int
	}{
		{0, 0},
		{ClaimBuckets[0], 0},
		{ClaimBuckets[0] + 1, 1},
		{ClaimBuckets[last-1], last - 1},
		{ClaimBuckets[last-1] + 1, last},
	} {
		before := h.snapshot()
		h.observe(tt.d)
		after := h.snapshot()
		if after.Counts[tt.want] != before.Counts[tt.want]+1 {
			t.Errorf("%v: counts %v, want bucket %d to grow", tt.d, after.Counts, tt.want)
		}
		if after.Nanos-before.Nanos != uint64(tt.d) {
			t.Errorf("%v: nanos grew by %d", tt.d, after.Nanos-before.Nanos)
		}
	}
}

func TestClaimLatencyCountsPoolClaimsByTierOnly(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	refillWarmVM(t, m)
	warm, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("warm claim: %v", err)
	}
	if _, err = m.Fork(t.Context(), warm.ID, Cred{Token: warm.Token}, 1, 0, "", ""); err != nil {
		t.Fatalf("fork: %v", err)
	}
	if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Fatalf("clone claim: %v", err)
	}
	ckpt, err := m.Checkpoint(t.Context(), warm.ID, Cred{Token: warm.Token}, "", "")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err = m.ClaimCheckpoint(t.Context(), ckpt.ID, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Fatalf("claim checkpoint: %v", err)
	}
	if _, err = m.ClaimProvision(t.Context(), tplKey("cold:1"), ClaimOptions{TTL: time.Hour}); err != nil {
		t.Fatalf("cold claim: %v", err)
	}

	c := m.Counters()
	for _, tt := range []struct {
		tier    string
		latency ClaimLatency
		claims  uint64
	}{
		{"warm", c.ClaimWarmLatency, 1},
		{"clone", c.ClaimCloneLatency, 1},
		{"cold", c.ClaimColdLatency, 1},
	} {
		var total uint64
		for _, n := range tt.latency.Counts {
			total += n
		}
		if total != tt.claims || tt.latency.Nanos == 0 {
			t.Errorf("%s: %d observations and %d ns, want %d", tt.tier, total, tt.latency.Nanos, tt.claims)
		}
	}
	if c.ClaimsClone < 3 {
		t.Errorf("claims_total clone = %d, want the fork child and the checkpoint branch counted", c.ClaimsClone)
	}
	if want := c.ClaimWarmLatency.Nanos + c.ClaimCloneLatency.Nanos + c.ClaimColdLatency.Nanos; c.ClaimNanos != want {
		t.Errorf("ClaimNanos %d, want the tiers' sum %d", c.ClaimNanos, want)
	}
}

func TestJournalRotationKeepsOneBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("newJournal: %v", err)
	}
	j.size = journalMaxBytes

	if err := j.append(usage.Event{Event: "claim", ID: "sb_1"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("backup missing: %v", err)
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), `"sb_1"`) {
		t.Errorf("post-rotation file missing the event: %q", raw)
	}
}

func TestTenantStampedInJournals(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := claimTenant(t, m, "acme")

	raw, err := os.ReadFile(filepath.Join(m.dataDir, "claims.json"))
	if err != nil {
		t.Fatalf("read claims journal: %v", err)
	}
	claims := map[string]*types.Sandbox{}
	if err = json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("parse claims journal: %v", err)
	}
	if claims[sb.ID] == nil || claims[sb.ID].Tenant != "acme" {
		t.Errorf("persisted claim %+v, want tenant acme", claims[sb.ID])
	}

	found := false
	for _, ev := range usageJournal(t, m) {
		if ev.Event == "claim" && ev.ID == sb.ID {
			found = true
			if ev.Tenant != "acme" {
				t.Errorf("claim event tenant %q, want acme", ev.Tenant)
			}
			if ev.Template != testKey.Template || ev.Net != string(testKey.Net) || ev.Size != string(testKey.Size) {
				t.Errorf("claim event template %q net %q size %q, want the key %+v", ev.Template, ev.Net, ev.Size, testKey)
			}
			if !ev.Deadline.Equal(sb.Deadline) {
				t.Errorf("claim event deadline %v, want %v", ev.Deadline, sb.Deadline)
			}
		}
	}
	if !found {
		t.Error("no claim event for the tenant claim")
	}
}

func TestEveryClaimScopedUsageEventCarriesTheTenant(t *testing.T) {
	m := tenantManager(t, t.TempDir())
	ctx := t.Context()
	if err := m.PutTenant(ctx, tenants.TenantSpec{Name: "acme", Token: "acme-tok"}); err != nil {
		t.Fatalf("put tenant: %v", err)
	}
	sb := claimTenant(t, m, "acme")
	cred := Cred{Token: sb.Token}
	if _, err := m.Renew(ctx, sb.ID, cred, time.Hour, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	hibernateClaim(t, m, sb)
	if _, _, _, err := m.WakeAgentSocket(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
	children, err := m.Fork(ctx, sb.ID, cred, 1, time.Hour, "", "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if _, err = m.Checkpoint(ctx, sb.ID, cred, "", ""); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, _, err = m.Promote(ctx, sb.ID, cred, "tpl:acme", "acme"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if err = m.Release(ctx, sb.ID, cred); err != nil {
		t.Fatalf("release: %v", err)
	}
	expire(m, children[0])
	m.reapOnce(ctx)
	waitFor(t, func() bool { return strings.HasSuffix(usageEventsOf(t, m, children[0].ID), ",reap") })
	evs := usageJournal(t, m)
	if last := evs[len(evs)-1]; last.ID != children[0].ID || !last.Deadline.Equal(children[0].Deadline) {
		t.Errorf("reap %+v, want the deadline it ended under, %v", last, children[0].Deadline)
	}

	kinds := map[string]bool{}
	for _, ev := range usageJournal(t, m) {
		kinds[ev.Event] = true
		if ev.Tenant != "acme" {
			t.Errorf("%s event of %s carries tenant %q, want acme", ev.Event, ev.ID, ev.Tenant)
		}
	}
	for _, kind := range []string{"claim", "renew", "hibernate", "wake", "fork", "checkpoint", "promote", "release", "reap"} {
		if !kinds[kind] {
			t.Errorf("no %s event in the journal", kind)
		}
	}
}

func TestEveryEventThatMovesTheDeadlineStatesIt(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), archivePool(3600))
	ctx := t.Context()
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	assertDeadline := func(kind string) {
		t.Helper()
		evs := usageJournal(t, m)
		last := evs[len(evs)-1]
		if want := m.deadlineOf(sb); last.Event != kind || want.IsZero() || !last.Deadline.Equal(want) {
			t.Errorf("last event %s deadline %v, want %s deadline %v", last.Event, last.Deadline, kind, want)
		}
	}
	assertDeadline("claim")
	if _, err := m.Renew(ctx, sb.ID, cred, 2*time.Hour, ""); err != nil {
		t.Fatalf("renew: %v", err)
	}
	assertDeadline("renew")
	hibernateClaim(t, m, sb)
	if _, _, _, err := m.WakeAgentSocket(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
	assertDeadline("wake")
	mustArchive(t, m, sb)
	assertDeadline("archive")
	if _, _, _, err := m.WakeAgentSocket(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake archived: %v", err)
	}
	assertDeadline("unarchive")
}

func TestAPurgeReapStatesTheRetentionEnd(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), archivePool(3600))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	expire(m, sb)
	want := m.deadlineOf(sb)
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return strings.HasSuffix(usageEventsOf(t, m, sb.ID), ",reap") })
	evs := usageJournal(t, m)
	if last := evs[len(evs)-1]; !last.Deadline.Equal(want) {
		t.Errorf("purge reap deadline %v, want %v", last.Deadline, want)
	}
}

func TestAnArchiveKeptForeverStatesNoDeadline(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), archivePool(0))
	sb := mustClaim(t, m, testKey)
	mustArchive(t, m, sb)
	evs := usageJournal(t, m)
	if last := evs[len(evs)-1]; last.Event != "archive" || !last.Deadline.IsZero() {
		t.Errorf("last event %s deadline %v, want an archive without one", last.Event, last.Deadline)
	}
}

func TestArchiveEventPrecedesAWakeDuringItsTeardown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(3600))
		ctx := t.Context()
		sb := mustClaim(t, m, testKey)
		hibernateClaim(t, m, sb)
		stall := make(chan struct{})
		eng.mu.Lock()
		eng.removeStall = stall
		eng.mu.Unlock()
		archived := make(chan error, 1)
		go func() { archived <- m.archive(ctx, sb) }()
		synctest.Wait()
		if _, _, _, err := m.WakeAgentSocket(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("wake: %v", err)
		}
		close(stall)
		if err := <-archived; err != nil {
			t.Fatalf("archive: %v", err)
		}
		if got, want := usageEventsOf(t, m, sb.ID), "claim,hibernate,archive,unarchive"; got != want {
			t.Errorf("events %s, want %s", got, want)
		}
	})
}

func TestReconcileRecordsTheEndOfTheClaimsItDrops(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	lost := claimTenant(t, m, "acme")
	kept := mustClaim(t, m, testKey)
	delete(eng.vms, lost.VMName)

	m2 := newTestManagerAt(t, eng, dir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, want := usageEventsOf(t, m2, lost.ID), "claim,reap"; got != want {
		t.Errorf("events of the dropped claim %s, want %s", got, want)
	}
	if got, want := usageEventsOf(t, m2, kept.ID), "claim"; got != want {
		t.Errorf("events of the kept claim %s, want %s", got, want)
	}
	evs := usageJournal(t, m2)
	if last := evs[len(evs)-1]; last.Reference != "vm_gone" || last.Tenant != "acme" || last.VMName != lost.VMName || !last.Deadline.Equal(lost.Deadline) {
		t.Errorf("reap of the dropped claim %+v, want vm_gone of %s under acme with deadline %v", last, lost.VMName, lost.Deadline)
	}
}

func TestReconcileRecordsTheWakeACrashLost(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	delete(eng.stopped, sb.VMName)

	m2 := newTestManagerAt(t, eng, dir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, want := usageEventsOf(t, m2, sb.ID), "claim,hibernate,wake"; got != want {
		t.Errorf("events %s, want %s", got, want)
	}
	evs := usageJournal(t, m2)
	if last := evs[len(evs)-1]; !last.Deadline.Equal(sb.Deadline) {
		t.Errorf("wake deadline %v, want %v", last.Deadline, sb.Deadline)
	}
}

func TestReconcileRecordsTheEndOnceTheDropIsDurable(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	sb := mustClaim(t, m, testKey)
	delete(eng.vms, sb.VMName)

	m2 := newTestManagerAt(t, eng, dir)
	restore := breakClaimStore(t, m2)
	if err := m2.Reconcile(t.Context()); err == nil {
		t.Fatal("Reconcile with claims.json unwritable succeeded")
	}
	if got, want := usageEventsOf(t, m2, sb.ID), "claim"; got != want {
		t.Errorf("events after a failed save %s, want %s", got, want)
	}
	restore()

	m3 := newTestManagerAt(t, eng, dir)
	if err := m3.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, want := usageEventsOf(t, m3, sb.ID), "claim,reap"; got != want {
		t.Errorf("events after the next boot %s, want %s", got, want)
	}
}

func TestSandboxesIndexOmitsTokens(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.ClaimRef = "ns/workload"
	m.mu.Unlock()

	list := m.Sandboxes("", "", nil)
	if len(list) != 1 || list[0].ID != sb.ID || list[0].ClaimRef != "ns/workload" || list[0].Hibernated {
		t.Fatalf("index %+v", list)
	}
	raw, err := json.Marshal(list)
	if err != nil {
		t.Fatalf("marshal the index: %v", err)
	}
	if strings.Contains(string(raw), sb.Token) {
		t.Error("index leaked a token")
	}
}

func TestSandboxesFilterByClaimRef(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	a := mustClaim(t, m, testKey)
	b := mustClaim(t, m, testKey)
	unnamed := mustClaim(t, m, testKey)
	m.mu.Lock()
	a.ClaimRef, a.Tenant = "team-a/demo", "acme"
	b.ClaimRef = "team-a/other"
	m.mu.Unlock()

	for _, tt := range []struct {
		tenant, claimRef string
		want             []string
	}{
		{"", "team-a/demo", []string{a.ID}},
		{"acme", "team-a/demo", []string{a.ID}},
		{"beta", "team-a/demo", nil},
		{"", "team-a/missing", nil},
		{"", "", []string{a.ID, b.ID, unnamed.ID}},
	} {
		var got []string
		for _, row := range m.Sandboxes(tt.tenant, tt.claimRef, nil) {
			got = append(got, row.ID)
		}
		slices.Sort(tt.want)
		if !slices.Equal(got, tt.want) {
			t.Errorf("Sandboxes(%q, %q) = %v, want %v", tt.tenant, tt.claimRef, got, tt.want)
		}
	}
}

func TestGuestPortDialsWriteAuditEvents(t *testing.T) {
	tests := []struct {
		name string
		op   string
		dial func(m *Manager, sb *types.Sandbox) error
	}{
		{"preview", "preview", func(m *Manager, sb *types.Sandbox) error {
			_, err := m.PreviewDial(t.Context(), sb.ID, 8080)
			return err
		}},
		{"port passthrough", "port", func(m *Manager, sb *types.Sandbox) error {
			_, err := m.DialPort(t.Context(), sb.ID, Cred{Token: sb.Token}, 8080)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}
			m := mustManager(t, cfg, newFakeEngine())
			sb := mustClaim(t, m, testKey)
			if dialErr := tt.dial(m, sb); dialErr == nil {
				t.Fatal("fake engine dial unexpectedly succeeded")
			}

			raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
			if err != nil {
				t.Fatalf("read audit journal: %v", err)
			}
			var ev struct {
				ID   string `json:"id"`
				Op   string `json:"op"`
				Port uint16 `json:"port"`
			}
			line := strings.TrimSpace(string(raw))
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("bad audit line %q: %v", line, err)
			}
			if ev.ID != sb.ID || ev.Op != tt.op || ev.Port != 8080 {
				t.Errorf("audit event %+v", ev)
			}
			if strings.Contains(string(raw), sb.Token) {
				t.Error("audit journal leaked the sandbox token")
			}
		})
	}
}

func TestSandboxesEchoAndFilterMetadata(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	large := types.PoolKey{Template: testKey.Template, Net: types.NetNone, Size: types.SizeLarge}
	a, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Metadata: types.Metadata{"team": "a", "env": "prod"}})
	if err != nil {
		t.Fatalf("claim a: %v", err)
	}
	b, err := m.ClaimProvision(t.Context(), large, ClaimOptions{Metadata: types.Metadata{"team": "a", "env": "dev"}})
	if err != nil {
		t.Fatalf("claim b: %v", err)
	}
	bare := mustClaim(t, m, testKey)

	for _, tt := range []struct {
		filter types.Metadata
		want   []string
	}{
		{nil, []string{a.ID, b.ID, bare.ID}},
		{types.Metadata{"team": "a"}, []string{a.ID, b.ID}},
		{types.Metadata{"team": "a", "env": "dev"}, []string{b.ID}},
		{types.Metadata{"team": "b"}, nil},
	} {
		var got []string
		for _, row := range m.Sandboxes("", "", tt.filter) {
			got = append(got, row.ID)
		}
		slices.Sort(tt.want)
		if !slices.Equal(got, tt.want) {
			t.Errorf("Sandboxes(metadata %v) = %v, want %v", tt.filter, got, tt.want)
		}
	}

	row, ok := m.Sandbox(b.ID)
	if !ok || !maps.Equal(row.Metadata, b.Metadata) || row.CPUCount != 4 || row.MemTotalBytes != 4<<30 {
		t.Errorf("summary %+v, want the claim metadata and the large tier's 4 CPUs and 4 GiB", row)
	}
	if row, _ := m.Sandbox(bare.ID); row.Metadata != nil || row.CPUCount != 1 || row.MemTotalBytes != 512<<20 {
		t.Errorf("summary %+v, want no metadata and the small tier's 1 CPU and 512 MiB", row)
	}
}

func TestWarmClaimRecordsMetadata(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	m.pools[testKey].warm = append(m.pools[testKey].warm, &types.Sandbox{VMName: "sbx-warm-1", Key: testKey})
	sb, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{Metadata: types.Metadata{"team": "a"}})
	if err != nil {
		t.Fatalf("ClaimWarm: %v", err)
	}
	if row, ok := m.Sandbox(sb.ID); !ok || row.Metadata["team"] != "a" {
		t.Errorf("warm claim summary %+v, want metadata team=a", row)
	}
}

func TestClaimMetadataSurvivesARestart(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Metadata: types.Metadata{"team": "a"}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	m2 := newTestManagerAt(t, eng, dir)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if row, ok := m2.Sandbox(sb.ID); !ok || row.Metadata["team"] != "a" {
		t.Errorf("after restart %+v (found %t), want metadata team=a", row, ok)
	}
}

func TestEgressUsageEventNamesTheInjectedCredentials(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey})
	m.recordEgress(t.Context(), "sb_v", "acme", egress.Event{Method: http.MethodGet, Host: "graph.example.com", Port: 443, Decision: egress.DecisionAllow, Injected: "claim:FB"})
	if evs := usageJournal(t, m); len(evs) != 1 || evs[0].Event != "egress" || evs[0].Reference != "graph.example.com" || evs[0].Secret != "claim:FB" {
		t.Errorf("usage events %+v, want one egress to graph.example.com naming claim:FB", evs)
	}
}

func TestEgressAuditNamesTenantAndEndsARelayedFrameCannot(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}
	m := mustManager(t, cfg, newFakeEngine())
	m.recordEgress(t.Context(), "sb_v", "acme", egress.Event{
		Method: http.MethodConnect, Host: "example.com", Port: 443, Decision: egress.DecisionAllow,
		Local: netip.MustParseAddrPort("198.51.100.7:40123"), Remote: netip.MustParseAddrPort("93.184.216.34:443"),
	})
	m.Audit(t.Context(), "sb_v", []byte(`{"op":"egress","dest":"x","tenant":"other","src_addr":"198.51.100.7:1","dst_addr":"192.0.2.1:25"}`))
	lines := auditLines(t, dir)
	if len(lines) != 2 {
		t.Fatalf("audit lines %q, want 2", lines)
	}
	var got [2]egressFrame
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &got[i]); err != nil {
			t.Fatalf("bad audit line %q: %v", line, err)
		}
	}
	if got[0].Tenant != "acme" || got[0].Src.String() != "198.51.100.7:40123" || got[0].Dst.String() != "93.184.216.34:443" {
		t.Errorf("egress audit %+v, want tenant acme, 198.51.100.7:40123 -> 93.184.216.34:443", got[0])
	}
	if got[1].Tenant != "" || got[1].Src.IsValid() || got[1].Dst.IsValid() {
		t.Errorf("relayed frame set attribution %+v", got[1])
	}
}

func TestATokenReadIsAuditedWithItsCaller(t *testing.T) {
	dir := t.TempDir()
	m := mustManager(t, &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}, newFakeEngine())
	m.AuditTokenRead(WithCaller(t.Context(), "operator"), "sb_t")
	if lines := auditLines(t, dir); len(lines) != 1 || !strings.Contains(lines[0], `"id":"sb_t","op":"token","caller":"operator"`) {
		t.Errorf("audit lines %q, want one token read by the operator", lines)
	}
	offDir := t.TempDir()
	off := mustManager(t, &config.Config{DataDir: offDir, Pools: []config.PoolSpec{}}, newFakeEngine())
	off.AuditTokenRead(WithCaller(t.Context(), "operator"), "sb_t")
	if _, err := os.Stat(filepath.Join(offDir, "audit.jsonl")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("audit_log off wrote a journal: %v", err)
	}
}

func TestARelayedFrameCannotForgeOrDropItsAuditLine(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}
	m := mustManager(t, cfg, newFakeEngine())
	frames := []string{
		`{"op":"exec","argv":["id"],"decision":1,"src_addr":"x","secret":"gh","upstream":"up:3128","method":"GET","added":["t"],"caller":"root"}`,
		`{"op":"exec","argv":["id"],"path":1}`,
		`{"op":"exec","argv":["id"],"x":1,"x":2}`,
	}
	for _, frame := range frames {
		m.Audit(WithCaller(t.Context(), "claim"), "sb_f", []byte(frame))
	}
	lines := auditLines(t, dir)
	if len(lines) != len(frames) {
		t.Fatalf("audit lines %q for %d frames", lines, len(frames))
	}
	for i, line := range lines {
		if !strings.Contains(line, `"op":"exec"`) || !strings.Contains(line, `"argv":["id"]`) || !strings.Contains(line, `"caller":"claim"`) {
			t.Errorf("frame %q audited as %q, want the exec, its argv and the claim caller", frames[i], line)
		}
		if strings.Contains(line, `"caller":"root"`) {
			t.Errorf("audit line %q carries the frame's caller", line)
		}
		for _, forged := range []string{"decision", "src_addr", "secret", "upstream", "method", "added", "path"} {
			if strings.Contains(line, `"`+forged+`"`) {
				t.Errorf("audit line %q carries the frame's %s", line, forged)
			}
		}
	}
}
