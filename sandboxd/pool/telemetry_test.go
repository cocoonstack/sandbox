package pool

import (
	"encoding/json/v2"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestUsageJournalRecordsLifecycle(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
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

func TestJournalRotationKeepsOneBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	j, err := newJournal(path)
	if err != nil {
		t.Fatalf("newJournal: %v", err)
	}
	j.size = journalMaxBytes

	if err := j.append(usageEvent{Event: "claim", ID: "sb_1"}); err != nil {
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

func TestQuotaRefusesClaimsPastCap(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
	m.maxClaims = 1

	first := mustClaim(t, m, testKey)
	if _, err := claimAny(t.Context(), m, testKey, time.Hour); !errors.Is(err, ErrQuota) {
		t.Fatalf("claim past cap: %v, want ErrQuota", err)
	}

	if err := m.Release(t.Context(), first.ID, Cred{Token: first.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := claimAny(t.Context(), m, testKey, time.Hour); err != nil {
		t.Errorf("claim after release: %v", err)
	}
}

func TestTenantQuotaBindsPerTenant(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	setTenantCaps(t, m, map[string]int{"acme": 1, "beta": 2})

	first, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
	if err != nil {
		t.Fatalf("acme claim: %v", err)
	}
	if first.Tenant != "acme" {
		t.Errorf("tenant %q, want acme", first.Tenant)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"}); !errors.Is(err, ErrQuota) {
		t.Fatalf("acme past its cap: %v, want ErrQuota", err)
	}

	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "beta"}); err != nil {
		t.Errorf("beta claim while acme is at cap: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Errorf("root claim while acme is at cap: %v", err)
	}
	counts := m.TenantClaims()
	if counts["acme"] != 1 || counts["beta"] != 1 {
		t.Errorf("TenantClaims %v, want acme=1 beta=1", counts)
	}
	if err := m.Release(t.Context(), first.ID, Cred{Token: first.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"}); err != nil {
		t.Errorf("acme claim after release: %v", err)
	}
}

func TestTenantStampedInJournals(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour, Tenant: "acme"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

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

	usage, err := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if err != nil {
		t.Fatalf("read usage journal: %v", err)
	}
	found := false
	for line := range strings.SplitSeq(strings.TrimSpace(string(usage)), "\n") {
		var ev usageEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("bad journal line %q: %v", line, err)
		}
		if ev.Event == "claim" && ev.ID == sb.ID {
			found = true
			if ev.Tenant != "acme" {
				t.Errorf("claim event tenant %q, want acme", ev.Tenant)
			}
		}
	}
	if !found {
		t.Error("no claim event for the tenant claim")
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
	raw, _ := json.Marshal(list)
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
			m, err := NewManager(t.Context(), &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}, newFakeEngine(), testSecrets(t))
			if err != nil {
				t.Fatalf("setup manager: %v", err)
			}
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
	raw, err := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	var ev usageEvent
	if err = json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if ev.Event != "egress" || ev.Reference != "graph.example.com" || ev.Secret != "claim:FB" {
		t.Errorf("usage event %+v, want egress to graph.example.com naming claim:FB", ev)
	}
}

func usageEventsOf(t *testing.T, m *Manager, id string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	var events []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var ev usageEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("bad journal line %q: %v", line, err)
		}
		if ev.ID == id {
			events = append(events, ev.Event)
		}
	}
	return strings.Join(events, ",")
}
