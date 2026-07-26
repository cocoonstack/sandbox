package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestUsageJournalRecordsLifecycle(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}

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
		if ev.ID == sb.ID {
			events = append(events, ev.Event)
		}
	}
	want := []string{"claim", "hibernate", "wake", "release"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Errorf("events %v, want %v", events, want)
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
	defer func() { _ = j.close() }()
	j.size = journalMaxBytes // force the next append to rotate

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
	// The refused VM must not leak, and the pool state stays sane.
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
	m.tenantMax = map[string]int{"acme": 1, "beta": 2}

	first, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "")
	if err != nil {
		t.Fatalf("acme claim: %v", err)
	}
	if first.Tenant != "acme" {
		t.Errorf("tenant %q, want acme", first.Tenant)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", ""); !errors.Is(err, ErrQuota) {
		t.Fatalf("acme past its cap: %v, want ErrQuota", err)
	}
	// The node-wide cap (unset here) stays untouched: other tenants and root
	// keep claiming while acme is full.
	if _, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "beta", ""); err != nil {
		t.Errorf("beta claim while acme is at cap: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "", ""); err != nil {
		t.Errorf("root claim while acme is at cap: %v", err)
	}
	counts := m.TenantClaims()
	if counts["acme"] != 1 || counts["beta"] != 1 {
		t.Errorf("TenantClaims %v, want acme=1 beta=1", counts)
	}
	if err := m.Release(t.Context(), first.ID, Cred{Token: first.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", ""); err != nil {
		t.Errorf("acme claim after release: %v", err)
	}
}

func TestTenantStampedInJournals(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "")
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

	list := m.Sandboxes()
	if len(list) != 1 || list[0].ID != sb.ID || list[0].ClaimRef != "ns/workload" || list[0].Hibernated {
		t.Fatalf("index %+v", list)
	}
	raw, _ := json.Marshal(list)
	if strings.Contains(string(raw), sb.Token) {
		t.Error("index leaked a token")
	}
}

func TestPreviewDialWritesAuditEvent(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dir, AuditLog: true, Pools: []config.PoolSpec{}}, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	sb := mustClaim(t, m, testKey)
	// The fake engine cannot complete the dial; the audit event records the
	// access attempt against the live claim, before the engine is involved.
	if _, dialErr := m.PreviewDial(t.Context(), sb.ID, 8080); dialErr == nil {
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
	if ev.ID != sb.ID || ev.Op != "preview_dial" || ev.Port != 8080 {
		t.Errorf("audit event %+v", ev)
	}
}
