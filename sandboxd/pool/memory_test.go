package pool

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const gib = int64(1) << 30

func TestMemoryLimitCrossings(t *testing.T) {
	tests := []struct {
		name  string
		limit config.MemoryLimit
		anon  int64
		psi   string
		low   bool
	}{
		{name: "off", anon: 900 * gib, psi: "99.00"},
		{name: "anon under", limit: config.MemoryLimit{Anon: "300G"}, anon: 299 * gib, psi: "99.00"},
		{name: "anon at the limit", limit: config.MemoryLimit{Anon: "300G"}, anon: 300 * gib, psi: "0.00"},
		{name: "anon over", limit: config.MemoryLimit{Anon: "300G"}, anon: 301 * gib, psi: "0.00", low: true},
		{name: "pressure under", limit: config.MemoryLimit{PSISomeAvg10: 40}, anon: 900 * gib, psi: "39.99"},
		{name: "pressure over", limit: config.MemoryLimit{PSISomeAvg10: 40}, psi: "40.01", low: true},
		{name: "either over", limit: config.MemoryLimit{Anon: "300G", PSISomeAvg10: 40}, anon: 10 * gib, psi: "41.00", low: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memoryManager(t, tt.limit)
			writeMemorySignals(t, m, tt.anon, tt.psi)
			m.checkMemory(t.Context())
			if got := m.memoryLow.Load(); got != tt.low {
				t.Errorf("memory low %v, want %v", got, tt.low)
			}
		})
	}
}

func TestMemoryOverLimitRefusesClaimsUntilItFalls(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())

	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Errorf("claim over the limit: %v, want ErrQuota", err)
	}
	if _, g := m.Info(); !g.AtCapacity || g.AtCapacityReason != "memory above limit" || !g.MemoryLow || g.AnonBytes != 301*gib {
		t.Errorf("gauges %+v, want at capacity for memory with the anon bytes", g)
	}

	writeMemorySignals(t, m, 299*gib, "0.00")
	m.checkMemory(t.Context())
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); err != nil {
		t.Errorf("claim under the limit: %v", err)
	}
	if _, g := m.Info(); g.AtCapacity || g.MemoryLow {
		t.Errorf("gauges %+v, want no capacity block under the limit", g)
	}
}

func TestMemoryOverLimitKeepsFreeingAndOwnedWakesWorking(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	sb := mustClaim(t, m, testKey)
	cred := Cred{Token: sb.Token}
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())

	if err := m.Hibernate(t.Context(), sb.ID, cred); err != nil {
		t.Errorf("hibernate over the limit: %v", err)
	}
	if err := m.Wake(t.Context(), sb.ID, cred); err != nil {
		t.Errorf("wake of an owned claim over the limit: %v", err)
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, cred, "", ""); err != nil {
		t.Errorf("checkpoint over the limit: %v", err)
	}
}

func TestMemoryOverLimitAdvertisesNoPool(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	p := m.pools[testKey]
	p.warm = append(p.warm, &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"})
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())
	if n, listed := m.WarmCounts()[p.hash]; listed {
		t.Errorf("warm count %d over the limit, want the pool unlisted so peers route around the node", n)
	}
}

func TestMemoryOverLimitParksRefill(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	p := m.pools[testKey]
	p.goldenDir, p.floor = "golden", 1
	refilling := func() int {
		m.mu.Lock()
		defer m.mu.Unlock()
		return p.refilling + len(p.warm)
	}
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())
	m.refillOnce(t.Context())
	if n := refilling(); n != 0 {
		t.Errorf("%d refills over the limit, want none", n)
	}
	writeMemorySignals(t, m, 299*gib, "0.00")
	m.checkMemory(t.Context())
	m.refillOnce(t.Context())
	if n := refilling(); n == 0 {
		t.Error("no refill under the limit")
	}
}

func TestAnUnreadableSignalKeepsTheMemoryState(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())

	if err := os.Remove(filepath.Join(m.procDir, "meminfo")); err != nil {
		t.Fatalf("remove meminfo: %v", err)
	}
	m.checkMemory(t.Context())
	if !m.memoryLow.Load() {
		t.Error("a failed read cleared the memory-low state")
	}
	if !m.memoryReadFailed.Load() {
		t.Error("a failed read was not noted")
	}

	writeMemorySignals(t, m, 299*gib, "0.00")
	m.checkMemory(t.Context())
	if m.memoryLow.Load() || m.memoryReadFailed.Load() {
		t.Errorf("after the read recovered: low %v, failed %v, want both clear", m.memoryLow.Load(), m.memoryReadFailed.Load())
	}
}

func TestRemovingTheMemoryLimitClearsTheState(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	writeMemorySignals(t, m, 301*gib, "0.00")
	m.checkMemory(t.Context())

	tuneView(m, func(v *configView) { v.memoryAnon = 0 })
	m.checkMemory(t.Context())
	if _, g := m.Info(); g.MemoryLow || g.AnonBytes != 0 {
		t.Errorf("gauges %+v, want no memory state with the limit off", g)
	}
}

func memoryManager(t *testing.T, limit config.MemoryLimit) *Manager {
	t.Helper()
	cfg := &config.Config{DataDir: t.TempDir(), MemoryLimit: &limit, Pools: []config.PoolSpec{{PoolKey: testKey}}}
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	m.procDir = t.TempDir()
	if err := os.Mkdir(filepath.Join(m.procDir, "pressure"), 0o750); err != nil {
		t.Fatalf("setup proc dir: %v", err)
	}
	return m
}

func writeMemorySignals(t *testing.T, m *Manager, anon int64, psi string) {
	t.Helper()
	meminfo := "MemTotal:       395136616 kB\nMemFree:         1234 kB\nAnonPages:      " + strconv.FormatInt(anon/1024, 10) + " kB\nMapped:         4321 kB\n"
	pressure := "some avg10=" + psi + " avg60=0.00 avg300=0.00 total=12\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=3\n"
	for name, content := range map[string]string{"meminfo": meminfo, "pressure/memory": pressure} {
		if err := os.WriteFile(filepath.Join(m.procDir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}
