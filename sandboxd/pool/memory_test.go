package pool

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/go-units"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestMemoryLimitCrossings(t *testing.T) {
	tests := []struct {
		name  string
		limit config.MemoryLimit
		anon  int64
		psi   string
		low   bool
	}{
		{name: "off", anon: 900 * units.GiB, psi: "99.00"},
		{name: "anon under", limit: config.MemoryLimit{Anon: "300G"}, anon: 299 * units.GiB, psi: "99.00"},
		{name: "anon at the limit", limit: config.MemoryLimit{Anon: "300G"}, anon: 300 * units.GiB, psi: "0.00"},
		{name: "anon over", limit: config.MemoryLimit{Anon: "300G"}, anon: 301 * units.GiB, psi: "0.00", low: true},
		{name: "pressure under", limit: config.MemoryLimit{PSISomeAvg10: 40}, anon: 900 * units.GiB, psi: "39.99"},
		{name: "pressure over", limit: config.MemoryLimit{PSISomeAvg10: 40}, psi: "40.01", low: true},
		{name: "either over", limit: config.MemoryLimit{Anon: "300G", PSISomeAvg10: 40}, anon: 10 * units.GiB, psi: "41.00", low: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memoryManager(t, tt.limit)
			observeSignals(t, m, tt.anon, tt.psi)
			if got := m.memoryLow.Load(); got != tt.low {
				t.Errorf("memory low %v, want %v", got, tt.low)
			}
		})
	}
}

func TestMemoryOverLimitRefusesClaimsUntilItFalls(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	observeAnon(t, m, 301*units.GiB)

	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Errorf("claim over the limit: %v, want ErrQuota", err)
	}
	if _, g := m.Info(); !g.AtCapacity || g.AtCapacityReason != "memory above limit" || !g.MemoryLow || g.AnonBytes != 301*units.GiB {
		t.Errorf("gauges %+v, want at capacity for memory with the anon bytes", g)
	}

	observeAnon(t, m, 299*units.GiB)
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
	observeAnon(t, m, 301*units.GiB)

	if err := m.Hibernate(t.Context(), sb.ID, cred); err != nil {
		t.Errorf("hibernate over the limit: %v", err)
	}
	if err := m.Wake(t.Context(), sb.ID, cred); err != nil {
		t.Errorf("wake of an owned claim over the limit: %v", err)
	}
	ck, err := m.Checkpoint(t.Context(), sb.ID, cred, "", "")
	if err != nil {
		t.Fatalf("checkpoint over the limit: %v", err)
	}
	if _, err := m.ClaimCheckpoint(t.Context(), ck.ID, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Errorf("checkpoint claim over the limit: %v, want ErrQuota", err)
	}
	if _, err := m.Fork(t.Context(), sb.ID, cred, 1, 0, "", ""); !errors.Is(err, ErrQuota) {
		t.Errorf("fork over the limit: %v, want ErrQuota", err)
	}
}

func TestMemoryOverLimitStillArchives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine(), archivePool(3600))
		sb := mustClaim(t, m, testKey)
		m.memoryLow.Store(true)

		backdate(m, sb, 5*time.Second)
		sweepIdle(t, m)
		m.archiveOnce(t.Context())
		waitFor(t, func() bool { return archivedCount(m) == 1 })
	})
}

func TestMemoryOverLimitAdvertisesNoPool(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	p := m.pools[testKey]
	if _, listed := m.WarmCounts()[p.hash]; !listed {
		t.Fatal("pool unlisted under the limit")
	}
	observeAnon(t, m, 301*units.GiB)
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
	observeAnon(t, m, 301*units.GiB)
	m.refillOnce(t.Context())
	if n := refilling(); n != 0 {
		t.Errorf("%d refills over the limit, want none", n)
	}
	observeAnon(t, m, 299*units.GiB)
	m.refillOnce(t.Context())
	if n := refilling(); n == 0 {
		t.Error("no refill under the limit")
	}
}

func TestAnUnreadableSignalDoesNotHoldTheNodeLow(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G", PSISomeAvg10: 40})
	observeAnon(t, m, 301*units.GiB)
	if !m.memoryLow.Load() || !m.memoryReadFailed.Load() {
		t.Errorf("anon over with no PSI: low %v, failed %v, want both set", m.memoryLow.Load(), m.memoryReadFailed.Load())
	}

	observeAnon(t, m, 299*units.GiB)
	if m.memoryLow.Load() {
		t.Error("anon under with no PSI left the node low")
	}

	observeSignals(t, m, 299*units.GiB, "0.00")
	if m.memoryLow.Load() || m.memoryReadFailed.Load() {
		t.Errorf("after the read recovered: low %v, failed %v, want both clear", m.memoryLow.Load(), m.memoryReadFailed.Load())
	}
}

func TestRemovingTheMemoryLimitClearsTheState(t *testing.T) {
	m := memoryManager(t, config.MemoryLimit{Anon: "300G"})
	observeAnon(t, m, 301*units.GiB)

	editView(m, func(v *configView) { v.memoryAnon = 0 })
	m.checkMemory(t.Context())
	if _, g := m.Info(); g.MemoryLow || g.AnonBytes != 0 {
		t.Errorf("gauges %+v, want no memory state with the limit off", g)
	}
}

func TestMemorySignalsParseTheHostProc(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc files exist on linux only")
	}
	if anon, err := readAnonBytes("/proc"); err != nil || anon <= 0 {
		t.Errorf("anon %d, err %v, want a positive size", anon, err)
	}
	if _, err := os.Stat("/proc/pressure/memory"); err != nil {
		t.Skipf("kernel has no PSI: %v", err)
	}
	if _, err := readPSISomeAvg10("/proc"); err != nil {
		t.Errorf("read PSI: %v", err)
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

func observeSignals(t *testing.T, m *Manager, anon int64, psi string) {
	t.Helper()
	pressure := "some avg10=" + psi + " avg60=0.00 avg300=0.00 total=12\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=3\n"
	if err := os.WriteFile(filepath.Join(m.procDir, "pressure", "memory"), []byte(pressure), 0o600); err != nil {
		t.Fatalf("write pressure file: %v", err)
	}
	observeAnon(t, m, anon)
}

func observeAnon(t *testing.T, m *Manager, anon int64) {
	t.Helper()
	meminfo := "MemTotal:       395136616 kB\nMemFree:         1234 kB\nAnonPages:      " + strconv.FormatInt(anon/units.KiB, 10) + " kB\nMapped:         4321 kB\nAnonHugePages:  77 kB\n"
	if err := os.WriteFile(filepath.Join(m.procDir, "meminfo"), []byte(meminfo), 0o600); err != nil {
		t.Fatalf("write meminfo: %v", err)
	}
	m.checkMemory(t.Context())
}
