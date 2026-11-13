package pool

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestStatsOfAnUnknownIDIsNotOK(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	if _, ok := m.Stats(t.Context(), "sb_missing"); ok {
		t.Error("Stats on an unknown id, want ok=false")
	}
}

func TestOperatorReadsRaceFreeUnderHibernate(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	sb := mustClaim(t, m, testKey)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		for range 100 {
			if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Errorf("hibernate: %v", err)
				return
			}
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Errorf("wake: %v", err)
				return
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				m.Sandbox(sb.ID)
				m.Stats(t.Context(), sb.ID)
			}
		}
	})
	wg.Wait()
}

func TestStatsHibernatedIsUnmeasured(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	before := eng.listCalls()

	st, ok := m.Stats(t.Context(), sb.ID)
	if !ok {
		t.Fatal("Stats: want ok=true for a claimed sandbox")
	}
	if !st.Hibernated {
		t.Error("Hibernated = false, want true")
	}
	if st.MemUsedMeasured {
		t.Error("MemUsedMeasured = true for a hibernated claim, want false")
	}
	if st.CPUMeasured {
		t.Error("CPUMeasured = true for a hibernated claim, want false")
	}
	if got := eng.listCalls(); got != before {
		t.Errorf("engine.List called %d more time(s), want 0: a hibernated claim must not consult the engine", got-before)
	}
}

func TestStatsNoPIDIsUnmeasured(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	sb := mustClaim(t, m, testKey)

	st, ok := m.Stats(t.Context(), sb.ID)
	if !ok {
		t.Fatal("Stats: want ok=true for a claimed sandbox")
	}
	if st.MemUsedMeasured {
		t.Error("MemUsedMeasured = true with no PID in the engine's record, want false")
	}
	if st.CPUMeasured {
		t.Error("CPUMeasured = true with no PID in the engine's record, want false")
	}
}

func TestStatsReadsResidentSetFromPID(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc/PID/statm is Linux-only")
	}
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	sb := mustClaim(t, m, testKey)
	eng.pids[sb.VMName] = os.Getpid()

	st, ok := m.Stats(t.Context(), sb.ID)
	if !ok {
		t.Fatal("Stats: want ok=true for a claimed sandbox")
	}
	if !st.MemUsedMeasured {
		t.Fatal("MemUsedMeasured = false, want true with a live PID")
	}
	if st.MemUsedBytes <= 0 {
		t.Errorf("MemUsedBytes = %d, want > 0", st.MemUsedBytes)
	}
	if !st.CPUMeasured {
		t.Error("CPUMeasured = false, want true with a live PID")
	}
}

func TestCPUSecondsSumsUserAndSystemTime(t *testing.T) {
	proc := t.TempDir()
	writeStat(t, proc, 42, "42 (cloud (hyper) visor) S 1 42 42 0 -1 4194560 100 0 0 0 1234 566 0 0 20 0 8 0 100 0 0\n")
	got, ok := cpuSeconds(proc, 42)
	if !ok {
		t.Fatal("cpuSeconds: want ok=true")
	}
	if got != 18 {
		t.Errorf("cpuSeconds = %v, want 18 (1234+566 ticks at 100 Hz)", got)
	}
}

func TestCPUSecondsOfAnUnreadableStatIsUnmeasured(t *testing.T) {
	proc := t.TempDir()
	writeStat(t, proc, 1, "1 cloud-hypervisor S 1 1 1 0 -1 0 0 0 0 0 1 2\n")
	writeStat(t, proc, 2, "2 (cloud-hypervisor) S 1 2 2 0 -1 0 0 0 0 0 1\n")
	writeStat(t, proc, 3, "3 (cloud-hypervisor) S 1 3 3 0 -1 0 0 0 0 0 x 2\n")
	for _, pid := range []int{1, 2, 3, 4} {
		if got, ok := cpuSeconds(proc, pid); ok {
			t.Errorf("cpuSeconds(pid %d) = %v, want ok=false", pid, got)
		}
	}
}

func writeStat(t *testing.T, proc string, pid int, line string) {
	t.Helper()
	dir := filepath.Join(proc, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
}
