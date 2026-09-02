package pool

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

func TestStatsUnknownSandbox(t *testing.T) {
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
			_ = m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
			_ = m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
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
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
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
}
