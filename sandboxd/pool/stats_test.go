package pool

import (
	"os"
	"runtime"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

// TestStatsUnknownSandbox: an unclaimed or bad id must not surface any usage.
func TestStatsUnknownSandbox(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	if _, ok := m.Stats(t.Context(), "sb_missing"); ok {
		t.Error("Stats on an unknown id, want ok=false")
	}
}

// TestStatsHibernatedIsUnmeasured: a hibernated claim has no VMM process, so
// MemUsedMeasured must stay false rather than reading a stale or zero RSS —
// and Stats must not even ask the engine, which a hibernated claim can't answer.
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

// TestStatsNoPIDIsUnmeasured: engine.List answering with no PID (VM not found,
// or not yet running) must not be read as a zero-byte RSS.
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

// TestStatsReadsResidentSetFromPID proves the PID engine.List reports is the
// one actually read: pointing it at this test process's own PID, the
// resident-set read must succeed and land on a real value. /proc is
// Linux-only, so this is skipped elsewhere.
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
