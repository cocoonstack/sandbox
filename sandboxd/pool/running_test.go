package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestMaxRunningRefusesPastCapAndIgnoresSleepingClaims(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	m.maxRunning = 2
	first, second := mustClaim(t, m, testKey), mustClaim(t, m, testKey)
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Fatalf("claim past cap: %v, want ErrQuota", err)
	}
	hibernateClaim(t, m, first)
	mustClaim(t, m, testKey)
	if err := wakeClaim(t, m, first); !errors.Is(err, ErrQuota) {
		t.Fatalf("wake past cap: %v, want ErrQuota", err)
	}
	if first.HibernateSnap == "" {
		t.Fatal("a refused wake changed the claim's sleep state")
	}
	if _, g := m.Info(); g.Claimed != 3 || g.Running != 2 || g.MaxRunning != 2 || g.Hibernated != 1 {
		t.Fatalf("gauges %+v, want 3 claimed, 2 running of 2, 1 hibernated", g)
	}
	if err := m.Release(t.Context(), second.ID, Cred{Token: second.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := wakeClaim(t, m, first); err != nil {
		t.Fatalf("wake under cap: %v", err)
	}
}

func TestMaxRunningLeavesTheWarmPoolAlone(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	mustClaim(t, m, testKey)
	fillOneWarm(t, m)
	m.maxRunning = 1
	if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Fatalf("warm claim past cap: %v, want ErrQuota", err)
	}
	if got := warmLen(m, testKey); got != 1 {
		t.Errorf("warm %d after a refused claim, want 1", got)
	}
}

func TestMaxRunningCountsEveryForkChild(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	m.maxRunning = 2
	parent := mustClaim(t, m, testKey)
	if _, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 2, 0, "", ""); !errors.Is(err, ErrQuota) {
		t.Fatalf("fork past cap: %v, want ErrQuota", err)
	}
	if _, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 1, 0, "", ""); err != nil {
		t.Fatalf("fork under cap: %v", err)
	}
}

func TestARefusedWakeOfAnArchivedClaimKeepsItsRow(t *testing.T) {
	c := newFailoverCluster(t)
	a := c.node(t, "a", t.TempDir(), keepAnHour)
	sb, other := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	before, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok {
		t.Fatalf("row before the wake: %v %v", ok, err)
	}
	a.maxRunning = 1
	if wakeErr := wakeClaim(t, a, sb); !errors.Is(wakeErr, ErrQuota) {
		t.Fatalf("wake past cap: %v, want ErrQuota", wakeErr)
	}
	after, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || after.Consumed || after.Epoch != before.Epoch || sb.ArchiveCk == "" {
		t.Fatalf("row %+v present %v err %v, claim checkpoint %q, want the archive untouched", after, ok, err, sb.ArchiveCk)
	}
	if err := a.Release(t.Context(), other.ID, Cred{Token: other.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := wakeClaim(t, a, sb); err != nil {
		t.Fatalf("wake under cap: %v", err)
	}
	if _, g := a.Info(); g.Running != 1 || g.Archived != 0 {
		t.Errorf("gauges %+v, want the archived claim running", g)
	}
}

func TestConcurrentWakesStayUnderMaxRunning(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	claims := make([]*types.Sandbox, 4)
	for i := range claims {
		claims[i] = mustClaim(t, m, testKey)
		hibernateClaim(t, m, claims[i])
	}
	m.maxRunning = 2
	eng.restoreGate = make(chan struct{})
	var refused, woken atomic.Int32
	var wg sync.WaitGroup
	for _, sb := range claims {
		wg.Go(func() {
			switch err := wakeClaim(t, m, sb); {
			case err == nil:
				woken.Add(1)
			case errors.Is(err, ErrQuota):
				refused.Add(1)
			default:
				t.Errorf("wake: %v", err)
			}
		})
	}
	waitFor(t, func() bool { return refused.Load() == 2 })
	if _, g := m.Info(); g.Running != 2 {
		t.Errorf("running %d with two wakes in flight, want 2", g.Running)
	}
	close(eng.restoreGate)
	wg.Wait()
	if woken.Load() != 2 {
		t.Errorf("woken %d, want 2", woken.Load())
	}
	if _, g := m.Info(); g.Running != 2 || g.Hibernated != 2 {
		t.Errorf("gauges %+v, want 2 running and 2 hibernated", g)
	}
}

func TestAFailedWakeGivesBackItsSlot(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	m.maxRunning = 1
	eng.restoreErr = errors.New("restore failed")
	if err := wakeClaim(t, m, sb); err == nil || errors.Is(err, ErrQuota) {
		t.Fatalf("wake: %v, want the restore error", err)
	}
	if _, g := m.Info(); g.Running != 0 {
		t.Errorf("running %d after a failed wake, want 0", g.Running)
	}
	mustClaim(t, m, testKey)
}

func TestAFailedClaimHoldsNoSlotUntilItsWake(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	m.maxRunning = 1
	sb := mustClaim(t, m, testKey)
	sb.Transition.Lock()
	m.failLocked(t.Context(), sb, "vmm exited")
	sb.Transition.Unlock()
	other := mustClaim(t, m, testKey)
	if err := m.Wake(t.Context(), sb.ID, Cred{Operator: true}); !errors.Is(err, ErrQuota) {
		t.Fatalf("wake of a failed claim past cap: %v, want ErrQuota", err)
	}
	if err := m.Release(t.Context(), other.ID, Cred{Token: other.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Operator: true}); err != nil {
		t.Fatalf("wake under cap: %v", err)
	}
	if _, g := m.Info(); g.Running != 1 || g.Failed != 0 {
		t.Errorf("gauges %+v, want the claim running", g)
	}
}

func TestARestartRecountsRunningClaims(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	mustClaim(t, m, testKey)
	hibernateClaim(t, m, mustClaim(t, m, testKey))
	restarted := newTestManagerAt(t, eng, dir)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, g := restarted.Info(); g.Claimed != 2 || g.Running != 1 {
		t.Errorf("gauges %+v after a restart, want 2 claimed and 1 running", g)
	}
}

func wakeClaim(t *testing.T, m *Manager, sb *types.Sandbox) error {
	t.Helper()
	_, _, done, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if done != nil {
		done()
	}
	return err
}
