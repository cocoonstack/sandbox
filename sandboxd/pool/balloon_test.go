package pool

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const balloonIdleFor = time.Minute

func TestAnIdleClaimInflatesOnceAndDeflatesWhenActiveAgain(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Fatalf("an active claim was ballooned: %q", calls)
	}

	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	sweepBalloons(t, m)
	if calls, want := balloonCalls(eng), []string{sb.VMName + " 50% after 0 hibernates"}; !slices.Equal(calls, want) {
		t.Fatalf("balloon calls %q, want one inflate however short the guest stopped", calls)
	}
	if _, g := m.Info(); g.BalloonInflated != 1 {
		t.Errorf("inflated gauge %d, want 1", g.BalloonInflated)
	}

	sb.Touch()
	sweepBalloons(t, m)
	deflate := sb.VMName + " " + strconv.FormatInt(fakeBalloonDefault, 10) + " after 0 hibernates"
	if calls := balloonCalls(eng); len(calls) != 2 || calls[1] != deflate {
		t.Fatalf("balloon calls %q, want a deflate to the boot target in bytes", calls)
	}
	c, g := m.Counters(), gaugesOf(m)
	if c.BalloonInflations != 1 || c.BalloonDeflations != 1 || c.BalloonFailures != 0 || g.BalloonInflated != 0 {
		t.Errorf("counters %d/%d/%d gauge %d, want 1/1/0 and 0", c.BalloonInflations, c.BalloonDeflations, c.BalloonFailures, g.BalloonInflated)
	}
}

func TestARelayDoesNotWaitBehindABalloonCall(t *testing.T) {
	eng := newFakeEngine()
	stall := make(chan struct{})
	eng.balloonStall = stall
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	m.balloonOnce(t.Context())
	waitFor(t, func() bool { return len(balloonCalls(eng)) == 1 })
	done := make(chan error, 1)
	go func() { done <- wakeClaim(t, m, sb) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("relay during a balloon call: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("the relay waited behind the balloon call")
	}
	close(stall)
	waitFor(t, func() bool { return !m.sweeps.balloon.Load() })
}

func TestAHeldClaimNeverInflates(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	sb.Hold()
	defer sb.UnholdIdle()
	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Errorf("a claim with an open connection was ballooned: %q", calls)
	}
}

func TestAVMWithoutABalloonIsLeftAloneAndAFailureRetries(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey, Warm: 2})
	nodev, flaky := mustClaim(t, m, testKey), mustClaim(t, m, testKey)
	backdate(m, nodev, 2*balloonIdleFor)
	eng.mu.Lock()
	eng.balloonErr = engine.ErrBalloonUnsupported
	eng.mu.Unlock()
	sweepBalloons(t, m)
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 1 || m.Counters().BalloonFailures != 0 {
		t.Fatalf("balloon calls %q failures %d, want one try and no failure for a VM with no balloon", calls, m.Counters().BalloonFailures)
	}

	backdate(m, flaky, 2*balloonIdleFor)
	eng.mu.Lock()
	eng.balloonErr = errors.New("vm sbx: vm not running")
	eng.mu.Unlock()
	sweepBalloons(t, m)
	if got := m.Counters().BalloonFailures; got != 1 {
		t.Fatalf("failures %d, want 1", got)
	}
	eng.mu.Lock()
	eng.balloonErr = nil
	eng.mu.Unlock()
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 2 {
		t.Fatalf("balloon calls %q, want no retry inside the backoff", calls)
	}
	m.mu.Lock()
	flaky.BalloonRetryAt = time.Now().Add(-time.Second)
	m.mu.Unlock()
	sweepBalloons(t, m)
	if c := m.Counters(); c.BalloonInflations != 1 || len(balloonCalls(eng)) != 3 {
		t.Errorf("inflations %d calls %q, want the failed claim retried and inflated after the backoff", c.BalloonInflations, balloonCalls(eng))
	}
}

func TestACocoonWithoutTheVerbTurnsReclaimOffAfterOneCheck(t *testing.T) {
	eng := newFakeEngine()
	eng.balloonSupportErr = errors.New("balloon reclaim requires cocoon with vm balloon")
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	backdate(m, mustClaim(t, m, testKey), 2*balloonIdleFor)
	sweepBalloons(t, m)
	sweepBalloons(t, m)
	eng.mu.Lock()
	checks := eng.balloonChecks
	eng.mu.Unlock()
	if calls := balloonCalls(eng); len(calls) != 0 || checks != 1 {
		t.Errorf("balloon calls %q after %d support checks, want none after one", calls, checks)
	}
}

func TestAnIdleHibernateInflatesFirstAndAnExplicitOneDoesNot(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), archivePool(keepAnHour))
	idle := mustClaim(t, m, testKey)
	backdate(m, idle, 5*time.Second)
	sweepIdle(t, m)
	if calls, want := balloonCalls(eng), []string{idle.VMName + " 50% after 0 hibernates"}; !slices.Equal(calls, want) || hibernated(m) != 1 {
		t.Fatalf("balloon calls %q with %d hibernated, want the inflate before the idle hibernate", calls, hibernated(m))
	}

	explicit := mustClaim(t, m, testKey)
	hibernateClaim(t, m, explicit)
	if calls := balloonCalls(eng); len(calls) != 1 {
		t.Errorf("balloon calls %q, want no inflate before an explicit hibernate", calls)
	}
}

func TestARestartedNodeLearnsBalloonStateFromCocoon(t *testing.T) {
	eng, dir := newFakeEngine(), t.TempDir()
	m := balloonManager(t, eng, dir, config.PoolSpec{PoolKey: testKey, Warm: 2})
	inflated, plain := mustClaim(t, m, testKey), mustClaim(t, m, testKey)
	eng.mu.Lock()
	eng.balloons[inflated.VMName] = types.VMBalloon{Target: 2 * fakeBalloonDefault, Default: fakeBalloonDefault}
	eng.balloons[plain.VMName] = types.VMBalloon{Target: fakeBalloonDefault, Default: fakeBalloonDefault}
	eng.mu.Unlock()

	restarted := balloonManager(t, eng, dir, config.PoolSpec{PoolKey: testKey, Warm: 2})
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	sweepBalloons(t, restarted)
	deflate := inflated.VMName + " " + strconv.FormatInt(fakeBalloonDefault, 10) + " after 0 hibernates"
	if calls := balloonCalls(eng); !slices.Equal(calls, []string{deflate}) {
		t.Errorf("balloon calls %q, want one deflate of the claim cocoon reports inflated", calls)
	}
	sweepBalloons(t, restarted)
	if calls := balloonCalls(eng); len(calls) != 1 {
		t.Errorf("balloon calls %q, want the learned state to settle both claims", calls)
	}
}

func TestAnArchiveWakeComesBackAtTheBootTarget(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), archivePool(keepAnHour))
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	mustArchive(t, m, sb)
	if err := wakeClaim(t, m, sb); err != nil {
		t.Fatalf("wake: %v", err)
	}
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 1 || gaugesOf(m).BalloonInflated != 0 {
		t.Errorf("balloon calls %q gauge %d, want no deflate of a clone that starts at its boot target", calls, gaugesOf(m).BalloonInflated)
	}
}

func TestTurningBalloonOffDeflatesTheInflatedClaims(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	editView(m, func(v *configView) { v.balloonIdle, v.balloonTarget = 0, "" })
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 2 || m.Counters().BalloonDeflations != 1 {
		t.Errorf("balloon calls %q, want the idle claim deflated once reclaim is off", calls)
	}
	sweepBalloons(t, m)
	if m.balloonSeen.Load() {
		t.Error("the sweep keeps scanning with reclaim off and nothing left to deflate")
	}
}

func balloonManager(t *testing.T, eng *fakeEngine, dataDir string, pools ...config.PoolSpec) *Manager {
	t.Helper()
	m := newTestManagerAt(t, eng, dataDir, pools...)
	editView(m, func(v *configView) { v.balloonIdle, v.balloonTarget = balloonIdleFor, "50%" })
	return m
}

func sweepBalloons(t *testing.T, m *Manager) {
	t.Helper()
	m.balloonOnce(t.Context())
	waitFor(t, func() bool { return !m.sweeps.balloon.Load() })
}

func balloonCalls(eng *fakeEngine) []string {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.balloonCalls)
}

func gaugesOf(m *Manager) Gauges {
	_, g := m.Info()
	return g
}
