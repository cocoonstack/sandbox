package pool

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
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

func TestASweepOfManyIdleClaimsLeavesAReapItsTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		stall := make(chan struct{})
		eng.balloonStall = stall
		m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
		width := cap(m.maintSem)
		for range width + 1 {
			backdate(m, mustClaim(t, m, testKey), 2*balloonIdleFor)
		}
		m.balloonOnce(t.Context())
		synctest.Wait()
		if calls := len(balloonCalls(eng)); calls != width {
			t.Fatalf("%d balloon calls in flight, want %d", calls, width)
		}
		reaped := make(chan struct{})
		m.runBounded(t.Context(), 1, func(context.Context, int) { close(reaped) })
		synctest.Wait()
		stall <- struct{}{}
		synctest.Wait()
		select {
		case <-reaped:
		default:
			t.Error("the reap waited behind the rest of the balloon sweep")
		}
		close(stall)
		waitFor(t, func() bool { return !m.sweeps.balloon.Load() })
	})
}

func TestABalloonCallInFlightKeepsAConcurrentOneFromPublishingOverIt(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.Balloon = types.BalloonUnknown
	m.mu.Unlock()
	release := make(chan struct{})
	eng.mu.Lock()
	eng.inspectHook = func() { <-release }
	eng.mu.Unlock()
	m.balloonOnce(t.Context())
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return sb.BalloonBusy
	})
	m.balloonTo(t.Context(), sb, "50%")
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Errorf("balloon calls %q, want the second call to stand aside while the first is in flight", calls)
	}
	close(release)
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

	unreported := mustClaim(t, m, testKey)
	restarted := balloonManager(t, eng, dir, config.PoolSpec{PoolKey: testKey, Warm: 2})
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	sweepBalloons(t, restarted)
	deflate := inflated.VMName + " " + strconv.FormatInt(fakeBalloonDefault, 10) + " after 0 hibernates"
	if calls := balloonCalls(eng); len(calls) != 2 || !slices.Contains(calls, deflate) || !slices.Contains(calls, unreported.VMName+" 25% after 0 hibernates") {
		t.Errorf("balloon calls %q, want a byte deflate of the claim cocoon reports inflated and a boot-target deflate of the one it reports nothing for", calls)
	}
	sweepBalloons(t, restarted)
	if calls := balloonCalls(eng); len(calls) != 2 {
		t.Errorf("balloon calls %q, want the learned state to settle every claim", calls)
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

func TestAReloadedIdlePercentReachesClaimsAlreadyInflated(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	editView(m, func(v *configView) { v.balloonTarget = "70%" })
	sweepBalloons(t, m)
	sweepBalloons(t, m)
	if calls, want := balloonCalls(eng), []string{sb.VMName + " 50% after 0 hibernates", sb.VMName + " 70% after 0 hibernates"}; !slices.Equal(calls, want) {
		t.Errorf("balloon calls %q, want %q", calls, want)
	}
}

func TestAnIdleHibernateKeepsTheBalloonBackoff(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), archivePool(keepAnHour))
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.BalloonRetryAt = time.Now().Add(time.Hour)
	m.mu.Unlock()
	backdate(m, sb, 5*time.Second)
	sweepIdle(t, m)
	if calls := balloonCalls(eng); len(calls) != 0 || hibernated(m) != 1 {
		t.Errorf("balloon calls %q with %d hibernated, want the hibernate without a call inside the backoff", calls, hibernated(m))
	}
}

func TestTurningBalloonOffStillDeflatesAClaimThatWasAsleep(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	sweepBalloons(t, m)
	hibernateClaim(t, m, sb)
	editView(m, func(v *configView) { v.balloonIdle, v.balloonTarget = 0, "" })
	sweepBalloons(t, m)
	if !m.balloonSeen.Load() {
		t.Fatal("the sweep stopped with an inflated claim asleep")
	}
	if err := wakeClaim(t, m, sb); err != nil {
		t.Fatalf("wake: %v", err)
	}
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 2 || m.Counters().BalloonDeflations != 1 {
		t.Errorf("balloon calls %q, want the woken claim deflated once reclaim is off", calls)
	}
}

func TestAFailedInflateIsReadBackOnceTheClaimIsActive(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, 2*balloonIdleFor)
	eng.mu.Lock()
	eng.balloonErr = errors.New("cocoon vm balloon: signal: killed")
	eng.balloons[sb.VMName] = types.VMBalloon{Target: 2 * fakeBalloonDefault, Default: fakeBalloonDefault}
	eng.mu.Unlock()
	sweepBalloons(t, m)
	eng.mu.Lock()
	eng.balloonErr = nil
	eng.mu.Unlock()
	sb.Touch()
	m.mu.Lock()
	sb.BalloonRetryAt = time.Time{}
	m.mu.Unlock()
	sweepBalloons(t, m)
	deflate := sb.VMName + " " + strconv.FormatInt(fakeBalloonDefault, 10) + " after 0 hibernates"
	if calls := balloonCalls(eng); len(calls) != 2 || calls[1] != deflate {
		t.Errorf("balloon calls %q, want the inflate that reported failure read back and deflated", calls)
	}
}

func TestReclaimTurnedOnAfterARestartRepairsAnEarlierRunsInflate(t *testing.T) {
	eng, dir := newFakeEngine(), t.TempDir()
	m := newTestManagerAt(t, eng, dir, config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	eng.mu.Lock()
	eng.balloons[sb.VMName] = types.VMBalloon{Target: 2 * fakeBalloonDefault, Default: fakeBalloonDefault}
	eng.mu.Unlock()
	restarted := newTestManagerAt(t, eng, dir, config.PoolSpec{PoolKey: testKey})
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	editView(restarted, func(v *configView) { v.balloonIdle, v.balloonTarget = balloonIdleFor, "50%" })
	sweepBalloons(t, restarted)
	deflate := sb.VMName + " " + strconv.FormatInt(fakeBalloonDefault, 10) + " after 0 hibernates"
	if calls := balloonCalls(eng); !slices.Equal(calls, []string{deflate}) {
		t.Errorf("balloon calls %q, want the earlier run's inflate deflated once reclaim is on", calls)
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
}

func TestReclaimMemoryInflatesAnActiveClaimUntilItsNextUse(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	if err := m.ReclaimMemory(sb.ID); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(m.balloonKick) != 1 {
		t.Fatal("reclaim did not kick a balloon sweep")
	}
	sweepBalloons(t, m)
	sweepBalloons(t, m)
	if calls, want := balloonCalls(eng), []string{sb.VMName + " 50% after 0 hibernates"}; !slices.Equal(calls, want) {
		t.Fatalf("balloon calls %q, want one inflate of a claim used a moment ago", calls)
	}
	sb.Hold()
	sb.Unhold()
	sweepBalloons(t, m)
	if calls := balloonCalls(eng); len(calls) != 2 {
		t.Fatalf("balloon calls %q, want a deflate after the next relay", calls)
	}
}

func TestReclaimMemoryRefusesWhatTheSweepCannotInflate(t *testing.T) {
	eng := newFakeEngine()
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey, Warm: 5})
	held, asleep, archived, bare, arriving := mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey)
	held.Hold()
	defer held.UnholdIdle()
	m.mu.Lock()
	asleep.HibernateSnap, archived.ArchiveCk, bare.Balloon, arriving.MoveIn = "snap", "ck_00000000000000aa", types.BalloonUnsupported, true
	m.mu.Unlock()
	for _, tt := range []struct {
		id   string
		want error
	}{
		{"sb_missing", ErrUnknownSandbox},
		{held.ID, ErrInUse},
		{asleep.ID, ErrPaused},
		{archived.ID, ErrArchived},
		{bare.ID, ErrNoBalloon},
		{arriving.ID, ErrEvacuating},
	} {
		if err := m.ReclaimMemory(tt.id); !errors.Is(err, tt.want) {
			t.Errorf("reclaim %s: %v, want %v", tt.id, err, tt.want)
		}
	}
	m.mu.Lock()
	asleep.HibernateSnap, archived.ArchiveCk, arriving.MoveIn = "", "", false
	m.mu.Unlock()
	editView(m, func(v *configView) { v.balloonIdle = 0 })
	if err := m.ReclaimMemory(held.ID); !errors.Is(err, ErrBalloonOff) {
		t.Errorf("reclaim with balloon off: %v, want ErrBalloonOff", err)
	}
	if len(m.balloonKick) != 0 || len(balloonCalls(eng)) != 0 {
		t.Error("a refused reclaim kicked a sweep or ballooned a claim")
	}
}

func TestReclaimMemoryRefusesOnACocoonWithoutTheVerb(t *testing.T) {
	eng := newFakeEngine()
	eng.balloonSupportErr = errors.New("balloon reclaim requires cocoon with vm balloon")
	m := balloonManager(t, eng, t.TempDir(), config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	if err := m.ReclaimMemory(sb.ID); !errors.Is(err, ErrBalloonOff) {
		t.Errorf("reclaim on a cocoon without vm balloon: %v, want ErrBalloonOff", err)
	}
	if len(m.balloonKick) != 0 {
		t.Error("a refused reclaim kicked a sweep")
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
