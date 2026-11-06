package pool

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestMaxRunningRefusesPastCapAndIgnoresSleepingClaims(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	editView(m, func(v *configView) { v.maxRunning = 2 })
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
	editView(m, func(v *configView) { v.maxRunning = 1 })
	if _, err := m.ClaimWarm(t.Context(), testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Fatalf("warm claim past cap: %v, want ErrQuota", err)
	}
	if got := warmLen(m, testKey); got != 1 {
		t.Errorf("warm %d after a refused claim, want 1", got)
	}
}

func TestANodeAtACapGossipsNoPools(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 1})
	if _, served := m.WarmCounts()[testKey.Hash()]; !served {
		t.Fatal("a node under its caps names no pool")
	}
	editView(m, func(v *configView) { v.maxRunning = 1 })
	mustClaim(t, m, testKey)
	if counts := m.WarmCounts(); len(counts) != 0 {
		t.Errorf("counts %v at the cap, want none", counts)
	}
}

func TestMaxRunningCountsEveryForkChild(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	editView(m, func(v *configView) { v.maxRunning = 2 })
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
	editView(a, func(v *configView) { v.maxRunning = 1 })
	if wakeErr := wakeClaim(t, a, sb); !errors.Is(wakeErr, ErrQuota) {
		t.Fatalf("wake past cap: %v, want ErrQuota", wakeErr)
	}
	after, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || after.Consumed || after.Owner != before.Owner || after.Checkpoint != before.Checkpoint || sb.ArchiveCk == "" {
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
	editView(m, func(v *configView) { v.maxRunning = 2 })
	eng.restoreStall = make(chan struct{})
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
	close(eng.restoreStall)
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
	editView(m, func(v *configView) { v.maxRunning = 1 })
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
	editView(m, func(v *configView) { v.maxRunning = 1 })
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

func TestAReloadAppliesTheClaimCapsWithoutStoppingAClaim(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	mustClaim(t, m, testKey)
	mustClaim(t, m, testKey)
	next := *m.cfg
	next.MaxClaims, next.MaxRunning = 5, 1
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, g := m.Info(); g.Running != 2 || g.MaxRunning != 1 {
		t.Errorf("gauges %+v, want both claims running over the lowered cap of 1", g)
	}
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) {
		t.Fatalf("claim over the reloaded cap: %v, want ErrQuota", err)
	}
	off := next
	off.MaxClaims, off.MaxRunning = 2, 0
	if _, err := m.ReloadConfig(t.Context(), &off); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, err := claimAny(t.Context(), m, testKey, ClaimOptions{TTL: time.Hour}); !errors.Is(err, ErrQuota) || !strings.Contains(err.Error(), "live claims") {
		t.Fatalf("claim over the reloaded max_claims: %v, want ErrQuota naming live claims", err)
	}
}

func TestAReloadAppliesTheNodeSettings(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	reloadWith(t, m, func(c *config.Config) {
		c.MaxForkCount, c.ReleaseDelaySeconds, c.VMMRestart = 1, 5, types.VMMRestartNone
		c.SyncClaims, c.CheckpointTTLHours = true, 24
		c.IdleHibernateSeconds, c.ArchiveAfterSeconds, c.ArchiveDeleteAfterSeconds = 60, 120, 3600
	})
	v := m.view.Load()
	got := []time.Duration{v.releaseDelay, v.ckptTTL, v.idleDefault, v.archiveAfterDefault, v.archiveDeleteDefault}
	want := []time.Duration{5 * time.Second, 24 * time.Hour, time.Minute, 2 * time.Minute, time.Hour}
	if !slices.Equal(got, want) || v.maxFork != 1 || v.vmmRestart != types.VMMRestartNone {
		t.Errorf("durations %v max fork %d restart %q, want %v, 1 and none", got, v.maxFork, v.vmmRestart, want)
	}
	if !m.store.sync {
		t.Error("sync_claims did not reach the store")
	}
	parent := mustClaim(t, m, testKey)
	if _, err := m.Fork(t.Context(), parent.ID, Cred{Token: parent.Token}, 2, 0, "", ""); !errors.Is(err, ErrBadCount) {
		t.Errorf("fork of 2 over a reloaded limit of 1: %v, want ErrBadCount", err)
	}
}

func TestARefusedWakeIsCountedByTheLimitThatRefusedIt(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	editView(m, func(v *configView) { v.maxRunning = 1 })
	sleeper := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sleeper)
	mustClaim(t, m, testKey)
	if err := wakeClaim(t, m, sleeper); !errors.Is(err, ErrQuota) {
		t.Fatalf("wake past max_running: %v, want ErrQuota", err)
	}
	editView(m, func(v *configView) { v.maxRunning = 0 })
	m.diskLow.Store(true)
	if err := wakeClaim(t, m, sleeper); !errors.Is(err, ErrQuota) {
		t.Fatalf("wake under disk_reserve: %v, want ErrQuota", err)
	}
	m.diskLow.Store(false)
	if c := m.Counters(); c.WakesRefusedRunning != 1 || c.WakesRefusedDisk != 1 || c.WakeHandoffs != 0 {
		t.Errorf("counters running %d disk %d handoffs %d, want 1 1 0", c.WakesRefusedRunning, c.WakesRefusedDisk, c.WakeHandoffs)
	}
}

func TestAnArchivedWakeOnAFullOwnerMovesToAPeerWithRoom(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	mustClaim(t, a, testKey)
	editView(a, func(v *configView) { v.maxRunning = 1 })
	if err := wakeClaim(t, a, sb); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("wake on a full owner: %v, want ErrUnknownSandbox once a peer took it", err)
	}
	if _, held := a.byID(sb.ID); held {
		t.Error("the full owner still lists the handed-off claim")
	}
	if _, err := a.wakeResolved(t.Context(), sb); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("a wake that waited on the old owner: %v, want ErrUnknownSandbox", err)
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "b" || rec.Consumed {
		t.Fatalf("row owner %q consumed %v, want b holding it unconsumed", rec.Owner, rec.Consumed)
	}
	if err := wakeClaim(t, b, sb); err != nil {
		t.Fatalf("wake on the peer: %v", err)
	}
	if got := a.Counters().WakeHandoffs; got != 1 {
		t.Errorf("hand-offs %d, want 1", got)
	}
}

func TestAFullPeerLeavesTheWakeOnItsOwnerAndTheRetryAsksNoPeer(t *testing.T) {
	c := newFailoverCluster(t)
	a, b := c.node(t, "a", t.TempDir(), keepAnHour), c.node(t, "b", t.TempDir(), keepAnHour)
	sb := mustClaim(t, a, testKey)
	mustArchive(t, a, sb)
	mustClaim(t, a, testKey)
	mustClaim(t, b, testKey)
	editView(a, func(v *configView) { v.maxRunning = 1 })
	editView(b, func(v *configView) { v.maxRunning = 1 })
	var asked atomic.Int32
	a.WithPeerAdopt(func(ctx context.Context, id, token string, room bool) bool {
		asked.Add(1)
		adopted, err := b.adopt(ctx, id, token, room)
		return err == nil && adopted
	})
	for range 2 {
		if err := wakeClaim(t, a, sb); !errors.Is(err, ErrQuota) {
			t.Fatalf("wake with every node full: %v, want ErrQuota", err)
		}
	}
	if rec, _, _ := c.reg.Get(t.Context(), sb.ID); rec.Owner != "a" || rec.Consumed {
		t.Errorf("row owner %q consumed %v, want a taking it back", rec.Owner, rec.Consumed)
	}
	if _, held := a.byID(sb.ID); !held || asked.Load() != 1 {
		t.Errorf("owner holds %v, peers asked %d times, want the claim kept and one ask inside the quiet period", held, asked.Load())
	}
}

func TestOneHandOffRunsAtATimeAndItsCooldownStartsWhenItFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFailoverCluster(t)
		a := c.node(t, "a", t.TempDir(), keepAnHour)
		slow, next := mustClaim(t, a, testKey), mustClaim(t, a, testKey)
		mustArchive(t, a, slow)
		mustArchive(t, a, next)
		mustClaim(t, a, testKey)
		editView(a, func(v *configView) { v.maxRunning = 1 })
		var asks atomic.Int32
		release := make(chan struct{})
		a.WithPeerAdopt(func(context.Context, string, string, bool) bool {
			if asks.Add(1) == 1 {
				<-release
			}
			return false
		})
		go func() { _ = wakeClaim(t, a, slow) }()
		synctest.Wait()
		time.Sleep(handOffQuiet + time.Second)
		if err := wakeClaim(t, a, next); !errors.Is(err, ErrQuota) || asks.Load() != 1 {
			t.Fatalf("wake during a long hand-off: %v, peers asked %d times, want 429 and no second hand-off", err, asks.Load())
		}
		close(release)
		synctest.Wait()
		time.Sleep(handOffQuiet - 10*time.Second)
		if err := wakeClaim(t, a, next); !errors.Is(err, ErrQuota) || asks.Load() != 1 {
			t.Fatalf("wake inside the cooldown after a failed hand-off: %v, peers asked %d times, want 429 and no ask", err, asks.Load())
		}
	})
}

func wakeClaim(t *testing.T, m *Manager, sb *types.Sandbox) error {
	t.Helper()
	_, _, done, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if done != nil {
		done()
	}
	return err
}
