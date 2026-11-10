package pool

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

func TestSetBalloonPassesTheTargetAndTheCallerRestoresIt(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	b, err := m.SetBalloon(t.Context(), sb.ID, "50%")
	if err != nil || b.Target != 2*fakeBalloonDefault || b.Default != fakeBalloonDefault {
		t.Fatalf("set 50%%: %+v, %v", b, err)
	}
	restore := strconv.FormatInt(b.Default, 10)
	if b, err = m.SetBalloon(t.Context(), sb.ID, restore); err != nil || b.Target != fakeBalloonDefault {
		t.Fatalf("restore %s: %+v, %v", restore, b, err)
	}
	want := []string{sb.VMName + " 50% after 0 hibernates", sb.VMName + " " + restore + " after 0 hibernates"}
	if calls := balloonCalls(eng); !slices.Equal(calls, want) {
		t.Errorf("balloon calls %q, want %q", calls, want)
	}
}

func TestNothingBalloonsAnIdleClaimOnItsOwn(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, IdleHibernateSeconds: 1})
	sb := mustClaim(t, m, testKey)
	backdate(m, sb, time.Hour)
	m.reapOnce(t.Context())
	sweepIdle(t, m)
	if hibernated(m) != 1 {
		t.Fatal("the idle claim was not hibernated")
	}
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Errorf("balloon calls %q for an idle claim and its hibernate, want none", calls)
	}
}

func TestSetBalloonRefusesWhatItCannotReach(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 6})
	asleep, archived, failed, waking, capturing, arriving := mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey), mustClaim(t, m, testKey)
	m.mu.Lock()
	asleep.HibernateSnap, archived.ArchiveCk, failed.Failed = "snap", "ck_00000000000000aa", "vmm exited"
	waking.Waking, capturing.PendingSnap, arriving.MoveIn = true, "pending", true
	m.mu.Unlock()
	for _, tt := range []struct {
		id   string
		want error
	}{
		{"sb_missing", ErrUnknownSandbox},
		{asleep.ID, ErrPaused},
		{archived.ID, ErrArchived},
		{failed.ID, ErrFailed},
		{waking.ID, ErrPaused},
		{capturing.ID, ErrPaused},
		{arriving.ID, ErrEvacuating},
	} {
		if _, err := m.SetBalloon(t.Context(), tt.id, "50%"); !errors.Is(err, tt.want) {
			t.Errorf("set balloon of %s: %v, want %v", tt.id, err, tt.want)
		}
	}
	m.mu.Lock()
	asleep.HibernateSnap, archived.ArchiveCk, failed.Failed = "", "", ""
	waking.Waking, capturing.PendingSnap, arriving.MoveIn = false, "", false
	m.mu.Unlock()
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Errorf("balloon calls %q for refused claims, want none", calls)
	}

	for _, tt := range []struct {
		cocoonErr error
		want      error
	}{
		{engine.ErrBalloonUnsupported, ErrNoBalloon},
		{fmt.Errorf("%w: balloon size 4294967296 must be below guest memory 2147483648", engine.ErrBalloonTarget), ErrBalloonTarget},
	} {
		eng.mu.Lock()
		eng.balloonErr = tt.cocoonErr
		eng.mu.Unlock()
		if _, err := m.SetBalloon(t.Context(), asleep.ID, "4G"); !errors.Is(err, tt.want) {
			t.Errorf("cocoon answering %v: %v, want %v", tt.cocoonErr, err, tt.want)
		}
	}
}

func TestSetBalloonRefusesOnACocoonWithoutTheVerb(t *testing.T) {
	eng := newFakeEngine()
	eng.balloonSupportErr = errors.New("balloon reclaim requires cocoon with vm balloon")
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey})
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetBalloon(t.Context(), sb.ID, "50%"); !errors.Is(err, ErrBalloonOff) {
		t.Errorf("set balloon on a cocoon without vm balloon: %v, want ErrBalloonOff", err)
	}
	if calls := balloonCalls(eng); len(calls) != 0 {
		t.Errorf("balloon calls %q, want none", calls)
	}
}

func balloonCalls(eng *fakeEngine) []string {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.balloonCalls)
}
