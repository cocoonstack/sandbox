package pool

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/moves/movestest"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestMoveHandsARunningClaimToThePeerWithBothNodesCountingIt(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	vm := sb.VMName
	var during [][2][3]int
	mc.p.roundHook = func(int) error {
		during = append(during, [2][3]int{moveCounts(mc.s, "acme"), moveCounts(mc.t, "acme")})
		return nil
	}
	mc.p.commitHook = mc.p.roundHook

	res, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if res.Rounds != 3 || res.Downtime <= 0 {
		t.Errorf("result %+v, want a baseline, one delta and the final round with a downtime", res)
	}
	for i, counts := range during {
		if counts != [2][3]int{{1, 1, 1}, {1, 1, 1}} {
			t.Errorf("step %d counts source/target %v, want 1/1/1 on both", i, counts)
		}
	}
	if got := [2][3]int{moveCounts(mc.s, "acme"), moveCounts(mc.t, "acme")}; got != [2][3]int{{0, 0, 0}, {1, 1, 1}} {
		t.Errorf("counts after the move %v, want 0/0/0 on the source and 1/1/1 on the target", got)
	}
	moved, ok := mc.t.byID(sb.ID)
	if !ok || moved.Token != sb.Token || moved.Tenant != "acme" || moved.MoveIn != nil || !moved.Deadline.Equal(sb.Deadline) {
		t.Fatalf("target claim %+v %v, want the source's record started", moved, ok)
	}
	if _, held := mc.s.byID(sb.ID); held || !fakeOf(mc.s).removed(vm) {
		t.Errorf("source still holds %s or its VM %s", sb.ID, vm)
	}
	if fakeOf(mc.t).paused[moved.VMName] || !slices.Contains(fakeOf(mc.t).starts, moved.VMName) {
		t.Errorf("target VM %s not started", moved.VMName)
	}
	if _, ok, _ := mc.moveReg.Get(t.Context(), sb.ID); ok {
		t.Error("the committed move record outlived the move")
	}
	if got := usageEventsOf(t, mc.s, sb.ID); got != "claim,move_out" {
		t.Errorf("source usage %s, want claim,move_out", got)
	}
	if got := usageEventsOf(t, mc.t, sb.ID); got != "move_in" {
		t.Errorf("target usage %s, want move_in", got)
	}
	assertNoMoveResidue(t, mc.t, sb.ID)
}

func TestMoveDisarmsTheSourceDoorsAndArmsTheTarget(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	dialEgress(t, sb.VsockSocket)
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket)); err == nil {
		t.Error("the source still serves the moved claim's egress door")
	}
	moved, _ := mc.t.byID(sb.ID)
	dialEgress(t, moved.VsockSocket)
}

func TestMoveRefusedByTheTargetLeavesTheClaimRunningOnTheSource(t *testing.T) {
	for name, refuse := range map[string]func(t *testing.T, mc *moveCluster){
		"max_claims": func(t *testing.T, mc *moveCluster) {
			tuneView(mc.t, func(v *configView) { v.maxClaims = 1 })
			mustClaim(t, mc.t, testKey)
		},
		"max_running": func(t *testing.T, mc *moveCluster) {
			tuneView(mc.t, func(v *configView) { v.maxRunning = 1 })
			mustClaim(t, mc.t, testKey)
		},
		"tenant cap": func(t *testing.T, mc *moveCluster) {
			setTenantCaps(t, mc.t, map[string]int{"acme": 1})
			claimTenant(t, mc.t, "acme")
		},
		"memory":      func(_ *testing.T, mc *moveCluster) { mc.t.memoryLow.Store(true) },
		"move budget": func(t *testing.T, mc *moveCluster) { reserveMove(t, mc.t, "sb_other", "zed") },
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			tuneView(mc.t, func(v *configView) { v.moveConcurrency = 1 })
			sb := claimTenant(t, mc.s, "acme")
			refuse(t, mc)
			before := moveCounts(mc.t, "acme")
			if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrQuota) {
				t.Fatalf("move: %v, want ErrQuota", err)
			}
			assertStillOnSource(t, mc, sb)
			if got := moveCounts(mc.t, "acme"); got != before {
				t.Errorf("target counts %v after the refusal, want %v", got, before)
			}
			assertNoMoveResidue(t, mc.t, sb.ID)
		})
	}
}

func TestMoveFailingAtAnyStepKeepsOneOwner(t *testing.T) {
	for name, tt := range map[string]struct {
		budget time.Duration
		fail   func(mc *moveCluster)
		want   error
	}{
		"admission":       {fail: func(mc *moveCluster) { mc.p.admitErr = ErrMoveTarget }, want: ErrMoveTarget},
		"move record":     {fail: func(mc *moveCluster) { mc.moveReg.SetDown(true) }, want: ErrMoveStoreDown},
		"baseline":        {fail: func(mc *moveCluster) { fakeOf(mc.s).captureErr = errors.New("no reflink") }, want: ErrNotMovable},
		"round transfer":  {fail: func(mc *moveCluster) { mc.p.roundHook = failRound(2) }, want: ErrMoveTarget},
		"downtime budget": {budget: time.Millisecond, want: ErrMoveDowntime},
		"final capture":   {fail: func(mc *moveCluster) { fakeOf(mc.s).finalCaptureErr = errors.New("pause failed") }},
		"final transfer":  {fail: func(mc *moveCluster) { mc.p.roundHook = failRound(3) }, want: ErrMoveTarget},
		"receive":         {fail: func(mc *moveCluster) { fakeOf(mc.t).receiveErr = errors.New("restore failed") }},
		"target memory":   {fail: func(mc *moveCluster) { mc.p.commitHook = func(int) error { mc.t.memoryLow.Store(true); return nil } }, want: ErrQuota},
		"source aborted first": {fail: func(mc *moveCluster) {
			mc.p.commitHook = func(int) error { abortRecord(mc); return nil }
		}, want: ErrNotMovable},
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			sb := claimTenant(t, mc.s, "acme")
			if tt.fail != nil {
				tt.fail(mc)
			}
			_, err := mc.s.Move(t.Context(), sb.ID, "b", cmp.Or(tt.budget, time.Minute))
			if err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("move: %v, want %v", err, tt.want)
			}
			mc.moveReg.SetDown(false)
			assertStillOnSource(t, mc, sb)
			if got := moveCounts(mc.t, "acme"); got != [3]int{} {
				t.Errorf("target counts %v, want none", got)
			}
			assertNoMoveResidue(t, mc.t, sb.ID)
			if _, ok, _ := mc.moveReg.Get(t.Context(), sb.ID); ok {
				t.Error("a move record outlived the failed move")
			}
		})
	}
}

func TestMoveWhoseCommitAnswerIsLostResolvesThroughTheRecord(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	mc.p.loseCommitReply = true
	res, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute)
	if err != nil {
		t.Fatalf("move whose commit answer was lost: %v, want the committed move", err)
	}
	if res.Rounds != 3 || res.Downtime <= 0 {
		t.Errorf("result %+v, want the three rounds and the pause", res)
	}
	if _, held := mc.s.byID(sb.ID); held {
		t.Error("the source kept a claim the target committed")
	}
	if moved, ok := mc.t.byID(sb.ID); !ok || moved.MoveIn != nil {
		t.Errorf("target claim %+v %v, want it started", moved, ok)
	}
	if got := [2][3]int{moveCounts(mc.s, "acme"), moveCounts(mc.t, "acme")}; got != [2][3]int{{0, 0, 0}, {1, 1, 1}} {
		t.Errorf("counts %v, want the claim on the target only", got)
	}
}

func TestMoveKeepsThePausedGuestUntilItsAbortLands(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	mc.p.commitHook = func(int) error {
		mc.moveReg.SetDown(true)
		return ErrMoveTarget
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveStoreDown) {
		t.Fatalf("move with the registry down after the final round: %v, want ErrMoveStoreDown", err)
	}
	if !pausedOn(mc.s, sb.VMName) {
		t.Fatal("the source resumed its guest before the abort landed")
	}
	if _, _, _, err := mc.s.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrEvacuating) {
		t.Errorf("dial during the unresolved final round: %v, want ErrEvacuating", err)
	}
	time.Sleep(2 * moveRetryInterval)
	if !pausedOn(mc.s, sb.VMName) {
		t.Fatal("the source resumed its guest while the registry was down")
	}
	mc.moveReg.SetDown(false)
	waitFor(t, func() bool { return !pausedOn(mc.s, sb.VMName) && !mc.s.movingNow(sb) })
	assertStillOnSource(t, mc, sb)
	if rec, ok, _ := mc.moveReg.Get(t.Context(), sb.ID); ok {
		t.Errorf("record %+v after the abort, want it deleted", rec)
	}
}

func TestMoveTargetCountsAReservationLikeAClaim(t *testing.T) {
	mc := newMoveCluster(t)
	tuneView(mc.t, func(v *configView) { v.maxClaims = 1 })
	reserveMove(t, mc.t, "sb_r", "acme")
	if _, err := claimAny(t.Context(), mc.t, testKey, ClaimOptions{}); !errors.Is(err, ErrQuota) {
		t.Fatalf("claim beside a reservation at max_claims 1: %v, want ErrQuota", err)
	}
	if got := mc.t.Sandboxes("", "", nil); len(got) != 0 {
		t.Errorf("listing shows %d claims, want the reservation invisible", len(got))
	}
	mc.t.DropMove("sb_r")
	mustClaim(t, mc.t, testKey)
}

func TestMoveTargetDropsAReservationItsSourceAbandoned(t *testing.T) {
	mc := newMoveCluster(t)
	reserveMove(t, mc.t, "sb_r", "acme")
	backdateReservation(mc.t, "sb_r")
	mc.t.dropIdleReservations()
	assertNoMoveResidue(t, mc.t, "sb_r")
}

func TestMoveTargetKeepsTheReservationOfARoundStillArriving(t *testing.T) {
	mc := newMoveCluster(t)
	reserveMove(t, mc.t, "sb_r", "acme")
	body, stream := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- mc.t.ReceiveMoveRound("sb_r", body) }()
	waitFor(t, func() bool {
		mc.t.mu.Lock()
		defer mc.t.mu.Unlock()
		return mc.t.incoming["sb_r"].receiving == 1
	})
	backdateReservation(mc.t, "sb_r")
	mc.t.dropIdleReservations()
	if _, reserved := reservationOf(mc.t, "sb_r"); !reserved {
		t.Fatal("the idle drop took a reservation with a round still arriving")
	}
	_ = stream.CloseWithError(errors.New("source hung up"))
	if err := <-done; err == nil {
		t.Fatal("a cut round was accepted")
	}
	if _, reserved := reservationOf(mc.t, "sb_r"); !reserved {
		t.Fatal("a cut round took the reservation")
	}
	backdateReservation(mc.t, "sb_r")
	mc.t.dropIdleReservations()
	assertNoMoveResidue(t, mc.t, "sb_r")
}

func TestMoveTargetRefusesARoundThatOutlivedItsReservation(t *testing.T) {
	mc := newMoveCluster(t)
	reserveMove(t, mc.t, "sb_r", "acme")
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "state.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	capture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(&captured, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer capture.Close()
	if err := peer.SendRound(t.Context(), capture.URL, "", "/", src, []string{"state.json"}); err != nil {
		t.Fatalf("capture a round: %v", err)
	}
	body, stream := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- mc.t.ReceiveMoveRound("sb_r", body) }()
	waitFor(t, func() bool {
		mc.t.mu.Lock()
		defer mc.t.mu.Unlock()
		return mc.t.incoming["sb_r"].receiving == 1
	})
	mc.t.DropMove("sb_r")
	go func() {
		_, _ = stream.Write(captured.Bytes())
		_ = stream.Close()
	}()
	if err := <-done; !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("round over a dropped reservation: %v, want ErrUnknownSandbox", err)
	}
	assertNoMoveResidue(t, mc.t, "sb_r")
}

func TestMoveTargetLetsTheSameSourceRetryAndRefusesAnother(t *testing.T) {
	mc := newMoveCluster(t)
	tuneView(mc.t, func(v *configView) { v.moveConcurrency, v.maxClaims = 1, 1 })
	reserveMove(t, mc.t, "sb_r", "acme")
	first, _ := reservationOf(mc.t, "sb_r")
	stale := filepath.Join(mc.t.moveDir("sb_r"), "state.json")
	if err := os.WriteFile(stale, []byte("a crashed source's round"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := mc.t.AdmitMove("sb_r", MoveAdmission{Tenant: "acme", Key: testKey, From: "other"}); !errors.Is(err, ErrNotMovable) {
		t.Errorf("admission from another source: %v, want ErrNotMovable", err)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Errorf("a refused admission touched the staging dir: %v", err)
	}
	reserveMove(t, mc.t, "sb_r", "acme")
	if again, _ := reservationOf(mc.t, "sb_r"); again == first {
		t.Error("the retry kept the crashed source's reservation")
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the retry kept the crashed source's files: %v", err)
	}
	if got := moveCounts(mc.t, "acme"); got != [3]int{1, 1, 1} {
		t.Errorf("target counts %v after the retry, want one reservation", got)
	}
}

func TestMoveTargetKeepsARetrysStagingDirWhenTheCrashedSourcesRoundEnds(t *testing.T) {
	mc := newMoveCluster(t)
	reserveMove(t, mc.t, "sb_r", "acme")
	body, stream := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- mc.t.ReceiveMoveRound("sb_r", body) }()
	waitFor(t, func() bool {
		in, _ := reservationOf(mc.t, "sb_r")
		mc.t.mu.Lock()
		defer mc.t.mu.Unlock()
		return in.receiving == 1
	})
	reserveMove(t, mc.t, "sb_r", "acme")
	retried := filepath.Join(mc.t.moveDir("sb_r"), "state.json")
	if err := os.WriteFile(retried, []byte("the retry's round"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseWithError(errors.New("the crashed source's connection ended"))
	if err := <-done; !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("the old round: %v, want ErrUnknownSandbox", err)
	}
	if _, err := os.Stat(retried); err != nil {
		t.Errorf("the old round removed the retry's files: %v", err)
	}
}

func TestMoveCommitOfACrashedSourceDoesNotTakeTheRetrysReservation(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	eng := fakeOf(mc.t)
	eng.receiveEntered, eng.receiveStall = make(chan struct{}), make(chan struct{})
	reserveMove(t, mc.t, sb.ID, "")
	done := make(chan error, 1)
	go func() {
		done <- mc.t.CommitMove(context.WithoutCancel(t.Context()), sb.ID, []byte(`{"id":"`+sb.ID+`","key":{"template":"rt:24.04","net":"none","size":"small"}}`), 1)
	}()
	<-eng.receiveEntered
	reserveMove(t, mc.t, sb.ID, "")
	retry, _ := reservationOf(mc.t, sb.ID)
	if err := os.WriteFile(filepath.Join(mc.t.moveDir(sb.ID), "state.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	close(eng.receiveStall)
	if err := <-done; !errors.Is(err, ErrNotMovable) {
		t.Fatalf("the crashed source's commit: %v, want ErrNotMovable", err)
	}
	if held, ok := reservationOf(mc.t, sb.ID); !ok || held != retry {
		t.Error("the crashed source's commit took the retry's reservation")
	}
	if _, claimed := mc.t.byID(sb.ID); claimed {
		t.Error("the crashed source's commit made a claim")
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	for _, name := range eng.receives {
		if _, live := eng.vms[name]; live {
			t.Errorf("the crashed source's received VM %s survived", name)
		}
	}
}

func TestMoveOfAReplicatedClaimDropsItsRowBeforeTheFinalRound(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.replicate = replicateEvery })
	third := mc.moveNode(t, "c")
	sb := mustClaim(t, mc.s, testKey)
	replicateNow(t, mc.s, sb)
	mc.s.mu.Lock()
	replica := sb.ReplicaCk
	mc.s.mu.Unlock()
	if _, ok, _ := mc.reg.Get(t.Context(), sb.ID); !ok || replica == "" {
		t.Fatal("setup: the claim has no replica row")
	}
	mc.p.commitHook = func(int) error {
		if _, ok, _ := mc.reg.Get(t.Context(), sb.ID); ok {
			t.Error("the replica row is still adoptable at the commit")
		}
		mc.reg.Expire("a")
		if adopted, err := third.AdoptArchived(t.Context(), sb.ID, sb.Token); adopted || err != nil {
			t.Errorf("a third node adopts the moving claim: %v %v", adopted, err)
		}
		_ = mc.reg.Renew(t.Context(), "a", time.Hour)
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}
	if ckExists(t, mc.s, replica) {
		t.Error("the stale replica outlived the move")
	}
}

func TestMoveHoldsOffCapturesReleasesAndEvacuationWhileItRuns(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	other := mustClaim(t, mc.s, testKey)
	cred := Cred{Token: sb.Token}
	mc.p.roundHook = func(round int) error {
		if round != 2 {
			return nil
		}
		_, forkErr := mc.s.Fork(t.Context(), sb.ID, cred, 1, time.Minute, "", "")
		_, ckErr := mc.s.Checkpoint(t.Context(), sb.ID, cred, "ck", "")
		_, _, promoteErr := mc.s.Promote(t.Context(), sb.ID, cred, "tpl", "")
		for op, err := range map[string]error{
			"fork": forkErr, "checkpoint": ckErr, "promote": promoteErr,
			"hibernate": mc.s.Hibernate(t.Context(), sb.ID, cred),
			"release":   mc.s.Release(t.Context(), sb.ID, cred),
		} {
			if !errors.Is(err, ErrEvacuating) {
				t.Errorf("%s during a move: %v, want ErrEvacuating", op, err)
			}
		}
		mc.s.mu.Lock()
		replicable := mc.s.replicable(sb)
		mc.s.mu.Unlock()
		if replicable {
			t.Error("a replica round may capture the moving claim")
		}
		expire(mc.s, sb)
		mc.s.reapOnce(t.Context())
		if _, held := mc.s.byID(sb.ID); !held {
			t.Error("the reap took the moving claim")
		}
		tuneView(mc.s, func(v *configView) { v.moveConcurrency = 1 })
		if _, err := mc.s.Move(t.Context(), other.ID, "b", time.Minute); !errors.Is(err, ErrQuota) {
			t.Errorf("a second move past a lowered move_concurrency: %v, want ErrQuota", err)
		}
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("the move the lowered budget did not stop: %v", err)
	}
	if _, held := mc.t.byID(sb.ID); !held {
		t.Error("the target does not hold the moved claim")
	}
}

func TestMoveGivesEachRoundADeadlineFromItsSize(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	fakeOf(mc.s).migrateSizes = []int64{1 << 30, 1, 1 << 40}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Hour); err != nil {
		t.Fatalf("move: %v", err)
	}
	if len(mc.p.roundBudgets) != 3 {
		t.Fatalf("round budgets %v, want three rounds", mc.p.roundBudgets)
	}
	for i, within := range [][2]time.Duration{
		{moveRoundBase + 100*time.Second, moveRoundBase + 107*time.Second},
		{moveRoundBase - time.Second, moveRoundBase},
		{moveFinalTimeout - 5*time.Second, moveFinalTimeout},
	} {
		if got := mc.p.roundBudgets[i]; got <= within[0] || got > within[1] {
			t.Errorf("round %d budget %s, want within %v", i+1, got, within)
		}
	}
}

func TestMoveEndsWhenAFailoverTookTheClaimFromTheSource(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	mc.p.commitHook = func(int) error {
		mc.s.dropAdopted(t.Context(), sb)
		return ErrMoveTarget
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveTarget) {
		t.Fatalf("move of a claim another node took: %v, want ErrMoveTarget", err)
	}
	if mc.s.movingNow(sb) {
		t.Fatal("the move still holds its slot")
	}
	if !sb.Transition.TryLock() {
		t.Fatal("the move still holds Transition")
	}
	sb.Transition.Unlock()
	eng := fakeOf(mc.s)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.aborts) != 0 {
		t.Errorf("aborts %v on a removed VM", eng.aborts)
	}
}

func TestMoveRefusesWritesThatWouldMissTheRecordTheTargetReceives(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	var deadline time.Time
	mc.p.roundHook = func(round int) error {
		if round != 1 {
			return nil
		}
		var err error
		if deadline, err = mc.s.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, ""); err != nil {
			t.Errorf("renew during the rounds: %v", err)
		}
		patchHostOnly(t, mc.s, sb, "rounds")
		return nil
	}
	mc.p.commitHook = func(int) error {
		if _, err := mc.s.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, 2*time.Hour, ""); !errors.Is(err, ErrEvacuating) {
			t.Errorf("renew during the final round: %v, want ErrEvacuating", err)
		}
		if err := mc.s.PatchEnv(t.Context(), sb.ID, types.EnvPatch{"GH_TOKEN": {Value: "final", Guest: hostOnly}}, ""); !errors.Is(err, ErrEvacuating) {
			t.Errorf("env write during the final round: %v, want ErrEvacuating", err)
		}
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}
	moved, ok := mc.t.byID(sb.ID)
	if !ok || !moved.Deadline.Equal(deadline) || moved.Env["GH_TOKEN"].Value != "rounds" {
		t.Errorf("target claim deadline %v env %v, want the deadline %v and the env of the rounds", moved.Deadline, moved.Env, deadline)
	}
}

func TestMoveRefusesClaimsThatCannotMove(t *testing.T) {
	mc := newMoveCluster(t)
	hibernatedSb := mustClaim(t, mc.s, testKey)
	hibernateClaim(t, mc.s, hibernatedSb)
	if _, err := mc.s.Move(t.Context(), hibernatedSb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Errorf("move of a hibernated claim: %v, want ErrNotMovable", err)
	}
	fakeOf(mc.s).migrateSupportErr = errors.New("cocoon too old")
	sb := mustClaim(t, mc.s, testKey)
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Errorf("move on a cocoon without migration: %v, want ErrNotMovable", err)
	}
	if _, err := mc.s.Move(t.Context(), "sb_none", "b", time.Minute); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("move of an unknown claim: %v, want ErrUnknownSandbox", err)
	}
	if len(fakeOf(mc.s).captures) != 0 {
		t.Errorf("a refused move captured %v", fakeOf(mc.s).captures)
	}
}

func TestMoveRestartResolvesAJournaledMoveThroughItsRecord(t *testing.T) {
	const vm, series = "sbx-moved", "s1"
	aborted := func(t *testing.T, eng *fakeEngine) {
		if !slices.Contains(eng.aborts, vm+":"+series) || slices.Contains(eng.removes, vm) {
			t.Errorf("aborts %v removes %v, want the series aborted and the VM kept", eng.aborts, eng.removes)
		}
	}
	movedOn := func(t *testing.T, eng *fakeEngine) {
		if len(eng.aborts) != 0 || !slices.Contains(eng.removes, vm) {
			t.Errorf("aborts %v removes %v, want the VM removed and never resumed", eng.aborts, eng.removes)
		}
	}
	started := func(t *testing.T, eng *fakeEngine) {
		if !slices.Contains(eng.starts, vm) || slices.Contains(eng.removes, vm) {
			t.Errorf("starts %v removes %v, want the received VM started", eng.starts, eng.removes)
		}
	}
	unstarted := func(t *testing.T, eng *fakeEngine) {
		if slices.Contains(eng.starts, vm) || !slices.Contains(eng.removes, vm) {
			t.Errorf("starts %v removes %v, want the received VM removed unstarted", eng.starts, eng.removes)
		}
	}
	for name, tt := range map[string]struct {
		node       string
		from, to   string
		reach      moves.State
		wantClaim  bool
		wantRecord moves.State
		check      func(t *testing.T, eng *fakeEngine)
	}{
		"source at pending":          {node: "a", from: "a", to: "b", reach: moves.Pending, wantClaim: true, check: aborted},
		"source at final":            {node: "a", from: "a", to: "b", reach: moves.Final, wantClaim: true, check: aborted},
		"source at aborted":          {node: "a", from: "a", to: "b", reach: moves.Aborted, wantClaim: true, check: aborted},
		"source at committed":        {node: "a", from: "a", to: "b", reach: moves.Committed, wantRecord: moves.Committed, check: movedOn},
		"source at started":          {node: "a", from: "a", to: "b", reach: moves.Started, check: movedOn},
		"source, record missing":     {node: "a", check: movedOn},
		"source, another node moved": {node: "a", from: "b", to: "c", reach: moves.Pending, wantRecord: moves.Pending, check: movedOn},
		"target at final":            {node: "b", from: "a", to: "b", reach: moves.Final, wantRecord: moves.Aborted, check: unstarted},
		"target at committed":        {node: "b", from: "a", to: "b", reach: moves.Committed, wantClaim: true, wantRecord: moves.Started, check: started},
		"target at started":          {node: "b", from: "a", to: "b", reach: moves.Started, wantClaim: true, wantRecord: moves.Started, check: started},
		"target at aborted":          {node: "b", from: "a", to: "b", reach: moves.Aborted, wantRecord: moves.Aborted, check: unstarted},
		"target, record missing":     {node: "b", check: unstarted},
	} {
		t.Run(name, func(t *testing.T) {
			c := newFailoverCluster(t)
			reg := movestest.NewMem()
			c.moves = reg
			ctx := t.Context()
			journalEpoch := int64(1)
			if tt.from != "" {
				journalEpoch = seedMoveRecord(t, reg, "sb_moved", tt.from, tt.to, tt.reach)
			}
			sb := &types.Sandbox{ID: "sb_moved", VMName: vm, Key: testKey, Token: "tok", Deadline: time.Now().Add(time.Hour)}
			if tt.node == "a" {
				sb.MoveOut = &types.MoveOut{To: "b", Epoch: journalEpoch, Series: series}
			} else {
				sb.MoveIn = &types.MoveIn{From: "a", Epoch: journalEpoch}
			}
			dataDir := t.TempDir()
			seedClaims(t, dataDir, map[string]*types.Sandbox{sb.ID: sb})
			if err := os.MkdirAll(filepath.Join(dataDir, movesDir, sb.ID), 0o700); err != nil {
				t.Fatal(err)
			}
			eng := newFakeEngine()
			eng.vms[vm], eng.stopped[vm] = "/vsock/"+vm, sb.MoveIn != nil
			eng.migrateSeries[vm] = series
			m := c.nodeOn(t, eng, tt.node, dataDir, keepAnHour)
			m.liftFence()
			if err := m.Reconcile(ctx); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			waitFor(t, func() bool {
				rec, ok, _ := reg.Get(ctx, sb.ID)
				settled := !m.moveHeld(sb.ID) && (tt.wantClaim || eng.removed(vm))
				return settled && (ok && rec.State == tt.wantRecord || !ok && tt.wantRecord == "")
			})
			held, ok := m.byID(sb.ID)
			if ok != tt.wantClaim {
				t.Fatalf("claim held %v, want %v", ok, tt.wantClaim)
			}
			if ok && (held.MoveOut != nil || held.MoveIn != nil) {
				t.Errorf("claim journal out %+v in %+v, want both cleared", held.MoveOut, held.MoveIn)
			}
			eng.mu.Lock()
			defer eng.mu.Unlock()
			tt.check(t, eng)
		})
	}
}

func TestStaleMoveRecordsAreDeletedOnlyByTheirSource(t *testing.T) {
	mc := newMoveCluster(t)
	ctx, reg := t.Context(), mc.moveReg
	seedMoveRecord(t, reg, "sb_done", "a", "b", moves.Committed)
	seedMoveRecord(t, reg, "sb_open", "a", "b", moves.Pending)
	later := time.Now().Add(2 * moveStaleAge)
	stateOf := func(id string) moves.State {
		rec, _, _ := reg.Get(ctx, id)
		return rec.State
	}

	mc.s.sweepStaleMoves(ctx, later)
	if got := stateOf("sb_done"); got != moves.Committed {
		t.Errorf("committed record after the source's sweep: %q, want it kept until the target advances it", got)
	}
	if got := stateOf("sb_open"); got != "" {
		t.Errorf("open record after the source's sweep: %q, want it aborted and deleted", got)
	}

	seedMoveRecord(t, reg, "sb_open", "a", "b", moves.Pending)
	mc.t.sweepStaleMoves(ctx, later)
	if got := stateOf("sb_done"); got != moves.Started {
		t.Errorf("committed record after the target's sweep: %q, want it started and kept", got)
	}
	if got := stateOf("sb_open"); got != moves.Aborted {
		t.Errorf("open record after the target's sweep: %q, want it aborted and kept", got)
	}

	mc.s.sweepStaleMoves(ctx, later)
	for _, id := range []string{"sb_done", "sb_open"} {
		if got := stateOf(id); got != "" {
			t.Errorf("record %s after the source's second sweep: %q, want it deleted", id, got)
		}
	}
}

func TestMoveStaleSweepLeavesACommittedRecordItsTargetStillHoldsMoveInFor(t *testing.T) {
	mc := newMoveCluster(t)
	ctx, reg := t.Context(), mc.moveReg
	epoch := seedMoveRecord(t, reg, "sb_held", "a", "b", moves.Committed)
	sb := &types.Sandbox{ID: "sb_held", VMName: "sbx-held", Key: testKey, Deadline: time.Now().Add(time.Hour), MoveIn: &types.MoveIn{From: "a", Epoch: epoch}}
	mc.t.mu.Lock()
	mc.t.addClaimLocked(sb)
	mc.t.store.set(sb)
	mc.t.mu.Unlock()
	mc.t.sweepStaleMoves(ctx, time.Now().Add(2*moveStaleAge))
	if rec, _, _ := reg.Get(ctx, "sb_held"); rec.State != moves.Committed {
		t.Errorf("record %+v, want committed until the target persists its journal", rec)
	}
}

func TestMoveReceivedClaimWaitingOutAnOutageIsNotHibernatedOrServed(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	ctx := t.Context()
	lossy := &lossyMoves{Registry: mc.moveReg, downAfterCommit: true}
	mc.t.moves = lossy
	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v, want it resolved through the committed record", err)
	}
	received, ok := mc.t.byID(sb.ID)
	if !ok {
		t.Fatal("the target does not hold the received claim")
	}
	waitFor(t, func() bool {
		if !received.Transition.TryLock() {
			return false
		}
		received.Transition.Unlock()
		return true
	})
	cred := Cred{Token: sb.Token}
	backdate(mc.t, received, time.Hour)
	sweepIdle(t, mc.t)
	expire(mc.t, received)
	mc.t.reapOnce(ctx)
	if _, held := mc.t.byID(sb.ID); !held {
		t.Fatal("the reap took the received claim")
	}
	mc.t.mu.Lock()
	mc.t.evac = &evacuation{pending: map[string]*types.Sandbox{sb.ID: received}, stop: func() {}}
	mc.t.mu.Unlock()
	if err := mc.t.evacuateClaim(ctx, received); !errors.Is(err, errArchiveBusy) {
		t.Errorf("evacuation of a received claim: %v, want errArchiveBusy", err)
	}
	mc.t.mu.Lock()
	mc.t.evac = nil
	mc.t.mu.Unlock()
	_, forkErr := mc.t.Fork(ctx, sb.ID, cred, 1, time.Minute, "", "")
	_, ckErr := mc.t.Checkpoint(ctx, sb.ID, cred, "ck", "")
	_, _, promoteErr := mc.t.Promote(ctx, sb.ID, cred, "tpl", "")
	_, renewErr := mc.t.Renew(ctx, sb.ID, cred, time.Hour, "")
	_, _, _, dialErr := mc.t.WakeAgentSocket(ctx, sb.ID, sb.Token)
	for op, err := range map[string]error{
		"fork": forkErr, "checkpoint": ckErr, "promote": promoteErr, "renew": renewErr, "dial": dialErr,
		"hibernate": mc.t.Hibernate(ctx, sb.ID, cred),
		"release":   mc.t.Release(ctx, sb.ID, cred),
		"env":       mc.t.PatchEnv(ctx, sb.ID, types.EnvPatch{"GH_TOKEN": {Value: "x", Guest: hostOnly}}, ""),
		"wake":      mc.t.Wake(ctx, sb.ID, cred),
	} {
		if !errors.Is(err, ErrEvacuating) {
			t.Errorf("%s of a received claim: %v, want ErrEvacuating", op, err)
		}
	}
	if _, err := mc.t.DialPort(ctx, sb.ID, Cred{Operator: true}, 80); !errors.Is(err, ErrPaused) {
		t.Errorf("operator dial of a received claim: %v, want ErrPaused", err)
	}
	if err := mc.t.SetInstanceMetadata(ctx, sb.ID, []byte("{}")); !errors.Is(err, ErrPaused) {
		t.Errorf("instance metadata of a received claim: %v, want ErrPaused", err)
	}
	eng := fakeOf(mc.t)
	if got := eng.hibernateCount(); got != 0 {
		t.Fatalf("%d hibernates of a paused received guest", got)
	}
	if got := startsOf(mc.t); len(got) != 0 {
		t.Fatalf("started %v before the record resolved", got)
	}

	lossy.setDown(false)
	waitFor(t, func() bool { return !mc.t.moveHeld(sb.ID) })
	mc.t.mu.Lock()
	snap := received.HibernateSnap
	mc.t.mu.Unlock()
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if snap != "" || len(eng.hibernates) != 0 || len(eng.exports) != 0 || len(eng.snapSaves) != 0 || !slices.Contains(eng.starts, received.VMName) || eng.stopped[received.VMName] {
		t.Errorf("snapshot %q hibernates %v exports %v saves %v starts %v, want only the start of the paused guest", snap, eng.hibernates, eng.exports, eng.snapSaves, eng.starts)
	}
}

func TestMoveFromTheHolderOfACommittedRecordMarksItStartedAndProceeds(t *testing.T) {
	t.Run("target whose started write was lost", func(t *testing.T) {
		mc := newMoveCluster(t)
		third := mc.moveNode(t, "c")
		mc.t.moves = &lossyMoves{Registry: mc.moveReg, loseStarted: true}
		sb := mustClaim(t, mc.s, testKey)
		if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move: %v", err)
		}
		if rec, _, _ := mc.moveReg.Get(t.Context(), sb.ID); rec.State != moves.Committed {
			t.Fatalf("record %+v, want it committed since the started write was lost", rec)
		}
		if _, err := mc.t.Move(t.Context(), sb.ID, "c", time.Minute); err != nil {
			t.Fatalf("move on from the target: %v", err)
		}
		if _, held := third.byID(sb.ID); !held {
			t.Error("the third node does not hold the claim")
		}
	})
	t.Run("adopter", func(t *testing.T) {
		mc := newMoveCluster(t)
		adopter := mc.moveNode(t, "c")
		sb := mustClaim(t, adopter, testKey)
		seedMoveRecord(t, mc.moveReg, sb.ID, "a", "lost", moves.Committed)
		if _, err := adopter.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move from the adopter: %v", err)
		}
		if _, held := mc.t.byID(sb.ID); !held {
			t.Error("the target does not hold the claim")
		}
	})
}

func TestMoveBeginLeavesARecordOnlyItsHolderMaySettle(t *testing.T) {
	for name, tt := range map[string]struct {
		reach moves.State
		edit  func(sb *types.Sandbox)
	}{
		"a move in flight":         {reach: moves.Pending},
		"final":                    {reach: moves.Final},
		"held under a journal":     {reach: moves.Committed, edit: func(sb *types.Sandbox) { sb.MoveOut = &types.MoveOut{To: "b", Epoch: 1} }},
		"received and not started": {reach: moves.Committed, edit: func(sb *types.Sandbox) { sb.MoveIn = &types.MoveIn{From: "a", Epoch: 1} }},
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			sb := mustClaim(t, mc.s, testKey)
			seedMoveRecord(t, mc.moveReg, sb.ID, "z", "y", tt.reach)
			if tt.edit != nil {
				mc.s.mu.Lock()
				tt.edit(sb)
				mc.s.mu.Unlock()
			}
			if _, err := mc.s.beginMove(t.Context(), sb, "b"); !errors.Is(err, moves.ErrLost) {
				t.Errorf("begin: %v, want ErrLost", err)
			}
			if rec, _, _ := mc.moveReg.Get(t.Context(), sb.ID); rec.State != tt.reach || rec.From != "z" {
				t.Errorf("record %+v, want %s untouched", rec, tt.reach)
			}
			mc.s.mu.Lock()
			sb.MoveOut, sb.MoveIn = nil, nil
			mc.s.mu.Unlock()
		})
	}
	t.Run("a claim this node no longer holds", func(t *testing.T) {
		mc := newMoveCluster(t)
		sb := mustClaim(t, mc.s, testKey)
		seedMoveRecord(t, mc.moveReg, sb.ID, "z", "y", moves.Committed)
		mc.s.forgetClaim(t.Context(), sb)
		if _, err := mc.s.beginMove(t.Context(), sb, "b"); !errors.Is(err, moves.ErrLost) {
			t.Errorf("begin: %v, want ErrLost", err)
		}
		if rec, _, _ := mc.moveReg.Get(t.Context(), sb.ID); rec.State != moves.Committed {
			t.Errorf("record %+v, want it committed", rec)
		}
	})
}

func TestMoveCommitWhoseRegistryReplyIsLostKeepsTheOnlyGuest(t *testing.T) {
	mc := newMoveCluster(t)
	mc.t.moves = &lossyMoves{Registry: mc.moveReg, loseCommit: true}
	sb := mustClaim(t, mc.s, testKey)
	vm := sb.VMName
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v, want it resolved through the committed record", err)
	}
	if _, held := mc.s.byID(sb.ID); held || !fakeOf(mc.s).removed(vm) {
		t.Fatal("the source kept a claim the target committed")
	}
	waitFor(t, func() bool {
		moved, ok := mc.t.byID(sb.ID)
		return ok && !mc.t.moveHeld(sb.ID) && slices.Contains(startsOf(mc.t), moved.VMName)
	})
	waitFor(t, func() bool {
		rec, ok, _ := mc.moveReg.Get(t.Context(), sb.ID)
		return !ok || rec.State == moves.Started
	})
}

func TestMoveTargetKilledBetweenCommitAndStartStartsTheGuestAfterItsRestart(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	ctx := t.Context()
	killed := make(chan struct{})
	mc.t.moves = &lossyMoves{Registry: mc.moveReg, killAfterCommit: killed, processEnd: ctx.Done()}
	mc.p.commitKilled = killed

	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move whose target died after its commit: %v, want the 200 the record proves", err)
	}
	if _, held := mc.s.byID(sb.ID); held {
		t.Fatal("the source kept a claim the target committed")
	}
	if got := startsOf(mc.t); len(got) != 0 {
		t.Fatalf("the killed target started %v", got)
	}
	if rec, ok, _ := mc.moveReg.Get(ctx, sb.ID); !ok || rec.State != moves.Committed {
		t.Fatalf("record %+v %v after the source finished, want it kept committed", rec, ok)
	}

	restarted := mc.restart(t, mc.t)
	restarted.liftFence()
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitFor(t, func() bool {
		rec, ok, _ := mc.moveReg.Get(ctx, sb.ID)
		return !restarted.moveHeld(sb.ID) && ok && rec.State == moves.Started
	})
	moved, ok := restarted.byID(sb.ID)
	if !ok || moved.Token != sb.Token || !slices.Contains(startsOf(restarted), moved.VMName) || fakeOf(restarted).removed(moved.VMName) {
		t.Fatalf("restarted target claim %+v %v starts %v, want the only guest started", moved, ok, startsOf(restarted))
	}
	if got := usageEventsOf(t, restarted, sb.ID); got != "move_in" {
		t.Errorf("target usage %s, want move_in", got)
	}
}

func TestMoveTargetMarksTheRecordStartedOnlyAfterItsJournalPersists(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	ctx := t.Context()
	fakeOf(mc.t).startHook = func() { breakStore(t, mc.t) }
	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}
	if rec, ok, _ := mc.moveReg.Get(ctx, sb.ID); !ok || rec.State != moves.Committed {
		t.Fatalf("record %+v %v with the target's journal unpersisted, want committed", rec, ok)
	}
	healStore(t, mc.t)
	later := time.Now().Add(2 * moveStaleAge)
	mc.t.sweepStaleMoves(ctx, later)
	if rec, ok, _ := mc.moveReg.Get(ctx, sb.ID); !ok || rec.State != moves.Started {
		t.Fatalf("record %+v %v after the target's sweep, want started", rec, ok)
	}
	mc.s.sweepStaleMoves(ctx, later)
	if _, ok, _ := mc.moveReg.Get(ctx, sb.ID); ok {
		t.Error("the source's sweep kept a started record")
	}
}

func TestMoveAbortKeepsTheRecordUntilTheJournalIsCleared(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	ctx := t.Context()
	mc.p.commitHook = func(int) error { return ErrMoveTarget }
	mc.p.dropHook = func() { breakStore(t, mc.s) }
	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveTarget) {
		t.Fatalf("move: %v, want ErrMoveTarget", err)
	}
	if rec, ok, _ := mc.moveReg.Get(ctx, sb.ID); !ok || rec.State != moves.Aborted {
		t.Fatalf("record %+v %v with the journal unpersisted, want it kept aborted", rec, ok)
	}
	assertStillOnSource(t, mc, sb)
	healStore(t, mc.s)
	mc.s.sweepStaleMoves(ctx, time.Now().Add(2*moveStaleAge))
	if _, ok, _ := mc.moveReg.Get(ctx, sb.ID); ok {
		t.Error("the sweep kept the aborted record")
	}
}

func TestMoveSourceBackFromAnOutageNeverResumesAClaimThatMovedOn(t *testing.T) {
	mc := newMoveCluster(t)
	third := mc.moveNode(t, "c")
	sb := mustClaim(t, mc.s, testKey)
	vm := sb.VMName
	down := &lossyMoves{Registry: mc.moveReg}
	mc.s.moves = down
	mc.p.commitHook = func(int) error {
		down.setDown(true)
		return nil
	}
	mc.p.loseCommitReply = true
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveStoreDown) {
		t.Fatalf("move with the source cut off: %v, want ErrMoveStoreDown", err)
	}
	mc.p.commitHook, mc.p.loseCommitReply = nil, false
	if !pausedOn(mc.s, vm) {
		t.Fatal("the source resumed while it could not read the record")
	}
	if _, err := mc.t.Move(t.Context(), sb.ID, "c", time.Minute); err != nil {
		t.Fatalf("move on to the third node: %v", err)
	}
	if _, held := third.byID(sb.ID); !held {
		t.Fatal("the third node does not hold the claim")
	}
	down.setDown(false)
	waitFor(t, func() bool {
		_, held := mc.s.byID(sb.ID)
		return !held && !mc.s.movingNow(sb)
	})
	eng := fakeOf(mc.s)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.aborts) != 0 || !slices.Contains(eng.removes, vm) {
		t.Errorf("source aborts %v removes %v, want the VM removed and never resumed", eng.aborts, eng.removes)
	}
}

func BenchmarkQuotaErr(b *testing.B) {
	for _, incoming := range []int{0, 2} {
		b.Run(map[int]string{0: "no reservations", 2: "two reservations"}[incoming], func(b *testing.B) {
			m := newTestManager(b, newFakeEngine())
			for i := range incoming {
				m.incoming["sb_"+string(rune('a'+i))] = &inMove{tenant: "acme", touched: time.Now()}
			}
			tuneView(m, func(v *configView) { v.maxClaims, v.maxRunning = 1000, 1000 })
			for b.Loop() {
				m.mu.Lock()
				_ = m.quotaErr(1, "")
				m.mu.Unlock()
			}
		})
	}
}

type moveCluster struct {
	*failoverCluster
	moveReg *movestest.Mem
	p       *fakeMovePeer
	s, t    *Manager
}

func newMoveCluster(t *testing.T) *moveCluster {
	t.Helper()
	return newMoveClusterWith(t, func(*failoverCluster) {})
}

func newMoveClusterWith(t *testing.T, edit func(*failoverCluster)) *moveCluster {
	t.Helper()
	c := newFailoverCluster(t)
	c.egressPol = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	edit(c)
	reg := movestest.NewMem()
	c.moves = reg
	mc := &moveCluster{failoverCluster: c, moveReg: reg, p: newFakeMovePeer(t, c)}
	mc.s, mc.t = mc.moveNode(t, "a"), mc.moveNode(t, "b")
	return mc
}

func (mc *moveCluster) moveNode(t *testing.T, name string) *Manager {
	t.Helper()
	eng := newFakeEngine()
	eng.sockRoot, eng.migrateDir = outboundtest.SockRoot(t), t.TempDir()
	m := mc.nodeOn(t, eng, name, t.TempDir(), keepAnHour)
	m.liftFence()
	m.WithMovePeer(mc.p)
	setTenantCaps(t, m, map[string]int{"acme": 0})
	return m
}

type fakeMovePeer struct {
	c   *failoverCluster
	srv *httptest.Server

	mu              sync.Mutex
	admitErr        error
	roundHook       func(round int) error
	commitHook      func(round int) error
	loseCommitReply bool
	dropHook        func()
	roundBudgets    []time.Duration
	commitKilled    <-chan struct{}
}

func newFakeMovePeer(t *testing.T, c *failoverCluster) *fakeMovePeer {
	t.Helper()
	p := &fakeMovePeer{c: c}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node, id, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if err := p.target(node).ReceiveMoveRound(id, r.Body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *fakeMovePeer) Admit(_ context.Context, addr, id string, a MoveAdmission) (string, error) {
	if p.admitErr != nil {
		return "", p.admitErr
	}
	return p.target(addr).AdmitMove(id, a)
}

func (p *fakeMovePeer) SendRound(ctx context.Context, addr, id string, round int, dir string, files []string) error {
	if deadline, ok := ctx.Deadline(); ok {
		p.roundBudgets = append(p.roundBudgets, time.Until(deadline))
	}
	if hook := p.roundHook; hook != nil {
		if err := hook(round); err != nil {
			return err
		}
	}
	if err := peer.SendRound(ctx, p.srv.URL, "", "/"+addr+"/"+id, dir, files); err != nil {
		return errors.Join(ErrMoveTarget, err)
	}
	return nil
}

func (p *fakeMovePeer) Commit(ctx context.Context, addr, id string, claim []byte, epoch int64) error {
	if hook := p.commitHook; hook != nil {
		if err := hook(0); err != nil {
			return err
		}
	}
	if p.commitKilled != nil {
		go func() { _ = p.target(addr).CommitMove(ctx, id, claim, epoch) }()
		<-p.commitKilled
		return ErrMoveTarget
	}
	err := p.target(addr).CommitMove(ctx, id, claim, epoch)
	if err == nil && p.loseCommitReply {
		return ErrMoveTarget
	}
	return err
}

func (p *fakeMovePeer) Drop(_ context.Context, addr, id string) error {
	if hook := p.dropHook; hook != nil {
		hook()
	}
	p.target(addr).DropMove(id)
	return nil
}

func (p *fakeMovePeer) target(addr string) *Manager {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := slices.IndexFunc(p.c.nodes, func(m *Manager) bool { return m.nodeID == addr })
	return p.c.nodes[i]
}

type lossyMoves struct {
	moves.Registry
	downAfterCommit bool
	loseStarted     bool
	loseCommit      bool
	down            atomic.Bool
	killAfterCommit chan struct{}
	processEnd      <-chan struct{}
}

func (r *lossyMoves) Begin(ctx context.Context, id, from, to string) (int64, error) {
	if r.down.Load() {
		return 0, moves.ErrUnavailable
	}
	return r.Registry.Begin(ctx, id, from, to)
}

func (r *lossyMoves) Advance(ctx context.Context, id string, expect int64, from, to moves.State) (int64, error) {
	if r.down.Load() {
		return 0, moves.ErrUnavailable
	}
	if to == moves.Started && r.loseStarted {
		r.loseStarted = false
		return 0, moves.ErrUnavailable
	}
	epoch, err := r.Registry.Advance(ctx, id, expect, from, to)
	if err == nil && to == moves.Committed && r.killAfterCommit != nil {
		close(r.killAfterCommit)
		<-r.processEnd
		runtime.Goexit()
	}
	if err == nil && to == moves.Committed && r.downAfterCommit {
		r.down.Store(true)
		return 0, moves.ErrUnavailable
	}
	if err == nil && to == moves.Committed && r.loseCommit {
		r.loseCommit = false
		return 0, moves.ErrUnavailable
	}
	return epoch, err
}

func (r *lossyMoves) Get(ctx context.Context, id string) (moves.Record, bool, error) {
	if r.down.Load() {
		return moves.Record{}, false, moves.ErrUnavailable
	}
	return r.Registry.Get(ctx, id)
}

func (r *lossyMoves) setDown(down bool) { r.down.Store(down) }

func seedMoveRecord(t *testing.T, reg moves.Registry, id, from, to string, state moves.State) int64 {
	t.Helper()
	ctx := t.Context()
	epoch, err := reg.Begin(ctx, id, from, to)
	if err != nil {
		t.Fatalf("begin %s: %v", id, err)
	}
	path := map[moves.State][]moves.State{
		moves.Final:     {moves.Final},
		moves.Committed: {moves.Final, moves.Committed},
		moves.Started:   {moves.Final, moves.Committed, moves.Started},
		moves.Aborted:   {moves.Aborted},
	}[state]
	at, reached := epoch, moves.Pending
	for _, next := range path {
		if at, err = reg.Advance(ctx, id, at, reached, next); err != nil {
			t.Fatalf("advance %s to %s: %v", id, next, err)
		}
		reached = next
	}
	return epoch
}

func backdateReservation(m *Manager, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incoming[id].touched = time.Now().Add(-2 * moveReservationIdle)
}

func reservationOf(m *Manager, id string) (*inMove, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.incoming[id]
	return in, ok
}

func startsOf(m *Manager) []string {
	eng := fakeOf(m)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.starts)
}

func moveCounts(m *Manager, tenant string) [3]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return [3]int{len(m.claimed) + len(m.incoming), m.store.runningCount() + len(m.incoming), m.tenantLive[tenant] + m.incomingOf(tenant)}
}

func reserveMove(t *testing.T, m *Manager, id, tenant string) {
	t.Helper()
	if _, err := m.AdmitMove(id, MoveAdmission{Tenant: tenant, Key: testKey, From: "zz"}); err != nil {
		t.Fatalf("reserve %s: %v", id, err)
	}
}

func assertStillOnSource(t *testing.T, mc *moveCluster, sb *types.Sandbox) {
	t.Helper()
	held, ok := mc.s.byID(sb.ID)
	if !ok || held != sb {
		t.Fatalf("the source lost %s", sb.ID)
	}
	mc.s.mu.Lock()
	journal, moving, vm := sb.MoveOut, mc.s.moving[sb.ID], sb.VMName
	mc.s.mu.Unlock()
	if journal != nil || moving != nil || pausedOn(mc.s, vm) || fakeOf(mc.s).removed(vm) {
		t.Errorf("source claim journal %+v moving %v paused %v, want running and unmarked", journal, moving, pausedOn(mc.s, vm))
	}
	if _, held := mc.t.byID(sb.ID); held {
		t.Error("the target holds the claim too")
	}
}

func assertNoMoveResidue(t *testing.T, m *Manager, id string) {
	t.Helper()
	m.mu.Lock()
	_, reserved := m.incoming[id]
	m.mu.Unlock()
	if reserved {
		t.Errorf("node %s keeps a reservation for %s", m.nodeID, id)
	}
	if _, err := os.Stat(m.moveDir(id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("node %s keeps the staging dir of %s: %v", m.nodeID, id, err)
	}
	if _, claimed := m.byID(id); claimed {
		return
	}
	eng := fakeOf(m)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	for _, name := range eng.receives {
		if _, live := eng.vms[name]; live {
			t.Errorf("node %s keeps the received VM %s", m.nodeID, name)
		}
	}
}

func dialEgress(t *testing.T, sock string) {
	t.Helper()
	conn, err := net.Dial("unix", engine.EgressSocketPath(sock))
	if err != nil {
		t.Fatalf("no egress door at %s: %v", sock, err)
	}
	_ = conn.Close()
}

func pausedOn(m *Manager, vm string) bool {
	eng := fakeOf(m)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.paused[vm]
}

func failRound(n int) func(int) error {
	return func(round int) error {
		if round == n {
			return ErrMoveTarget
		}
		return nil
	}
}

func abortRecord(mc *moveCluster) {
	ctx := context.Background()
	for _, rec := range mustStale(mc) {
		_, _ = mc.moveReg.Advance(ctx, rec.ID, rec.Epoch, rec.State, moves.Aborted)
	}
}

func mustStale(mc *moveCluster) []moves.Record {
	recs, _ := mc.moveReg.Stale(context.Background(), time.Now().Add(time.Hour))
	return recs
}
