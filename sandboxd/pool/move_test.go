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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/moves"
	"github.com/cocoonstack/sandbox/sandboxd/moves/movestest"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/store/peer"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestMoveHandsARunningClaimToThePeerWithBothNodesCountingIt(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	vm := sb.VMName
	var during [][2][3]int
	mc.p.roundHook = func(int) error {
		during = append(during, mc.counts("acme"))
		return nil
	}
	mc.p.commitHook = mc.p.roundHook

	res, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if res.Rounds != 3 || res.Downtime <= 0 || res.Send <= 0 || res.Commit <= 0 || res.Capture+res.Send+res.Commit > res.Downtime {
		t.Errorf("result %+v, want a baseline, one delta and the final round with a downtime that covers its phases", res)
	}
	for i, counts := range during {
		if counts != [2][3]int{{1, 1, 1}, {1, 1, 1}} {
			t.Errorf("step %d counts source/target %v, want 1/1/1 on both", i, counts)
		}
	}
	if got := mc.counts("acme"); got != [2][3]int{{0, 0, 0}, {1, 1, 1}} {
		t.Errorf("counts after the move %v, want 0/0/0 on the source and 1/1/1 on the target", got)
	}
	moved, ok := mc.t.byID(sb.ID)
	if !ok || moved.Token != sb.Token || moved.Tenant != "acme" || moved.MoveIn || !moved.Deadline.Equal(sb.Deadline) {
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

func TestMoveRefusesAClaimWhoseSaveIsUnsettled(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	mc.s.mu.Lock()
	sb.SaveSnap = "sbx-fork-unsettled"
	mc.s.mu.Unlock()
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Errorf("move with an unsettled save: %v, want ErrNotMovable", err)
	}
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
			editView(mc.t, func(v *configView) { v.maxClaims = 1 })
			mustClaim(t, mc.t, testKey)
		},
		"max_running": func(t *testing.T, mc *moveCluster) {
			editView(mc.t, func(v *configView) { v.maxRunning = 1 })
			mustClaim(t, mc.t, testKey)
		},
		"tenant cap": func(t *testing.T, mc *moveCluster) {
			setTenantCaps(t, mc.t, map[string]int{"acme": 1})
			claimTenant(t, mc.t, "acme")
		},
		"memory":      func(_ *testing.T, mc *moveCluster) { mc.t.memoryLow.Store(true) },
		"move budget": func(t *testing.T, mc *moveCluster) { reserveMove(t, mc.t, "sb_other", "acme") },
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			editView(mc.t, func(v *configView) { v.moveConcurrency = 1 })
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
		fail   func(t *testing.T, mc *moveCluster)
		want   error
	}{
		"admission":       {fail: func(_ *testing.T, mc *moveCluster) { mc.p.admitErr = ErrMoveTarget }, want: ErrMoveTarget},
		"move record":     {fail: func(_ *testing.T, mc *moveCluster) { mc.moveReg.SetDown(true) }, want: ErrMoveStoreDown},
		"baseline":        {fail: func(_ *testing.T, mc *moveCluster) { fakeOf(mc.s).captureErr = errors.New("no reflink") }, want: ErrNotMovable},
		"round transfer":  {fail: func(_ *testing.T, mc *moveCluster) { mc.p.roundHook = failRound(2) }, want: ErrMoveTarget},
		"downtime budget": {budget: time.Millisecond, want: ErrMoveDowntime},
		"delta capture": {fail: func(_ *testing.T, mc *moveCluster) {
			mc.p.roundHook = func(round int) error {
				if round == 1 {
					fakeOf(mc.s).captureErr = errors.New("vmm gone")
				}
				return nil
			}
		}, want: ErrNotMovable},
		"final capture":  {fail: func(_ *testing.T, mc *moveCluster) { fakeOf(mc.s).finalCaptureErr = errors.New("pause failed") }, want: ErrNotMovable},
		"final transfer": {fail: func(_ *testing.T, mc *moveCluster) { mc.p.roundHook = failRound(3) }, want: ErrMoveTarget},
		"receive":        {fail: func(_ *testing.T, mc *moveCluster) { fakeOf(mc.t).receiveErr = errors.New("restore failed") }},
		"target memory": {fail: func(_ *testing.T, mc *moveCluster) {
			mc.p.commitHook = func(int) error { mc.t.memoryLow.Store(true); return nil }
		}, want: ErrQuota},
		"source aborted first": {fail: func(t *testing.T, mc *moveCluster) {
			mc.p.commitHook = func(int) error { abortRecord(t, mc); return nil }
		}, want: ErrNotMovable},
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			sb := claimTenant(t, mc.s, "acme")
			if tt.fail != nil {
				tt.fail(t, mc)
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
	if moved, ok := mc.t.byID(sb.ID); !ok || moved.MoveIn {
		t.Errorf("target claim %+v %v, want it started", moved, ok)
	}
	if got := mc.counts("acme"); got != [2][3]int{{0, 0, 0}, {1, 1, 1}} {
		t.Errorf("counts %v, want the claim on the target only", got)
	}
}

func TestMoveKeepsThePausedGuestUntilItsAbortLands(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	reads := &countingMoves{Registry: mc.s.moves}
	mc.s.moves = reads
	mc.p.commitHook = func(int) error {
		mc.moveReg.SetDown(true)
		return ErrMoveTarget
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveStoreDown) {
		t.Fatalf("move with the registry down after the final round: %v, want ErrMoveStoreDown", err)
	}
	failed := reads.failedGets.Load()
	if !pausedOn(mc.s, sb.VMName) {
		t.Fatal("the source resumed its guest before the abort landed")
	}
	if _, _, _, err := mc.s.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrEvacuating) {
		t.Errorf("dial during the unresolved final round: %v, want ErrEvacuating", err)
	}
	waitFor(t, func() bool { return reads.failedGets.Load() > failed })
	failed = reads.failedGets.Load()
	waitFor(t, func() bool { return reads.failedGets.Load() > failed })
	if !pausedOn(mc.s, sb.VMName) {
		t.Fatal("the source resumed its guest while the registry was down")
	}
	mc.moveReg.SetDown(false)
	waitFor(t, func() bool { return !pausedOn(mc.s, sb.VMName) && !movingOn(mc.s, sb) })
	assertStillOnSource(t, mc, sb)
	if rec, ok, _ := mc.moveReg.Get(t.Context(), sb.ID); ok {
		t.Errorf("record %+v after the abort, want it deleted", rec)
	}
}

func TestMoveWhoseAbortFailsBlamesTheAbortNotTheRegistry(t *testing.T) {
	mc := newMoveCluster(t)
	sb := claimTenant(t, mc.s, "acme")
	eng := fakeOf(mc.s)
	eng.abortErr = errors.New("vmm busy")
	mc.p.commitHook = func(int) error { return ErrMoveTarget }
	_, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute)
	if !errors.Is(err, ErrMoveTarget) || errors.Is(err, ErrMoveStoreDown) {
		t.Fatalf("move whose abort failed: %v, want ErrMoveTarget and not ErrMoveStoreDown", err)
	}
	eng.mu.Lock()
	eng.abortErr = nil
	eng.mu.Unlock()
	waitFor(t, func() bool { return !pausedOn(mc.s, sb.VMName) && !movingOn(mc.s, sb) })
	assertStillOnSource(t, mc, sb)
}

func TestMoveRestartedSourceDropsTheTargetsReservation(t *testing.T) {
	mc := newMoveCluster(t)
	const id, vm, series = "sb_x", "sbx-x", "s1"
	reserveMove(t, mc.t, id, "acme")
	epoch := seedMoveRecord(t, mc.moveReg, id, "a", "b", moves.Pending)
	dataDir := t.TempDir()
	seedClaims(t, dataDir, map[string]*types.Sandbox{id: {
		ID: id, VMName: vm, Key: testKey, Token: "tok", Tenant: "acme", Deadline: time.Now().Add(time.Hour),
		MoveOut: &types.MoveOut{Epoch: epoch, Series: series, Addr: "b", To: "b"},
	}})
	eng := newFakeEngine()
	eng.vms[vm], eng.migrateSeries[vm] = "/vsock/"+vm, series
	restarted := mc.nodeOn(t, eng, "a", dataDir, keepAnHour)
	restarted.liftFence()
	restarted.WithMovePeer(mc.p)
	if err := restarted.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitFor(t, func() bool { return !restarted.moveHeld(id) })
	if mc.t.moveHeld(id) {
		t.Error("the target still holds the reservation of a move the restarted source aborted")
	}
}

func TestMoveTargetCountsAReservationLikeAClaim(t *testing.T) {
	mc := newMoveCluster(t)
	editView(mc.t, func(v *configView) { v.maxClaims = 1 })
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

func TestMoveTargetResolvesATenantItsCacheHasNotSeen(t *testing.T) {
	mc := newMoveCluster(t)
	claimTenant(t, mc.t, "acme")
	setTenantCaps(t, mc.t, map[string]int{"acme": 1})
	mc.t.tenants = &coldTenants{Source: mc.t.tenants}
	if _, err := mc.t.AdmitMove(t.Context(), "sb_cold", MoveAdmission{Tenant: "acme", Key: testKey, From: "a"}); !errors.Is(err, ErrQuota) {
		t.Errorf("admit past a capped tenant the cache has not seen: %v, want ErrQuota", err)
	}
	if _, err := mc.t.AdmitMove(t.Context(), "sb_gone", MoveAdmission{Tenant: "gone", Key: testKey, From: "a"}); !errors.Is(err, ErrTenantRemoved) {
		t.Errorf("admit for a removed tenant: %v, want ErrTenantRemoved", err)
	}
	mc.t.mu.Lock()
	defer mc.t.mu.Unlock()
	if len(mc.t.incoming) != 0 {
		t.Errorf("reservations %v, want none", mc.t.incoming)
	}
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
		t.Fatalf("write state: %v", err)
	}
	var captured bytes.Buffer
	capture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(&captured, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer capture.Close()
	if _, err := peer.SendRound(t.Context(), capture.URL, "", "/", src, []string{"state.json"}); err != nil {
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
	editView(mc.t, func(v *configView) { v.moveConcurrency, v.maxClaims = 1, 1 })
	reserveMove(t, mc.t, "sb_r", "acme")
	first, _ := reservationOf(mc.t, "sb_r")
	stale := filepath.Join(mc.t.moveDir("sb_r"), "state.json")
	if err := os.WriteFile(stale, []byte("a crashed source's round"), 0o600); err != nil {
		t.Fatalf("write stale round: %v", err)
	}
	if _, err := mc.t.AdmitMove(t.Context(), "sb_r", MoveAdmission{Tenant: "acme", Key: testKey, From: "other"}); !errors.Is(err, ErrNotMovable) {
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
		t.Fatalf("write retry round: %v", err)
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
		t.Fatalf("write state: %v", err)
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

func TestMoveTargetDroppingALostCommitKeepsTheRetrysStagingDir(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	reserveMove(t, mc.t, sb.ID, "")
	if err := os.WriteFile(filepath.Join(mc.t.moveDir(sb.ID), "state.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	eng := fakeOf(mc.t)
	stall := make(chan struct{})
	eng.removeStall = stall
	done := make(chan error, 1)
	go func() {
		done <- mc.t.CommitMove(context.WithoutCancel(t.Context()), sb.ID, []byte(`{"id":"`+sb.ID+`","key":{"template":"rt:24.04","net":"none","size":"small"}}`), 1)
	}()
	waitFor(t, func() bool {
		eng.mu.Lock()
		received := len(eng.receives) == 1
		eng.mu.Unlock()
		_, reserved := reservationOf(mc.t, sb.ID)
		_, claimed := mc.t.byID(sb.ID)
		return received && !reserved && !claimed
	})
	reserveMove(t, mc.t, sb.ID, "")
	retried := filepath.Join(mc.t.moveDir(sb.ID), "state.json")
	if err := os.WriteFile(retried, []byte("the retry's round"), 0o600); err != nil {
		t.Fatalf("write retry round: %v", err)
	}
	close(stall)
	if err := <-done; !errors.Is(err, ErrNotMovable) {
		t.Fatalf("a commit the source aborted: %v, want ErrNotMovable", err)
	}
	if _, err := os.Stat(retried); err != nil {
		t.Errorf("dropping the lost commit removed the retry's files: %v", err)
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
		if adopted, err := third.AdoptArchived(t.Context(), sb.ID, Cred{Token: sb.Token}); adopted || err != nil {
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

func TestMoveAbortedAfterDroppingTheRowPutsItBackOnTheNextRound(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.replicate = replicateEvery })
	sb := mustClaim(t, mc.s, testKey)
	replicateNow(t, mc.s, sb)
	mc.s.mu.Lock()
	replica := sb.ReplicaCk
	mc.s.mu.Unlock()
	mc.p.commitHook = func(int) error { return ErrMoveTarget }
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrMoveTarget) {
		t.Fatalf("move: %v, want ErrMoveTarget", err)
	}
	assertStillOnSource(t, mc, sb)
	if ckExists(t, mc.s, replica) {
		t.Error("the aborted move left the replica it dropped")
	}
	if _, ok, _ := mc.reg.Get(t.Context(), sb.ID); ok {
		t.Fatal("the row survived the final round")
	}
	mc.p.commitHook = nil
	replicateNow(t, mc.s, sb)
	if _, ok, _ := mc.reg.Get(t.Context(), sb.ID); !ok {
		t.Error("the next round did not put the row back")
	}
}

func TestMoveNeedsNoArchiveRegistry(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.noArchives = true })
	sb := claimTenant(t, mc.s, "acme")
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move without archive_failover: %v", err)
	}
	if _, held := mc.s.byID(sb.ID); held {
		t.Error("the source kept the claim")
	}
	if moved, ok := mc.t.byID(sb.ID); !ok || moved.MoveIn {
		t.Errorf("target claim %+v %v, want it started", moved, ok)
	}
	if got := mc.counts("acme"); got != [2][3]int{{0, 0, 0}, {1, 1, 1}} {
		t.Errorf("counts %v, want the claim on the target only", got)
	}
}

func TestMoveWhoseRowDeleteReplyIsLostKeepsTheOnlyGuest(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.replicate = replicateEvery })
	sb := mustClaim(t, mc.s, testKey)
	replicateNow(t, mc.s, sb)
	mc.s.archives = &hookRegistry{Registry: mc.reg, loseDeleteReply: true}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, archives.ErrUnavailable) {
		t.Fatalf("move: %v, want the lost reply's error", err)
	}
	assertStillOnSource(t, mc, sb)
	if _, ok, _ := mc.reg.Get(t.Context(), sb.ID); ok {
		t.Fatal("the delete did not land under its lost reply")
	}
	replicateNow(t, mc.s, sb)
	if _, held := mc.s.byID(sb.ID); !held {
		t.Fatal("the next replica round dropped the claim as adopted")
	}
	if rec, ok, _ := mc.reg.Get(t.Context(), sb.ID); !ok || rec.Owner != "a" {
		t.Errorf("row %+v %v after the next round, want this node's", rec, ok)
	}
}

func TestMoveWhoseRowConsumeReplyIsLostKeepsTheGuestAndItsReplica(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.replicate = replicateEvery })
	sb := mustClaim(t, mc.s, testKey)
	replicateNow(t, mc.s, sb)
	mc.s.mu.Lock()
	replica := sb.ReplicaCk
	mc.s.mu.Unlock()
	mc.s.archives = &hookRegistry{Registry: mc.reg, loseConsumeReply: true}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, archives.ErrUnavailable) {
		t.Fatalf("move: %v, want the lost reply's error", err)
	}
	assertStillOnSource(t, mc, sb)
	if rec, ok, _ := mc.reg.Get(t.Context(), sb.ID); !ok || !rec.Consumed || rec.Owner != "a" {
		t.Fatalf("row %+v %v, want this node's tombstone", rec, ok)
	}
	if !ckExists(t, mc.s, replica) {
		t.Fatal("the failed move deleted the replica the claim still names")
	}
	mc.s.sweepOrphanArchives(t.Context())
	if rec, ok, _ := mc.reg.Get(t.Context(), sb.ID); !ok || rec.Consumed || rec.Checkpoint != replica {
		t.Errorf("row %+v %v after the sweep, want the replica back on it", rec, ok)
	}
	replicateNow(t, mc.s, sb)
	if _, held := mc.s.byID(sb.ID); !held {
		t.Error("the next replica round dropped the claim")
	}
}

func TestMoveCutOffFromTheRegistryLeavesAnAdoptionToTheFence(t *testing.T) {
	mc := newMoveClusterWith(t, func(c *failoverCluster) { c.replicate = replicateEvery })
	u := mc.moveNode(t, "c")
	sb := mustClaim(t, mc.s, testKey)
	replicateNow(t, mc.s, sb)
	mc.s.archives = &unreachableRowWrites{Registry: mc.reg}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, archives.ErrUnavailable) {
		t.Fatalf("move: %v, want the registry's error", err)
	}
	mc.s.archives = mc.reg
	mc.expire("a")
	mustAdopt(t, u, sb, "adoption while the source was cut off")
	mc.renew(t, "a")
	mc.s.trip(t.Context())
	mc.s.unfence(t.Context(), time.Now())
	waitFor(t, func() bool { return fakeOf(mc.s).removed(sb.VMName) })
	if _, held := mc.s.byID(sb.ID); held {
		t.Error("the source kept serving a claim a peer adopted")
	}
}

func TestMoveRestartAfterTheConsumeKeepsTheClaim(t *testing.T) {
	for name, fieldsCleared := range map[string]bool{"before its fields are cleared": false, "after its fields are cleared": true} {
		t.Run(name, func(t *testing.T) {
			c := newFailoverCluster(t)
			reg := movestest.NewMem()
			c.moves = reg
			ctx := t.Context()
			epoch := seedMoveRecord(t, reg, "sb_moved", "a", "b", moves.Pending)
			rowEpoch, err := c.reg.Put(ctx, archives.Record{ID: "sb_moved", TokenSHA256: tenants.TokenSHA256("tok"), Owner: "a", Checkpoint: "ck_00000000000000aa", Claim: []byte("{}")})
			if err != nil {
				t.Fatalf("seed row: %v", err)
			}
			if _, err = c.reg.Consume(ctx, "sb_moved", rowEpoch); err != nil {
				t.Fatalf("seed tombstone: %v", err)
			}
			sb := &types.Sandbox{ID: "sb_moved", VMName: "sbx-moved", Key: testKey, Token: "tok", Deadline: time.Now().Add(time.Hour), MoveOut: &types.MoveOut{Epoch: epoch, Series: "s1"}}
			if !fieldsCleared {
				sb.ReplicaCk, sb.ArchiveEpoch = "ck_00000000000000aa", rowEpoch
			}
			dataDir := t.TempDir()
			seedClaims(t, dataDir, map[string]*types.Sandbox{sb.ID: sb})
			eng := newFakeEngine()
			eng.vms["sbx-moved"] = "/vsock/sbx-moved"
			m := c.nodeOn(t, eng, "a", dataDir, keepAnHour)
			m.liftFence()
			if err = m.Reconcile(ctx); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			waitFor(t, func() bool { return !m.moveHeld(sb.ID) })
			if _, held := m.byID(sb.ID); !held {
				t.Fatal("the restart dropped a claim whose move had not committed")
			}
			eng.mu.Lock()
			defer eng.mu.Unlock()
			if !slices.Contains(eng.aborts, "sbx-moved:s1") || slices.Contains(eng.removes, "sbx-moved") {
				t.Errorf("aborts %v removes %v, want the series aborted and the VM kept", eng.aborts, eng.removes)
			}
		})
	}
}

func TestRestartSweepsTheStagingOfAMovedInClaimItDrops(t *testing.T) {
	dataDir := t.TempDir()
	sb := &types.Sandbox{ID: "sb_in", VMName: "sbx-in", Key: testKey, Token: "tok", Deadline: time.Now().Add(time.Hour), MoveIn: true}
	seedClaims(t, dataDir, map[string]*types.Sandbox{sb.ID: sb})
	staging := filepath.Join(dataDir, movesDir, sb.ID)
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, newFakeEngine(), dataDir)
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging of a dropped moved-in claim: %v, want it removed", err)
	}
}

func TestMoveHoldsOffCapturesReleasesAndEvacuationWhileItRuns(t *testing.T) {
	for name, archived := range map[string]bool{"archives": true, "no archives": false} {
		t.Run(name, func(t *testing.T) { testMoveHoldsOffCallers(t, archived) })
	}
}

func TestMoveToANodeThatStartedDrainingAborts(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	mc.p.roundHook = func(round int) error {
		if round == 2 {
			mc.t.Drain(t.Context())
		}
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Fatalf("move to a draining node: %v, want ErrNotMovable", err)
	}
	if _, held := mc.t.byID(sb.ID); held {
		t.Error("the draining node took the claim")
	}
	if _, held := mc.s.byID(sb.ID); !held {
		t.Error("the source dropped the claim the target refused")
	}
}

func TestMoveGivesEachRoundADeadlineFromItsSize(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	fakeOf(mc.s).migrateSizes = []int64{1 << 30, 1, 1 << 40}
	mc.p.wireSizes = []int64{1 << 20, 0, 1 << 20}
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
	if movingOn(mc.s, sb) {
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

func TestMoveHoldsOffVolumeChanges(t *testing.T) {
	addData := types.VolumePatch{Add: []types.Volume{{Name: "data", AttachOnly: true}}}
	t.Run("during the rounds", func(t *testing.T) {
		mc := newMoveClusterWith(t, func(c *failoverCluster) { c.volumes = liveCatalog(t) })
		sb := mustClaim(t, mc.s, testKey)
		mc.p.roundHook = func(round int) error {
			if round == 1 {
				if _, err := mc.s.PatchVolumes(t.Context(), sb.ID, "", addData); !errors.Is(err, ErrEvacuating) {
					t.Errorf("volume change during the rounds: %v, want ErrEvacuating", err)
				}
			}
			return nil
		}
		if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move: %v", err)
		}
		if ops := fakeOf(mc.s).volumeOpsLog(); slices.ContainsFunc(ops, func(op string) bool { return strings.HasPrefix(op, "attach") }) {
			t.Errorf("volume ops %v during a move", ops)
		}
	})
	t.Run("on a received claim", func(t *testing.T) {
		mc := newMoveClusterWith(t, func(c *failoverCluster) { c.volumes = liveCatalog(t) })
		sb := mustClaim(t, mc.s, testKey)
		lossy := &lossyMoves{Registry: mc.moveReg, downAfterCommit: true}
		mc.t.moves = lossy
		if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
			t.Fatalf("move: %v", err)
		}
		if _, err := mc.t.PatchVolumes(t.Context(), sb.ID, "", addData); !errors.Is(err, ErrEvacuating) {
			t.Errorf("volume change on a received claim: %v, want ErrEvacuating", err)
		}
		lossy.setDown(false)
		waitFor(t, func() bool { return !mc.t.moveHeld(sb.ID) })
	})
}

func TestMoveFinalRoundAnswersUserCallsAtOnce(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	cred := Cred{Token: sb.Token}
	ctx := t.Context()
	mc.p.commitHook = func(int) error {
		assertEvacuating(t, evacuatingCalls(ctx, mc.s, sb.ID, cred), "during the final round")
		return nil
	}
	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}
}

func TestMoveEstimatesThePauseFromTheBytesOnTheWire(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	eng := fakeOf(mc.s)
	eng.migrateSizes = []int64{10 << 30, 520 << 20, 500 << 20, 480 << 20, 460 << 20}
	mc.p.wireSizes = []int64{1400 << 20, 520 << 20, 500 << 20, 480 << 20, 460 << 20}
	mc.p.roundHook = func(round int) error {
		if round == 1 {
			time.Sleep(250 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", MoveStartAllowance+50*time.Millisecond); !errors.Is(err, ErrMoveDowntime) {
		t.Fatalf("move of a hot guest: %v, want ErrMoveDowntime", err)
	}
	eng.mu.Lock()
	wentFinal := slices.ContainsFunc(eng.captures, func(c string) bool { return strings.HasSuffix(c, ":true") })
	eng.mu.Unlock()
	if wentFinal {
		t.Error("the move paused the guest for a final round that cannot fit")
	}
	assertStillOnSource(t, mc, sb)
}

func TestMoveWhosePausedRoundOverrunsTheBudgetResumesTheGuestOnTheSource(t *testing.T) {
	for name, tt := range map[string]struct {
		captureTime, sendTime time.Duration
		sizes, wire           []int64
		sentFinal             bool
	}{
		"final capture":     {captureTime: 200 * time.Millisecond},
		"final round grown": {sizes: []int64{1 << 20, 1 << 10, 1 << 40}, wire: []int64{1 << 20, 1 << 10}},
		"final transfer":    {sendTime: 200 * time.Millisecond, sentFinal: true},
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			sb := mustClaim(t, mc.s, testKey)
			fakeOf(mc.s).finalCaptureTime, fakeOf(mc.s).migrateSizes, mc.p.wireSizes = tt.captureTime, tt.sizes, tt.wire
			sentFinal, committed := false, false
			mc.p.roundHook = func(round int) error {
				if round == 3 {
					sentFinal = true
					time.Sleep(tt.sendTime)
				}
				return nil
			}
			mc.p.commitHook = func(int) error { committed = true; return nil }
			if _, err := mc.s.Move(t.Context(), sb.ID, "b", MoveStartAllowance+100*time.Millisecond); !errors.Is(err, ErrMoveDowntime) {
				t.Fatalf("move: %v, want ErrMoveDowntime", err)
			}
			eng := fakeOf(mc.s)
			eng.mu.Lock()
			wentFinal := slices.ContainsFunc(eng.captures, func(c string) bool { return strings.HasSuffix(c, ":true") })
			eng.mu.Unlock()
			if !wentFinal || sentFinal != tt.sentFinal || committed {
				t.Errorf("final capture %v sent %v commit %v, want the capture, a final round sent %v and no commit", wentFinal, sentFinal, committed, tt.sentFinal)
			}
			assertStillOnSource(t, mc, sb)
			assertNoMoveResidue(t, mc.t, sb.ID)
			if _, ok, _ := mc.moveReg.Get(t.Context(), sb.ID); ok {
				t.Error("the aborted move left its record")
			}
		})
	}
}

func TestMoveGivesUpOnDeltasThatStopShrinking(t *testing.T) {
	for name, tt := range map[string]struct {
		sizes    []int64
		captures int
	}{
		"second delta as large as the first":   {sizes: []int64{1000, 500, 500}, captures: 3},
		"second delta larger than the first":   {sizes: []int64{1000, 500, 600}, captures: 3},
		"second delta shrinks by 1%":           {sizes: []int64{1000, 519, 513}, captures: 3},
		"second delta shrinks by exactly 10%":  {sizes: []int64{1000, 500, 450}, captures: 3},
		"third delta shrinks by under 10%":     {sizes: []int64{1000, 800, 600, 550}, captures: 4},
		"each delta shrinks by over 10%":       {sizes: []int64{1000, 500, 449, 400, 350}, captures: 5},
		"shrinking but never fitting":          {sizes: []int64{1000, 900, 800, 700, 600}, captures: 5},
		"first delta larger than the baseline": {sizes: []int64{100, 200, 150, 120, 110}, captures: 5},
	} {
		t.Run(name, func(t *testing.T) {
			mc := newMoveCluster(t)
			sb := mustClaim(t, mc.s, testKey)
			eng := fakeOf(mc.s)
			mc.p.wireSizes = tt.sizes
			if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Millisecond); !errors.Is(err, ErrMoveDowntime) {
				t.Fatalf("move: %v, want ErrMoveDowntime", err)
			}
			eng.mu.Lock()
			captures := len(eng.captures)
			eng.mu.Unlock()
			if captures != tt.captures {
				t.Errorf("%d captures, want %d", captures, tt.captures)
			}
			assertStillOnSource(t, mc, sb)
		})
	}
}

func TestMoveLeavesAnExitedVMMToLivenessUntilItEnds(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	vm, eng := sb.VMName, fakeOf(mc.s)
	mc.p.roundHook = func(round int) error {
		if round != 1 {
			return nil
		}
		killVMM(eng, vm)
		if watched := mc.s.watchedClaims(""); slices.ContainsFunc(watched, func(s liveSuspect) bool { return s.sb == sb }) {
			t.Error("liveness watches the moving claim")
		}
		mc.s.recoverVMM(t.Context(), sb)
		if got := startsOf(mc.s); len(got) != 0 {
			t.Errorf("liveness started %v during the move", got)
		}
		return nil
	}
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Fatalf("a move whose VMM exited: %v, want ErrNotMovable", err)
	}
	if _, held := mc.t.byID(sb.ID); held {
		t.Error("the target holds a claim whose VMM exited mid-move")
	}
	assertNoMoveResidue(t, mc.t, sb.ID)
	if got := startsOf(mc.s); len(got) != 0 {
		t.Fatalf("liveness started %v before the move ended", got)
	}
	mc.s.recoverVMM(t.Context(), sb)
	if got := startsOf(mc.s); !slices.Contains(got, vm) {
		t.Errorf("starts %v after the move ended, want liveness to restart the VM", got)
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
	fakeOf(mc.s).migrateSupportErr = nil
	fakeOf(mc.t).migrateSupportErr = errors.New("cocoon too old")
	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); !errors.Is(err, ErrNotMovable) {
		t.Errorf("move to a cocoon without migration: %v, want ErrNotMovable", err)
	}
	if len(mc.p.roundBudgets) != 0 {
		t.Errorf("a move the target refused sent %d rounds", len(mc.p.roundBudgets))
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
	coldRestarted := func(t *testing.T, eng *fakeEngine) {
		if len(eng.starts) != 1 || slices.Contains(eng.removes, vm) {
			t.Errorf("starts %v removes %v, want one cold boot by liveness", eng.starts, eng.removes)
		}
	}
	unstarted := func(t *testing.T, eng *fakeEngine) {
		if slices.Contains(eng.starts, vm) || !slices.Contains(eng.removes, vm) {
			t.Errorf("starts %v removes %v, want the received VM removed unstarted", eng.starts, eng.removes)
		}
	}
	for name, tt := range map[string]struct {
		node         string
		from, to     string
		reach        moves.State
		wantClaim    bool
		wantRecord   moves.State
		vmmLost      bool
		inspectFails int
		check        func(t *testing.T, eng *fakeEngine)
	}{
		"source at pending":                                  {node: "a", from: "a", to: "b", reach: moves.Pending, wantClaim: true, check: aborted},
		"source at final":                                    {node: "a", from: "a", to: "b", reach: moves.Final, wantClaim: true, check: aborted},
		"source at aborted":                                  {node: "a", from: "a", to: "b", reach: moves.Aborted, wantClaim: true, check: aborted},
		"source at committed":                                {node: "a", from: "a", to: "b", reach: moves.Committed, wantRecord: moves.Committed, check: movedOn},
		"source at started":                                  {node: "a", from: "a", to: "b", reach: moves.Started, check: movedOn},
		"source, record missing":                             {node: "a", check: movedOn},
		"source, another node moved":                         {node: "a", from: "b", to: "c", reach: moves.Pending, wantRecord: moves.Pending, check: movedOn},
		"target at final":                                    {node: "b", from: "a", to: "b", reach: moves.Final, wantRecord: moves.Aborted, check: unstarted},
		"target at committed":                                {node: "b", from: "a", to: "b", reach: moves.Committed, wantClaim: true, wantRecord: moves.Started, check: started},
		"target at started":                                  {node: "b", from: "a", to: "b", reach: moves.Started, wantClaim: true, wantRecord: moves.Started, check: started},
		"target at committed, VMM lost with the host":        {node: "b", from: "a", to: "b", reach: moves.Committed, wantClaim: true, wantRecord: moves.Started, vmmLost: true, check: coldRestarted},
		"target at committed, VMM lost, first inspect fails": {node: "b", from: "a", to: "b", reach: moves.Committed, wantClaim: true, wantRecord: moves.Started, vmmLost: true, inspectFails: 1, check: coldRestarted},
		"target at aborted":                                  {node: "b", from: "a", to: "b", reach: moves.Aborted, wantRecord: moves.Aborted, check: unstarted},
		"target, record missing":                             {node: "b", check: unstarted},
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
				sb.MoveOut = &types.MoveOut{Epoch: journalEpoch, Series: series}
			} else {
				sb.MoveIn = true
			}
			dataDir := t.TempDir()
			seedClaims(t, dataDir, map[string]*types.Sandbox{sb.ID: sb})
			if err := os.MkdirAll(filepath.Join(dataDir, movesDir, sb.ID), 0o700); err != nil {
				t.Fatalf("stage move dir: %v", err)
			}
			eng := newFakeEngine()
			eng.vms[vm], eng.paused[vm], eng.stopped[vm] = "/vsock/"+vm, sb.MoveIn, tt.vmmLost
			eng.migrateSeries[vm], eng.inspectFails = series, tt.inspectFails
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
			if ok && (held.MoveOut != nil || held.MoveIn) {
				t.Errorf("claim journal out %+v in %+v, want both cleared", held.MoveOut, held.MoveIn)
			}
			if tt.vmmLost {
				m.livenessOnce(ctx)
				waitFor(t, func() bool {
					m.mu.Lock()
					defer m.mu.Unlock()
					return held.Restarts == 1 && !m.sweeps.liveness.Load()
				})
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
	seedMoveRecord(t, reg, "sb_held", "a", "b", moves.Committed)
	sb := &types.Sandbox{ID: "sb_held", VMName: "sbx-held", Key: testKey, Deadline: time.Now().Add(time.Hour), MoveIn: true}
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
	waitSettled(t, received)
	cred := Cred{Token: sb.Token}
	backdate(mc.t, received, time.Hour)
	received.Transition.Lock()
	sweepIdle(t, mc.t)
	received.Transition.Unlock()
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
	calls := evacuatingCalls(ctx, mc.t, sb.ID, cred)
	_, calls["renew"] = mc.t.Renew(ctx, sb.ID, cred, time.Hour, "")
	_, calls["dial"] = mc.t.DialPort(ctx, sb.ID, cred, 80)
	calls["env"] = mc.t.PatchEnv(ctx, sb.ID, types.EnvPatch{"GH_TOKEN": {Value: "x", Guest: hostOnly}}, "")
	calls["wake"] = mc.t.Wake(ctx, sb.ID, cred)
	assertEvacuating(t, calls, "of a received claim")
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
		"held under a journal":     {reach: moves.Committed, edit: func(sb *types.Sandbox) { sb.MoveOut = &types.MoveOut{Epoch: 1} }},
		"received and not started": {reach: moves.Committed, edit: func(sb *types.Sandbox) { sb.MoveIn = true }},
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
			sb.MoveOut, sb.MoveIn = nil, false
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

func TestMoveTargetStoppingBeforeItsStartKeepsTheGuestForItsRestart(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	ctx := t.Context()
	eng := fakeOf(mc.t)
	eng.mu.Lock()
	eng.startErr = engine.ErrStopping
	eng.mu.Unlock()

	if _, err := mc.s.Move(ctx, sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move whose target began to stop: %v, want the committed move", err)
	}
	held, ok := mc.t.byID(sb.ID)
	if !ok || !held.MoveIn || held.Failed != "" {
		t.Fatalf("target claim %+v %v, want it journaled and not failed", held, ok)
	}

	eng.mu.Lock()
	eng.startErr = nil
	eng.mu.Unlock()
	restarted := mc.restart(t, mc.t)
	restarted.liftFence()
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	waitFor(t, func() bool {
		rec, found, _ := mc.moveReg.Get(ctx, sb.ID)
		return !restarted.moveHeld(sb.ID) && found && rec.State == moves.Started
	})
	moved, ok := restarted.byID(sb.ID)
	if !ok || moved.Failed != "" || moved.MoveIn {
		t.Fatalf("restarted target claim %+v %v, want the guest started", moved, ok)
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
		return !held && !movingOn(mc.s, sb)
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
			editView(m, func(v *configView) { v.maxClaims, v.maxRunning = 1000, 1000 })
			for b.Loop() {
				m.mu.Lock()
				_ = m.quotaErr(1, "")
				m.mu.Unlock()
			}
		})
	}
}

func testMoveHoldsOffCallers(t *testing.T, archived bool) {
	t.Helper()
	mc := newMoveCluster(t)
	if !archived {
		mc.s.archives, mc.t.archives = nil, nil
	}
	sb := mustClaim(t, mc.s, testKey)
	other := mustClaim(t, mc.s, testKey)
	cred := Cred{Token: sb.Token}
	mc.p.roundHook = func(round int) error {
		if round != 2 {
			return nil
		}
		assertEvacuating(t, evacuatingCalls(t.Context(), mc.s, sb.ID, cred), "during a move")
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
		editView(mc.s, func(v *configView) { v.moveConcurrency = 1 })
		if _, err := mc.s.Move(t.Context(), other.ID, "b", time.Minute); !errors.Is(err, ErrQuota) {
			t.Errorf("a second move past a lowered move_concurrency: %v, want ErrQuota", err)
		}
		wantEvac := errArchiveBusy
		if !archived {
			wantEvac = errNoFailover
		}
		mc.s.mu.Lock()
		mc.s.evac = &evacuation{pending: map[string]*types.Sandbox{sb.ID: sb}, stop: func() {}}
		mc.s.mu.Unlock()
		evacErr := mc.s.evacuateClaim(t.Context(), sb)
		mc.s.mu.Lock()
		mc.s.evac = nil
		mc.s.mu.Unlock()
		if !errors.Is(evacErr, wantEvac) {
			t.Errorf("evacuation of the moving claim: %v, want %v", evacErr, wantEvac)
		}
		if _, held := mc.s.byID(sb.ID); !held || sb.ArchiveCk != "" {
			t.Error("the evacuation took the moving claim")
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
	c.egressPol = hostPolicy()
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

func (mc *moveCluster) counts(tenant string) [2][3]int {
	return [2][3]int{moveCounts(mc.s, tenant), moveCounts(mc.t, tenant)}
}

type moveHook func(round int) error

type fakeMovePeer struct {
	c   *failoverCluster
	srv *httptest.Server

	admitErr        error
	roundHook       moveHook
	commitHook      moveHook
	loseCommitReply bool
	dropHook        func()
	roundBudgets    []time.Duration
	wireSizes       []int64
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

func (p *fakeMovePeer) Admit(ctx context.Context, addr, id string, a MoveAdmission) (string, error) {
	if p.admitErr != nil {
		return "", p.admitErr
	}
	return p.target(addr).AdmitMove(ctx, id, a)
}

func (p *fakeMovePeer) SendRound(ctx context.Context, addr, id string, round int, dir string, files []string) (int64, error) {
	if deadline, ok := ctx.Deadline(); ok {
		p.roundBudgets = append(p.roundBudgets, time.Until(deadline))
	}
	if hook := p.roundHook; hook != nil {
		if err := hook(round); err != nil {
			return 0, err
		}
	}
	wire, err := peer.SendRound(ctx, p.srv.URL, "", "/"+addr+"/"+id, dir, files)
	if err != nil {
		return wire, errors.Join(ErrMoveTarget, err)
	}
	if len(p.wireSizes) > 0 {
		wire, p.wireSizes = p.wireSizes[0], p.wireSizes[1:]
	}
	return wire, nil
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
	i := slices.IndexFunc(p.c.nodes, func(m *Manager) bool { return m.nodeID == addr })
	return p.c.nodes[i]
}

type coldTenants struct {
	tenants.Source
	warm atomic.Bool
}

func (c *coldTenants) Peek(name string) (tenants.TenantRecord, tenants.Presence) {
	if !c.warm.Load() {
		return tenants.TenantRecord{}, tenants.Unknown
	}
	return c.Source.Peek(name)
}

func (c *coldTenants) Lookup(ctx context.Context, name string) (tenants.TenantRecord, bool, error) {
	c.warm.Store(true)
	return c.Source.Lookup(ctx, name)
}

type unreachableRowWrites struct {
	archives.Registry
}

func (r *unreachableRowWrites) Consume(context.Context, string, int64) (int64, error) {
	return 0, archives.ErrUnavailable
}

func (r *unreachableRowWrites) Delete(context.Context, string, int64) error {
	return archives.ErrUnavailable
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

type countingMoves struct {
	moves.Registry
	failedGets atomic.Int32
}

func (r *countingMoves) Get(ctx context.Context, id string) (moves.Record, bool, error) {
	rec, ok, err := r.Registry.Get(ctx, id)
	if err != nil {
		r.failedGets.Add(1)
	}
	return rec, ok, err
}

func seedMoveRecord(t *testing.T, reg moves.Registry, id, from, to string, state moves.State) int64 {
	t.Helper()
	epoch, err := reg.Begin(t.Context(), id, from, to)
	if err != nil {
		t.Fatalf("begin %s: %v", id, err)
	}
	movestest.Reach(t, reg, id, epoch, state)
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

func evacuatingCalls(ctx context.Context, m *Manager, id string, cred Cred) map[string]error {
	calls := map[string]error{}
	_, calls["fork"] = m.Fork(ctx, id, cred, 1, time.Minute, "", "")
	_, calls["checkpoint"] = m.Checkpoint(ctx, id, cred, "ck", "")
	_, _, calls["promote"] = m.Promote(ctx, id, cred, "tpl", "")
	calls["hibernate"] = m.Hibernate(ctx, id, cred)
	calls["release"] = m.Release(ctx, id, cred)
	return calls
}

func assertEvacuating(t *testing.T, calls map[string]error, when string) {
	t.Helper()
	for op, err := range calls {
		if !errors.Is(err, ErrEvacuating) {
			t.Errorf("%s %s: %v, want ErrEvacuating", op, when, err)
		}
	}
}

func moveCounts(m *Manager, tenant string) [3]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return [3]int{len(m.claimed) + len(m.incoming), m.store.runningCount() + len(m.incoming), m.tenantLive[tenant] + m.incomingOf(tenant)}
}

func reserveMove(t *testing.T, m *Manager, id, tenant string) {
	t.Helper()
	if _, err := m.AdmitMove(t.Context(), id, MoveAdmission{Tenant: tenant, Key: testKey, From: "zz"}); err != nil {
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

func movingOn(m *Manager, sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.midMoveLocked(sb)
}

func pausedOn(m *Manager, vm string) bool {
	eng := fakeOf(m)
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.paused[vm]
}

func failRound(n int) moveHook {
	return func(round int) error {
		if round == n {
			return ErrMoveTarget
		}
		return nil
	}
}

func abortRecord(t *testing.T, mc *moveCluster) {
	t.Helper()
	recs, err := mc.moveReg.Stale(t.Context(), time.Now().Add(time.Hour))
	if err != nil {
		t.Errorf("stale move records: %v", err)
	}
	for _, rec := range recs {
		_, _ = mc.moveReg.Advance(t.Context(), rec.ID, rec.Epoch, rec.State, moves.Aborted)
	}
}
