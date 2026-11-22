package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	coldBootID  = "6b0e4f2a-1c3d-4e5f-8a9b-0c1d2e3f4a5b"
	coldBootID2 = "7c1f5a3b-2d4e-4f6a-9b0c-1d2e3f4a5b6c"
)

func TestAGuestRebootRewritesTheEnvRemountsVolumesAndMovesRestartsOnce(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{
		Env:     types.Env{"MODE": {Value: "prod"}},
		Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}, {Name: "other", AttachOnly: true}},
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.mu.Lock()
	delete(eng.guestEnvs, sb.VsockSocket)
	mountsBefore := len(eng.volumeMounts)
	eng.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
	m.Rebooted(t.Context(), sb, "boot-2")

	if rebootQueued(m, sb) {
		t.Error("a second notice of the same boot queued another settle")
	}
	eng.mu.Lock()
	env, live, mounts := eng.guestEnvs[sb.VsockSocket], slices.Clone(eng.liveMounts), len(eng.volumeMounts)-mountsBefore
	eng.mu.Unlock()
	if env != "MODE=\"prod\"\n" {
		t.Errorf("guest env %q, want the claim env written again", env)
	}
	if !slices.Equal(live, []string{"data:/volumes/data"}) || mounts != 1 {
		t.Errorf("live mounts %v and %d mounts, want the mounted volume mounted again through the idempotent path and the attach-only one left alone", live, mounts)
	}
	if row := journaled(t, m)[sb.ID]; row.RestartedAt.IsZero() {
		t.Errorf("journal %+v, want the restart stamped", row)
	}
	if got := usageEventsOf(t, m, sb.ID); got != "claim" {
		t.Errorf("events %q, want no usage event for a reboot inside the VMM", got)
	}
}

func TestARemountAReleaseCatchesBeforeItsGuestCallIsCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		entered, hold := make(chan struct{}), make(chan struct{})
		eng.liveMountHook = func() {
			close(entered)
			<-hold
		}

		m.Rebooted(t.Context(), sb, "boot-2")
		<-entered
		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("release: %v", err)
		}
		close(hold)
		synctest.Wait()

		if got := liveMountsOf(eng); len(got) != 0 {
			t.Errorf("live mounts %v, want the release to cancel the remount it caught before its guest call", got)
		}
	})
}

func TestARemountAfterAReleaseDuringItsProbeIsSkipped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.liveMountIgnoresCtx = true
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		stall := make(chan struct{})
		eng.mu.Lock()
		eng.probeStall = stall
		eng.mu.Unlock()

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("release: %v", err)
		}
		close(stall)
		synctest.Wait()

		if got := liveMountsOf(eng); len(got) != 0 {
			t.Errorf("live mounts %v, want the run check to skip a remount the release overtook", got)
		}
	})
}

func TestANoticeWhileAWakeIsInFlightSettlesWhenTheWakeEnds(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	sb.Transition.Lock()
	m.mu.Lock()
	sb.Waking = true
	m.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	queued, inWake := rebootQueued(m, sb), restartsOf(m, sb.ID)
	m.mu.Lock()
	sb.Waking = false
	m.mu.Unlock()
	sb.Transition.Unlock()

	if !queued || inWake != 0 {
		t.Errorf("queued %v, restarts %d while the wake held the claim, want a settle queued behind it", queued, inWake)
	}
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
}

func TestARebootSettleTriesEveryVolumeWhenOneFails(t *testing.T) {
	eng := newFakeEngine()
	eng.liveMountErrFor = "data"
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}, {Name: "other"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")

	eng.mu.Lock()
	calls := slices.Clone(eng.liveMountCalls)
	eng.mu.Unlock()
	if !slices.Equal(calls, []string{"data", "other"}) {
		t.Errorf("remount dispatches %v, want both volumes tried", calls)
	}
	if got := liveMountsOf(eng); !slices.Equal(got, []string{"other:/volumes/other"}) {
		t.Errorf("live mounts %v, want the second volume mounted after the first failed", got)
	}
	want := map[string]types.VolumeState{
		"data":  {Name: "data", State: types.VolumePending, Error: volumeRemountFailed},
		"other": {Name: "other", State: types.VolumeAttached},
	}
	for name, st := range volumeStates(t, m, sb.ID) {
		if st != want[name] {
			t.Errorf("volume %s state %+v, want %+v", name, st, want[name])
		}
		delete(want, name)
	}
	if len(want) != 0 {
		t.Errorf("no state for %v", want)
	}
}

func TestAVolumeWhoseRemountFailedMountsAgainOnAPutOfTheSameSet(t *testing.T) {
	eng := newFakeEngine()
	eng.liveMountErrFor = "data"
	m := newVolumeManager(t, eng, liveCatalog(t))
	set := []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: set})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
	if row := journaled(t, m)[sb.ID]; len(row.Volumes) != 0 || len(row.VolumesWant) != 1 {
		t.Fatalf("journal volumes %v want %v, want the failed volume wanted but not applied", row.Volumes, row.VolumesWant)
	}
	eng.mu.Lock()
	eng.liveMountErrFor = ""
	eng.mu.Unlock()

	if _, err := m.SetVolumes(t.Context(), sb.ID, "", set); err != nil {
		t.Fatalf("put the same set: %v", err)
	}

	if st := volumeStates(t, m, sb.ID)["data"]; st.State != types.VolumeAttached || st.Error != "" {
		t.Errorf("state %+v, want the volume attached with its error cleared", st)
	}
	if got := liveMountsOf(eng); !slices.Equal(got, []string{"data:/volumes/data"}) {
		t.Errorf("live mounts %v, want the put to mount the volume again", got)
	}
}

func TestARebootSettleMountsTheVolumesWhenTheEnvWriteFails(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{
		Env:     types.Env{"MODE": {Value: "prod"}},
		Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}},
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.mu.Lock()
	eng.guestEnvErr = errors.New("guest write refused")
	eng.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")

	if got := liveMountsOf(eng); !slices.Equal(got, []string{"data:/volumes/data"}) {
		t.Errorf("live mounts %v, want the volume mounted after the env write failed", got)
	}
}

func TestAClaimReleasedBeforeItsArmGetsNoDoors(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = outboundtest.SockRoot(t)
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Egress: hostPolicy()})
	var released atomic.Bool
	var armedID string
	withOutbound(t, m, func(o *outbound.Options) {
		armed := o.Armed
		o.Armed = func(ctx context.Context, sb *types.Sandbox) {
			armed(ctx, sb)
			if released.CompareAndSwap(false, true) {
				armedID = sb.ID
				if err := m.Release(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
					t.Errorf("release: %v", err)
				}
			}
		}
	})

	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if armedID != sb.ID {
		t.Fatalf("released %q, want the claim %s released before its doors opened", armedID, sb.ID)
	}
	for _, path := range []string{engine.BootSocketPath(sb.VsockSocket), engine.EgressSocketPath(sb.VsockSocket)} {
		if conn, err := net.Dial("unix", path); err == nil {
			_ = conn.Close()
			t.Errorf("%s accepts for a released claim", path)
		}
	}
	if got := m.out.Route(sb); got != types.NetRouteNone {
		t.Errorf("route %q, want no doors for a released claim", got)
	}
}

func TestANoticeForAPausedClaimIsKeptPendingAndForAReleasedOneDropped(t *testing.T) {
	for _, tt := range []struct {
		name    string
		pending bool
		park    func(t *testing.T, m *Manager, sb *types.Sandbox)
	}{
		{"released", false, func(t *testing.T, m *Manager, sb *types.Sandbox) {
			if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatalf("release: %v", err)
			}
		}},
		{"hibernated", true, func(t *testing.T, m *Manager, sb *types.Sandbox) { hibernateClaim(t, m, sb) }},
		{"archived", true, func(t *testing.T, m *Manager, sb *types.Sandbox) { mustArchive(t, m, sb) }},
		{"failed", true, func(t *testing.T, m *Manager, sb *types.Sandbox) {
			editView(m, func(v *configView) { v.vmmRestart = types.VMMRestartNone })
			killVMM(fakeOf(m), sb.VMName)
			runLiveness(t, m)
			if got, _ := m.Sandbox(sb.ID); got.Failed == "" {
				t.Fatalf("summary %+v, want the claim failed", got)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t, newFakeEngine(), archivePool(keepAnHour))
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			tt.park(t, m, sb)

			m.Rebooted(t.Context(), sb, "boot-2")

			if rebootQueued(m, sb) {
				t.Error("a notice for a claim that does not run queued a settle")
			}
			row := journaled(t, m)[sb.ID]
			if tt.pending && (row == nil || !row.RebootPending || row.BootID != "boot-2") {
				t.Errorf("journal %+v, want the notice journaled pending", row)
			}
			if !tt.pending && row != nil {
				t.Errorf("journal %+v, want the released claim gone", row)
			}
		})
	}
}

func TestANoticeTakenDuringASettleStaysPendingForTheNextOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		eng.liveMountHook = sync.OnceFunc(func() { m.Rebooted(t.Context(), sb, "boot-3") })

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		if row := journaled(t, m)[sb.ID]; row.Restarts != 1 || !row.RebootPending {
			t.Fatalf("journal %+v, want the first settle committed and the notice it overlapped still pending", row)
		}
		time.Sleep(rebootSettleGap)
		synctest.Wait()

		if row := journaled(t, m)[sb.ID]; row.Restarts != 2 || row.RebootPending || row.BootID != "boot-3" {
			t.Errorf("journal %+v, want the overlapped notice settled at the gap end", row)
		}
	})
}

func TestANoticeWhoseJournalWriteFailsIsStillTaken(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	stall := make(chan struct{})
	eng.mu.Lock()
	eng.probeStall = stall
	eng.mu.Unlock()
	breakStore(t, m)

	m.Rebooted(t.Context(), sb, "boot-2")

	if !rebootPending(m, sb) {
		t.Error("a notice whose journal write failed was not kept")
	}
	healStore(t, m)
	close(stall)
	waitFor(t, func() bool { return restartsOf(m, sb.ID) == 1 && !rebootPending(m, sb) })
}

func TestAPendingRebootSettlesAfterARestart(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-rp-1"] = "/vsock/rp"
	dataDir := t.TempDir()
	seedClaims(t, dataDir, map[string]*types.Sandbox{"sb_rp": {
		ID: "sb_rp", VMName: "sbx-rp-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/rp",
		BootID: "boot-2", RebootPending: true, Env: types.Env{"MODE": {Value: "prod"}},
	}})
	m := mustManager(t, &config.Config{DataDir: dataDir}, eng)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitJournaledReboot(t, m, "sb_rp", 1, "boot-2")

	eng.mu.Lock()
	defer eng.mu.Unlock()
	if got := eng.guestEnvs["/vsock/rp"]; got != "MODE=\"prod\"\n" {
		t.Errorf("guest env %q, want the pending settle run after the restart", got)
	}
}

func TestANoticeDuringAHibernateCaptureSettlesAfterTheWake(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"MODE": {Value: "prod"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	rebootInCapture(t, m, eng, sb)
	if got, _ := m.Sandbox(sb.ID); !got.Hibernated || got.Restarts != 0 {
		t.Fatalf("summary %+v, want the claim hibernated with no settle yet", got)
	}
	eng.mu.Lock()
	delete(eng.guestEnvs, sb.VsockSocket)
	eng.mu.Unlock()

	sock := wakeSock(t, m, sb)

	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if got := eng.guestEnvs[sock]; got != "MODE=\"prod\"\n" {
		t.Errorf("guest env %q, want the env the reboot lost written after the wake", got)
	}
}

func TestANoticeDuringAFailedCaptureSettlesWhenTheClaimRunsAgain(t *testing.T) {
	eng := newFakeEngine()
	eng.hibernateErr = errors.New("hibernate failed")
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	rebootInCapture(t, m, eng, sb)

	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
	if got, _ := m.Sandbox(sb.ID); got.Hibernated || got.Failed != "" {
		t.Errorf("summary %+v, want the claim running after the failed capture", got)
	}
}

func TestAPendingRebootSettlesAfterAMoveIn(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	mc.p.roundHook = func(round int) error {
		if round == 3 {
			mc.s.Rebooted(t.Context(), sb, "boot-2")
		}
		return nil
	}

	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}

	waitJournaledReboot(t, mc.t, sb.ID, 1, "boot-2")
}

func TestARebootSettleWaitsForAVolumeChangeAndRemountsTheSetItApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		_, run, err := m.beginVolumeRun(t.Context(), sb)
		if err != nil {
			t.Fatalf("begin volume run: %v", err)
		}

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		if got := liveMountsOf(eng); len(got) != 0 {
			t.Fatalf("live mounts %v while a volume change runs, want the settle to wait", got)
		}
		m.mu.Lock()
		sb.Volumes = append(slices.Clone(sb.Volumes), types.Volume{Name: "other", Mount: "/volumes/other"})
		m.mu.Unlock()
		m.finishVolumeRun(sb, run)
		synctest.Wait()

		if got := liveMountsOf(eng); !slices.Equal(got, []string{"data:/volumes/data", "other:/volumes/other"}) {
			t.Errorf("live mounts %v, want the set applied when the change ended", got)
		}
	})
}

func TestTheSameBootNoticeTwiceSettlesOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine())
		sb := mustClaim(t, m, testKey)

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		m.Rebooted(t.Context(), sb, "boot-2")
		time.Sleep(2 * rebootSettleGap)
		synctest.Wait()

		if got := restartsOf(m, sb.ID); got != 1 {
			t.Errorf("restarts %d, want 1", got)
		}
	})
}

func TestABurstOfNoticesInsideTheGapSettlesOnceWhenTheGapEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine())
		sb := mustClaim(t, m, testKey)
		m.Rebooted(t.Context(), sb, "boot-1")
		synctest.Wait()
		if got := restartsOf(m, sb.ID); got != 1 {
			t.Fatalf("restarts %d after the first notice, want 1", got)
		}

		for i := range 5 {
			m.Rebooted(t.Context(), sb, fmt.Sprintf("boot-%d", i+2))
			time.Sleep(time.Second)
		}
		time.Sleep(rebootSettleGap - 6*time.Second)
		synctest.Wait()
		if got := restartsOf(m, sb.ID); got != 1 {
			t.Fatalf("restarts %d before the gap ends, want 1", got)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if got := restartsOf(m, sb.ID); got != 2 {
			t.Fatalf("restarts %d after the gap, want the burst settled once", got)
		}
		time.Sleep(2 * rebootSettleGap)
		synctest.Wait()
		if got := restartsOf(m, sb.ID); got != 2 {
			t.Errorf("restarts %d, want no settle past the one the burst earned", got)
		}
		if row := journaled(t, m)[sb.ID]; row == nil || row.BootID != "boot-6" {
			t.Errorf("journal %+v, want the last boot recorded", row)
		}
	})
}

func TestANoticeOfAColdBootIsRecordedWithoutASettle(t *testing.T) {
	for _, tt := range []struct {
		name     string
		pools    []config.PoolSpec
		door     bool
		clone    bool
		direct   bool
		wantBoot string
		park     func(t *testing.T, m *Manager, sb *types.Sandbox)
		boot     func(t *testing.T, m *Manager, sb *types.Sandbox)
	}{
		{
			name:     "liveness restart",
			door:     true,
			wantBoot: coldBootID,
			park:     func(_ *testing.T, m *Manager, sb *types.Sandbox) { killVMM(fakeOf(m), sb.VMName) },
			boot:     func(t *testing.T, m *Manager, _ *types.Sandbox) { runLiveness(t, m) },
		},
		{
			name:     "wake of a failed claim",
			door:     true,
			wantBoot: coldBootID,
			park: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				editView(m, func(v *configView) { v.vmmRestart = types.VMMRestartNone })
				killVMM(fakeOf(m), sb.VMName)
				runLiveness(t, m)
			},
			boot: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
					t.Fatalf("wake: %v", err)
				}
			},
		},
		{
			name:     "hibernate wake that cold-boots",
			direct:   true,
			wantBoot: coldBootID2,
			park: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				hibernateClaim(t, m, sb)
				fakeOf(m).probeFails = wakeRestoreTries
			},
			boot: wakeForTest,
		},
		{
			name:  "archived wake that cold-boots its clone",
			pools: []config.PoolSpec{archivePool(keepAnHour)},
			clone: true,
			park: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				mustArchive(t, m, sb)
				fakeOf(m).probeFails = wakeRestoreTries
			},
			boot: wakeForTest,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.sockRoot = outboundtest.SockRoot(t)
			m := newTestManager(t, eng, tt.pools...)
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			t.Cleanup(func() { m.out.Disarm(sb.ID, true) })
			tt.park(t, m, sb)
			name := lockedVMName(sb)
			var knockErr, dialErr error
			queuedInBoot := true
			eng.startHook = func() {
				if tt.clone {
					name = eng.clones[len(eng.clones)-1]
				}
				if tt.door {
					knockErr = knockBootDoor(eng.vms[name], coldBootID)
				} else if conn, err := net.Dial("unix", engine.BootSocketPath(eng.vms[name])); err == nil {
					_ = conn.Close()
				} else {
					dialErr = err
				}
				if tt.direct {
					m.Rebooted(t.Context(), sb, coldBootID2)
				}
				queuedInBoot = rebootQueued(m, sb)
			}

			tt.boot(t, m, sb)

			if tt.door {
				if knockErr != nil {
					t.Fatalf("knock during the boot: %v, want the listening door to ack", knockErr)
				}
				if err := knockBootDoor(lockedSock(m, sb), coldBootID); err != nil {
					t.Fatalf("late knock: %v", err)
				}
			} else if dialErr == nil {
				t.Fatal("a door accepted the dial during the boot, want the dial refused")
			}
			if queuedInBoot || rebootQueued(m, sb) {
				t.Errorf("queued during the boot %v, after it %v, want no reboot settle", queuedInBoot, rebootQueued(m, sb))
			}
			if row := journaled(t, m)[sb.ID]; row == nil || row.Restarts != 1 || row.BootID != tt.wantBoot {
				t.Errorf("journal %+v, want exactly the one cold boot counted and boot %q recorded", row, tt.wantBoot)
			}
		})
	}
}

func TestANoticeDuringAColdBootFinishedAtStartupIsRecordedWithoutASettle(t *testing.T) {
	eng := newFakeEngine()
	sock := outboundtest.SockRoot(t) + "/cb"
	eng.vms["sbx-cb-1"] = sock
	stall := make(chan struct{})
	eng.probeStall = stall
	dataDir := t.TempDir()
	seedClaims(t, dataDir, map[string]*types.Sandbox{"sb_cb": {
		ID: "sb_cb", VMName: "sbx-cb-1", Key: testKey, Token: "tok", VsockSocket: sock, ColdBooting: true,
	}})
	m := mustManager(t, &config.Config{DataDir: dataDir}, eng)
	probes := eng.probeCount()
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool { return eng.probeCount() > probes })
	sb, _ := m.byID("sb_cb")

	if err := knockBootDoor(sock, coldBootID); err != nil {
		t.Fatalf("knock during the boot: %v", err)
	}
	queuedInBoot := rebootQueued(m, sb)
	close(stall)
	waitFor(t, func() bool { row := journaled(t, m)["sb_cb"]; return row != nil && !row.ColdBooting })
	if err := knockBootDoor(sock, coldBootID); err != nil {
		t.Fatalf("late knock: %v", err)
	}

	if queuedInBoot || rebootQueued(m, sb) {
		t.Errorf("queued during the boot %v, after it %v, want no reboot settle", queuedInBoot, rebootQueued(m, sb))
	}
	if row := journaled(t, m)["sb_cb"]; row.Restarts != 1 || row.BootID != coldBootID {
		t.Errorf("journal %+v, want the one cold boot counted and its boot recorded", row)
	}
	if err := knockBootDoor(sock, coldBootID2); err != nil {
		t.Fatalf("knock of a new boot: %v", err)
	}
	waitJournaledReboot(t, m, "sb_cb", 2, coldBootID2)
}

func restartsOf(m *Manager, id string) int {
	got, _ := m.Sandbox(id)
	return got.Restarts
}

func rebootQueued(m *Manager, sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reboots[sb].queued
}

func rebootPending(m *Manager, sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.RebootPending
}

func waitJournaledReboot(t *testing.T, m *Manager, id string, restarts int, bootID string) {
	t.Helper()
	waitFor(t, func() bool {
		row := journaled(t, m)[id]
		return row != nil && row.Restarts == restarts && row.BootID == bootID && !row.RebootPending
	})
}

func liveMountsOf(eng *fakeEngine) []string {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return slices.Clone(eng.liveMounts)
}

func lockedSock(m *Manager, sb *types.Sandbox) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.VsockSocket
}

func knockBootDoor(sock, bootID string) error {
	conn, err := net.Dial("unix", engine.BootSocketPath(sock))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = io.WriteString(conn, bootID+"\n"); err != nil {
		return err
	}
	_, err = io.ReadFull(conn, make([]byte, 1))
	return err
}

func volumeStates(t *testing.T, m *Manager, id string) map[string]types.VolumeState {
	t.Helper()
	view, err := m.GetVolumes(id, "")
	if err != nil {
		t.Fatalf("get volumes: %v", err)
	}
	states := map[string]types.VolumeState{}
	for _, st := range view.States {
		states[st.Name] = st
	}
	return states
}

func rebootInCapture(t *testing.T, m *Manager, eng *fakeEngine, sb *types.Sandbox) {
	t.Helper()
	stall := make(chan struct{})
	eng.mu.Lock()
	eng.hibernateStall = stall
	eng.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.hibernates) > 0
	})
	m.Rebooted(t.Context(), sb, "boot-2")
	if rebootQueued(m, sb) {
		t.Error("a notice during a capture queued a settle")
	}
	close(stall)
	<-done
}

func wakeSock(t *testing.T, m *Manager, sb *types.Sandbox) string {
	t.Helper()
	sock, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token})
	if err != nil {
		t.Fatalf("wake: %v", err)
	}
	return sock
}

func wakeForTest(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
}
