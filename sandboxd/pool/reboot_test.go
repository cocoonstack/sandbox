package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
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
		probes := eng.probeCount()

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		if got := eng.probeCount(); got != probes+1 {
			t.Fatalf("probes %d before the release, want the settle held in its probe", got-probes)
		}
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

func TestANoticeDuringACaptureWhoseWakeColdBootsCountsOneRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		queuedByColdArm := false
		withOutbound(t, m, func(o *outbound.Options) {
			armed := o.Armed
			o.Armed = func(ctx context.Context, armedSB *types.Sandbox) {
				armed(ctx, armedSB)
				m.mu.Lock()
				cold := armedSB.ColdBooting
				m.mu.Unlock()
				queuedByColdArm = queuedByColdArm || cold && rebootQueued(m, armedSB)
			}
		})
		rebootInCapture(t, m, eng, sb)
		eng.mu.Lock()
		eng.probeFails = wakeRestoreTries
		eng.mu.Unlock()

		wakeForTest(t, m, sb)
		time.Sleep(2 * rebootSettleGap)
		synctest.Wait()

		if row := journaled(t, m)[sb.ID]; row.Restarts != 1 || row.RebootPending {
			t.Errorf("journal %+v, want the cold boot counted once and the notice it covered cleared", row)
		}
		if queuedByColdArm {
			t.Error("the cold boot's arm queued a reboot settle")
		}
	})
}

func TestASettleQueuedBeforeAColdBootDoesNothingAfterIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		m.Rebooted(t.Context(), sb, "boot-1")
		synctest.Wait()
		m.Rebooted(t.Context(), sb, "boot-2")
		killVMM(eng, sb.VMName)
		runLiveness(t, m)
		if got := restartsOf(m, sb.ID); got != 2 {
			t.Fatalf("restarts %d after the settle and the cold boot, want 2", got)
		}

		time.Sleep(2 * rebootSettleGap)
		synctest.Wait()

		if got := restartsOf(m, sb.ID); got != 2 {
			t.Errorf("restarts %d, want the settle queued before the cold boot to do nothing", got)
		}
	})
}

func TestANoticeDuringACaptureWhoseArchivedWakeColdBootsCountsOneRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng, archivePool(keepAnHour))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		rebootInCapture(t, m, eng, sb)
		if err := m.archive(t.Context(), sb); err != nil {
			t.Fatalf("archive: %v", err)
		}
		eng.mu.Lock()
		eng.probeFails = wakeRestoreTries
		eng.mu.Unlock()

		wakeForTest(t, m, sb)
		time.Sleep(2 * rebootSettleGap)
		synctest.Wait()

		if row := journaled(t, m)[sb.ID]; row.Restarts != 1 || row.RebootPending {
			t.Errorf("journal %+v, want the cold-booted clone counted once and the notice it covered cleared", row)
		}
	})
}

func TestARebootLeavesADetachingVolumeToItsDetachRetry(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.mu.Lock()
	eng.detachVolumeErr = errors.New("device busy")
	eng.mu.Unlock()
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Remove: []string{"data"}}); err == nil {
		t.Fatal("the detach succeeded, want it to fail and leave the volume detaching")
	}
	held := volumeHoldersOf(m, "data")
	eng.mu.Lock()
	eng.liveMountErrFor = "data"
	eng.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")

	eng.mu.Lock()
	calls := slices.Clone(eng.liveMountCalls)
	eng.mu.Unlock()
	if len(calls) != 0 {
		t.Errorf("remount dispatches %v, want the detaching volume left alone", calls)
	}
	if got := volumeHoldersOf(m, "data"); got != held || held == (volumeHolders{}) {
		t.Errorf("holders %+v, want the detaching volume's hold %+v kept", got, held)
	}
	if st := volumeStates(t, m, sb.ID)["data"]; st.State != types.VolumeDetaching {
		t.Errorf("state %+v, want the volume still detaching", st)
	}
	if row := journaled(t, m)[sb.ID]; len(row.Volumes) != 1 {
		t.Errorf("journal volumes %v, want the attached disk still applied", row.Volumes)
	}
}

func TestAFailedRemountKeepsTheMountConflictItHit(t *testing.T) {
	eng := newFakeEngine()
	eng.liveMountErrFor = "data"
	eng.liveMountErr = &engine.VolumeMountConflict{Message: "volume device has another mount at \"/elsewhere\""}
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	m.Rebooted(t.Context(), sb, "boot-2")
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")

	if st := volumeStates(t, m, sb.ID)["data"]; st.Error != eng.liveMountErr.Error() {
		t.Errorf("state %+v, want the mount conflict's text", st)
	}
}

func TestOnlyANoticeThatMakesAClaimPendingWritesTheJournal(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	hibernateClaim(t, m, sb)
	m.Rebooted(t.Context(), sb, "boot-2")
	breakStore(t, m)

	m.Rebooted(t.Context(), sb, "boot-3")

	if m.store.recommitting.Load() {
		t.Error("a notice for a claim already pending wrote the journal")
	}
	healStore(t, m)
}

func TestARebootSettleWhoseProbeFailsOnceSettlesOnTheRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, liveCatalog(t))
		sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		eng.mu.Lock()
		eng.probeFails = 1
		eng.mu.Unlock()

		m.Rebooted(t.Context(), sb, "boot-2")
		synctest.Wait()
		if restartsOf(m, sb.ID) != 0 || !rebootPending(m, sb) || !rebootQueued(m, sb) {
			t.Fatalf("restarts %d, pending %v, queued %v after a failed probe, want no commit and a retry queued", restartsOf(m, sb.ID), rebootPending(m, sb), rebootQueued(m, sb))
		}
		time.Sleep(rebootSettleGap)
		synctest.Wait()

		if restartsOf(m, sb.ID) != 1 || rebootPending(m, sb) {
			t.Errorf("restarts %d, pending %v, want the retry to settle once", restartsOf(m, sb.ID), rebootPending(m, sb))
		}
		if got := liveMountsOf(eng); !slices.Equal(got, []string{"data:/volumes/data"}) {
			t.Errorf("live mounts %v, want the retry to mount the volume", got)
		}
	})
}

func TestARebootSettleStopsAfterItsProbeRetriesAndAFreshNoticeQueuesAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		eng.mu.Lock()
		eng.probeErr = errors.New("silkd probe: context deadline exceeded")
		eng.mu.Unlock()
		probes := eng.probeCount()

		m.Rebooted(t.Context(), sb, "boot-2")
		time.Sleep(10 * rebootSettleGap)
		synctest.Wait()

		if got := eng.probeCount() - probes; got != 1+rebootProbeRetries {
			t.Errorf("probes %d, want one try and %d retries", got, rebootProbeRetries)
		}
		if rebootQueued(m, sb) || !rebootPending(m, sb) || restartsOf(m, sb.ID) != 0 {
			t.Fatalf("queued %v, pending %v, restarts %d, want nothing queued, the notice still pending and no restart", rebootQueued(m, sb), rebootPending(m, sb), restartsOf(m, sb.ID))
		}
		m.Rebooted(t.Context(), sb, "boot-3")
		time.Sleep(10 * rebootSettleGap)
		synctest.Wait()

		if got := eng.probeCount() - probes; got != 2*(1+rebootProbeRetries) {
			t.Errorf("probes %d, want the fresh notice to start a new round of retries", got)
		}
	})
}

func TestAnArmSettlesAPendingNoticeAfterTheProbeRetriesStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		eng.mu.Lock()
		eng.probeErr = errors.New("silkd probe: context deadline exceeded")
		eng.mu.Unlock()
		m.Rebooted(t.Context(), sb, "boot-2")
		time.Sleep(10 * rebootSettleGap)
		synctest.Wait()
		eng.mu.Lock()
		eng.probeErr = nil
		eng.mu.Unlock()

		if err := m.out.ArmProxy(t.Context(), sb); err != nil {
			t.Fatalf("arm: %v", err)
		}
		synctest.Wait()

		if restartsOf(m, sb.ID) != 1 || rebootPending(m, sb) {
			t.Errorf("restarts %d, pending %v, want the arm to settle the pending notice", restartsOf(m, sb.ID), rebootPending(m, sb))
		}
	})
}

func TestAReleaseDuringArmKeepsTheTapLockedUntilTheDelayedRemoval(t *testing.T) {
	eng := newFakeEngine()
	eng.tap = "tap-fake0"
	eng.sockRoot = outboundtest.SockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	editView(m, func(v *configView) { v.releaseDelay = 100 * time.Millisecond })
	var taps outboundtest.TapLog
	var released atomic.Bool
	withOutbound(t, m, func(o *outbound.Options) {
		o.Lock, o.Unlock = taps.Lock, taps.Unlock
		armed := o.Armed
		o.Armed = func(ctx context.Context, sb *types.Sandbox) {
			armed(ctx, sb)
			if released.CompareAndSwap(false, true) {
				if err := m.Release(ctx, sb.ID, Cred{Token: sb.Token}); err != nil {
					t.Errorf("release: %v", err)
				}
			}
		}
	})

	sb, err := m.ClaimProvision(t.Context(), egKey, ClaimOptions{})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if got := taps.Unlocked(); len(got) != 0 {
		t.Fatalf("unlocked %v while the released VM still runs, want the tap kept locked", got)
	}
	time.Sleep(150 * time.Millisecond)
	m.retryRemovals(t.Context()).Wait()
	if got := taps.Unlocked(); !slices.Equal(got, []string{"tap-fake0"}) || !eng.removed(sb.VMName) {
		t.Errorf("unlocked %v, removed %v, want the removal to unlock the tap", got, eng.removed(sb.VMName))
	}
}

func TestANoticeDuringAMoveInStartSettlesWhenTheMoveInCommits(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	var noticed atomic.Bool
	withOutbound(t, mc.t, func(o *outbound.Options) {
		armed := o.Armed
		o.Armed = func(ctx context.Context, moved *types.Sandbox) {
			armed(ctx, moved)
			mc.t.mu.Lock()
			moving := moved.MoveIn
			mc.t.mu.Unlock()
			if moving && noticed.CompareAndSwap(false, true) {
				mc.t.Rebooted(ctx, moved, "boot-2")
			}
		}
	})

	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}

	if !noticed.Load() {
		t.Fatal("no notice reached the destination before its move-in committed")
	}
	waitJournaledReboot(t, mc.t, sb.ID, 1, "boot-2")
}

func TestANoticeHeldByAHibernateWhoseIntentFailedSettles(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.RebootPending = true
	m.mu.Unlock()
	_, held, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatalf("hold a volume run: %v", err)
	}
	breakStore(t, m)

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("the hibernate succeeded, want its intent write to fail")
	}
	if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
		t.Fatalf("heal store: %v", err)
	}
	m.finishVolumeRun(sb, held)

	waitFor(t, func() bool { return restartsOf(m, sb.ID) == 1 && !rebootPending(m, sb) })
}

func TestANoticeBetweenAMoveInStartAndItsProbeReachesTheBootDoor(t *testing.T) {
	mc := newMoveCluster(t)
	sb := mustClaim(t, mc.s, testKey)
	t.Cleanup(func() { mc.t.out.Disarm(sb.ID, true) })
	dest := fakeOf(mc.t)
	var knockErr error
	knocked := false
	dest.startHook = func() {
		knocked = true
		knockErr = knockBootDoor(dest.vms[dest.receives[len(dest.receives)-1]], coldBootID)
	}

	if _, err := mc.s.Move(t.Context(), sb.ID, "b", time.Minute); err != nil {
		t.Fatalf("move: %v", err)
	}

	if !knocked || knockErr != nil {
		t.Fatalf("knock between start and probe: ran %v, err %v, want the boot door to ack", knocked, knockErr)
	}
	waitJournaledReboot(t, mc.t, sb.ID, 1, coldBootID)
}

func TestANoticeDuringARolledBackReleaseSettlesAfterTheRollback(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	_, held, err := m.beginVolumeRun(t.Context(), sb)
	if err != nil {
		t.Fatalf("hold a volume run: %v", err)
	}
	breakStore(t, m)
	m.store.writeMu.Lock()
	done := make(chan error, 1)
	go func() { done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	waitFor(t, func() bool { return claimedCount(m) == 0 })

	m.Rebooted(t.Context(), sb, "boot-2")
	m.store.writeMu.Unlock()
	if err := <-done; err == nil {
		t.Fatal("the release committed, want its journal write to fail and roll back")
	}
	if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
		t.Fatalf("heal store: %v", err)
	}
	m.finishVolumeRun(sb, held)

	waitFor(t, func() bool { return restartsOf(m, sb.ID) == 1 && !rebootPending(m, sb) })
	healStore(t, m)
}

func TestARolledBackReleaseCountsAnEarlierQueuedSettleOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine())
		sb := mustClaim(t, m, testKey)
		m.Rebooted(t.Context(), sb, "boot-1")
		synctest.Wait()
		m.Rebooted(t.Context(), sb, "boot-2")
		_, held, err := m.beginVolumeRun(t.Context(), sb)
		if err != nil {
			t.Fatalf("hold a volume run: %v", err)
		}
		breakStore(t, m)
		m.store.writeMu.Lock()
		done := make(chan error, 1)
		go func() { done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
		for claimedCount(m) != 0 {
			runtime.Gosched()
		}

		m.Rebooted(t.Context(), sb, "boot-3")
		m.store.writeMu.Unlock()
		if err := <-done; err == nil {
			t.Fatal("the release committed, want its journal write to fail and roll back")
		}
		if err := os.MkdirAll(filepath.Dir(m.store.path), 0o750); err != nil {
			t.Fatalf("heal store: %v", err)
		}
		m.finishVolumeRun(sb, held)
		time.Sleep(3 * rebootSettleGap)
		synctest.Wait()

		if got := restartsOf(m, sb.ID); got != 2 || rebootPending(m, sb) {
			t.Errorf("restarts %d, pending %v, want the settle queued before the release and the rollback's settle to count once", got, rebootPending(m, sb))
		}
		rows, loadErr := newClaimStore(filepath.Dir(m.store.path), false).load()
		if loadErr != nil {
			t.Fatalf("load journal: %v", loadErr)
		}
		if row := rows[sb.ID]; row == nil || row.BootID != "boot-3" {
			t.Errorf("journal %+v, want the notice delivered during the release recorded", row)
		}
	})
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
		other := types.Volume{Name: "other", Mount: "/volumes/other"}
		sb.Volumes, sb.VolumesWant = append(slices.Clone(sb.Volumes), other), append(slices.Clone(sb.VolumesWant), other)
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
	t.Cleanup(func() { m.out.Disarm("sb_cb", true) })
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
