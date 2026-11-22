package pool

import (
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
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

func TestARebootSettleRacingAReleaseNeverMountsAfterIt(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	entered, hold := make(chan struct{}), make(chan struct{})
	eng.mu.Lock()
	eng.liveMountHook = func() {
		close(entered)
		<-hold
	}
	eng.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	<-entered
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	close(hold)
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.volumeRuns[sb] == nil
	})

	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.liveMounts) != 0 {
		t.Errorf("live mounts %v, want the release to cancel the remount it raced", eng.liveMounts)
	}
}

func TestARebootSettleRacingAReleaseBetweenTheProbeAndTheMountNeverMounts(t *testing.T) {
	eng := newFakeEngine()
	stall := make(chan struct{})
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	probes := eng.probeCount()
	eng.mu.Lock()
	eng.probeStall = stall
	eng.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	waitFor(t, func() bool { return eng.probeCount() > probes })
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release: %v", err)
	}
	close(stall)
	waitFor(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.volumeRuns[sb] == nil
	})

	if got := liveMountsOf(eng); len(got) != 0 {
		t.Errorf("live mounts %v, want no mount after the release that landed before it", got)
	}
}

func TestANoticeWhileAWakeIsInFlightSettles(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	m.mu.Lock()
	sb.Waking = true
	m.mu.Unlock()

	m.Rebooted(t.Context(), sb, "boot-2")
	queued := rebootQueued(m, sb)
	m.mu.Lock()
	sb.Waking = false
	m.mu.Unlock()

	if !queued {
		t.Error("a notice at the door of a claim whose wake has not ended queued no settle")
	}
	waitJournaledReboot(t, m, sb.ID, 1, "boot-2")
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

	if got := liveMountsOf(eng); !slices.Equal(got, []string{"other:/volumes/other"}) {
		t.Errorf("live mounts %v, want the second volume mounted after the first failed", got)
	}
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil {
		t.Fatalf("get volumes: %v", err)
	}
	for _, st := range view.States {
		if failed := st.Name == "data"; failed != (st.Error != "") {
			t.Errorf("volume %s state %+v, want an error on the failed remount only", st.Name, st)
		}
	}
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

func TestANoticeForAClaimThatDoesNotRunIsDropped(t *testing.T) {
	for _, tt := range []struct {
		name string
		park func(t *testing.T, m *Manager, sb *types.Sandbox)
	}{
		{"released", func(t *testing.T, m *Manager, sb *types.Sandbox) {
			if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatalf("release: %v", err)
			}
		}},
		{"hibernated", func(t *testing.T, m *Manager, sb *types.Sandbox) { hibernateClaim(t, m, sb) }},
		{"archived", func(t *testing.T, m *Manager, sb *types.Sandbox) { mustArchive(t, m, sb) }},
		{"failed", func(t *testing.T, m *Manager, sb *types.Sandbox) {
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

			m.mu.Lock()
			bootID := sb.BootID
			m.mu.Unlock()
			if rebootQueued(m, sb) || bootID != "" {
				t.Errorf("queued %v, boot %q, want the notice dropped", rebootQueued(m, sb), bootID)
			}
		})
	}
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
			name:   "archived wake that cold-boots its clone",
			pools:  []config.PoolSpec{archivePool(keepAnHour)},
			clone:  true,
			direct: true,
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
			tt.park(t, m, sb)
			name := lockedVMName(sb)
			var knockErr error
			queuedInBoot := true
			eng.startHook = func() {
				if tt.clone {
					name = eng.clones[len(eng.clones)-1]
				}
				knockErr = knockBootDoor(eng.vms[name], coldBootID)
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
			} else if knockErr == nil {
				t.Fatal("a door listened during the boot, want the notice refused")
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

func waitJournaledReboot(t *testing.T, m *Manager, id string, restarts int, bootID string) {
	t.Helper()
	waitFor(t, func() bool {
		row := journaled(t, m)[id]
		return row != nil && row.Restarts == restarts && row.BootID == bootID
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

func wakeForTest(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
}
