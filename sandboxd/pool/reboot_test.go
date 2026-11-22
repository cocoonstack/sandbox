package pool

import (
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
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
	waitFor(t, func() bool { return restartsOf(m, sb.ID) == 1 })
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
	if row := journaled(t, m)[sb.ID]; row == nil || row.Restarts != 1 || row.RestartedAt.IsZero() || row.BootID != "boot-2" {
		t.Errorf("journal %+v, want one restart and the new boot recorded", row)
	}
	if got := usageEventsOf(t, m, sb.ID); got != "claim" {
		t.Errorf("events %q, want no usage event for a reboot inside the VMM", got)
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
		name  string
		pools []config.PoolSpec
		park  func(t *testing.T, m *Manager, sb *types.Sandbox)
		boot  func(t *testing.T, m *Manager, sb *types.Sandbox)
	}{
		{
			name: "liveness restart",
			park: func(_ *testing.T, m *Manager, sb *types.Sandbox) { killVMM(fakeOf(m), sb.VMName) },
			boot: func(t *testing.T, m *Manager, _ *types.Sandbox) { runLiveness(t, m) },
		},
		{
			name: "wake of a failed claim",
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
			name: "hibernate wake that cold-boots",
			park: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				hibernateClaim(t, m, sb)
				fakeOf(m).probeFails = wakeRestoreTries
			},
			boot: wakeForTest,
		},
		{
			name:  "archived wake that cold-boots its clone",
			pools: []config.PoolSpec{archivePool(keepAnHour)},
			park: func(t *testing.T, m *Manager, sb *types.Sandbox) {
				mustArchive(t, m, sb)
				fakeOf(m).probeFails = wakeRestoreTries
			},
			boot: wakeForTest,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng, tt.pools...)
			sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{TTL: time.Hour})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			tt.park(t, m, sb)
			queuedInBoot := true
			eng.startHook = func() {
				m.Rebooted(t.Context(), sb, "boot-cold")
				queuedInBoot = rebootQueued(m, sb)
			}

			tt.boot(t, m, sb)
			m.Rebooted(t.Context(), sb, "boot-cold")

			if queuedInBoot || rebootQueued(m, sb) {
				t.Errorf("queued during the boot %v, after it %v, want no reboot settle", queuedInBoot, rebootQueued(m, sb))
			}
			if row := journaled(t, m)[sb.ID]; row == nil || row.Restarts != 1 || row.BootID != "boot-cold" {
				t.Errorf("journal %+v, want the one cold boot counted and its boot recorded", row)
			}
		})
	}
}

func TestANoticeDuringAColdBootFinishedAtStartupIsRecordedWithoutASettle(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-cb-1"] = "/vsock/cb"
	stall := make(chan struct{})
	eng.probeStall = stall
	dataDir := t.TempDir()
	seedClaims(t, dataDir, map[string]*types.Sandbox{"sb_cb": {
		ID: "sb_cb", VMName: "sbx-cb-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/cb", ColdBooting: true,
	}})
	m := mustManager(t, &config.Config{DataDir: dataDir}, eng)
	probes := eng.probeCount()
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool { return eng.probeCount() > probes })
	sb, _ := m.byID("sb_cb")

	m.Rebooted(t.Context(), sb, "boot-cold")
	queuedInBoot := rebootQueued(m, sb)
	close(stall)
	waitFor(t, func() bool { row := journaled(t, m)["sb_cb"]; return row != nil && !row.ColdBooting })
	m.Rebooted(t.Context(), sb, "boot-cold")

	if queuedInBoot || rebootQueued(m, sb) {
		t.Errorf("queued during the boot %v, after it %v, want no reboot settle", queuedInBoot, rebootQueued(m, sb))
	}
	if row := journaled(t, m)["sb_cb"]; row.Restarts != 1 || row.BootID != "boot-cold" {
		t.Errorf("journal %+v, want the one cold boot counted and its boot recorded", row)
	}
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

func wakeForTest(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake: %v", err)
	}
}
