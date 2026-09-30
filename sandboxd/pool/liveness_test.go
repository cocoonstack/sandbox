package pool

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLivenessRestartsExitedVMM(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)

	runLiveness(t, m)

	got, _ := m.Sandbox(sb.ID)
	if got.Restarts != 1 || got.Failed != "" || got.RestartedAt.IsZero() {
		t.Fatalf("summary %+v, want one restart and no failure", got)
	}
	if !slices.Equal(eng.starts, []string{sb.VMName}) {
		t.Errorf("starts %v, want %s cold-booted once", eng.starts, sb.VMName)
	}
	if _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Errorf("data plane after restart: %v", err)
	}
	if got, want := usageEventsOf(t, m, sb.ID), "claim,vmm_exit,vmm_restart"; got != want {
		t.Errorf("events %s, want %s", got, want)
	}
	if c := m.Counters(); c.VMMRestarts != 1 || c.VMMFailures != 0 {
		t.Errorf("counters %+v, want one restart", c)
	}
	reloaded, err := newClaimStore(m.dataDir, false).load()
	if err != nil || reloaded[sb.ID].Restarts != 1 {
		t.Errorf("journal restarts %d (err %v), want 1", reloaded[sb.ID].Restarts, err)
	}
}

func TestLivenessNonePolicyFailsAndWakeRestarts(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.vmmRestart = types.VMMRestartNone
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)

	runLiveness(t, m)

	if got, _ := m.Sandbox(sb.ID); got.Failed != "vmm exited" {
		t.Fatalf("failed %q, want vmm exited", got.Failed)
	}
	if len(eng.starts) != 0 {
		t.Errorf("starts %v, want none under the none policy", eng.starts)
	}
	if _, g := m.Info(); g.Failed != 1 {
		t.Errorf("failed gauge %d, want 1", g.Failed)
	}
	if _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrFailed) {
		t.Errorf("relay on a failed claim: %v, want ErrFailed", err)
	}
	if _, err := m.DialPort(t.Context(), sb.ID, Cred{Operator: true}, 80); !errors.Is(err, ErrFailed) {
		t.Errorf("passive dial on a failed claim: %v, want ErrFailed", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrFailed) {
		t.Errorf("hibernate on a failed claim: %v, want ErrFailed", err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake a failed claim: %v", err)
	}
	if got, _ := m.Sandbox(sb.ID); got.Failed != "" || got.Restarts != 1 {
		t.Errorf("summary %+v, want the wake to cold-boot it", got)
	}
	if got, want := usageEventsOf(t, m, sb.ID), "claim,vmm_exit,vmm_failed,vmm_restart"; got != want {
		t.Errorf("events %s, want %s", got, want)
	}
}

func TestLivenessCrashLoopSpendsBudget(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	for range vmmRestartBudget {
		killVMM(eng, sb.VMName)
		m.recoverVMM(t.Context(), sb)
	}
	if got, _ := m.Sandbox(sb.ID); got.Restarts != vmmRestartBudget || got.Failed != "" {
		t.Fatalf("summary %+v, want %d restarts inside the budget", got, vmmRestartBudget)
	}
	killVMM(eng, sb.VMName)
	m.recoverVMM(t.Context(), sb)

	got, _ := m.Sandbox(sb.ID)
	if !strings.Contains(got.Failed, "restarts within") {
		t.Errorf("failed %q, want the spent budget named", got.Failed)
	}
	if len(eng.starts) != vmmRestartBudget {
		t.Errorf("starts %d, want no start past the budget", len(eng.starts))
	}
}

func TestLivenessBudgetForgetsOldRestarts(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	old := time.Now().Add(-2 * vmmRestartWindow)
	sb.RestartLog = []time.Time{old, old, old}
	killVMM(eng, sb.VMName)

	m.recoverVMM(t.Context(), sb)

	if got, _ := m.Sandbox(sb.ID); got.Failed != "" || got.Restarts != 1 {
		t.Errorf("summary %+v, want restarts outside the window forgotten", got)
	}
}

func TestLivenessRestartFailureRetriesThenFails(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)
	eng.startErr = errors.New("boot failed")

	for range vmmRestartBudget - 1 {
		m.recoverVMM(t.Context(), sb)
		if got, _ := m.Sandbox(sb.ID); got.Failed != "" {
			t.Fatalf("failed %q after a retryable attempt", got.Failed)
		}
	}
	m.recoverVMM(t.Context(), sb)

	got, _ := m.Sandbox(sb.ID)
	if got.Failed != reasonColdBootFailed || got.Restarts != 0 {
		t.Errorf("summary %+v, want the fixed reason %q after %d attempts", got, reasonColdBootFailed, vmmRestartBudget)
	}
	if len(eng.stops) != vmmRestartBudget {
		t.Errorf("stops %v, want every failed start stopped", eng.stops)
	}
	if got, want := usageEventsOf(t, m, sb.ID), "claim,vmm_exit,vmm_failed"; got != want {
		t.Errorf("events %s, want one exit across the attempts: %s", got, want)
	}
}

func TestLivenessLeavesLiveAndPausedClaims(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	live := mustClaim(t, m, testKey)
	paused := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), paused.ID, Cred{Token: paused.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	runLiveness(t, m)
	m.recoverVMM(t.Context(), live)
	m.recoverVMM(t.Context(), paused)

	if len(eng.starts) != 0 {
		t.Errorf("starts %v, want a running and a hibernated claim left alone", eng.starts)
	}
	for _, sb := range []*types.Sandbox{live, paused} {
		if got, _ := m.Sandbox(sb.ID); got.Failed != "" || got.Restarts != 0 {
			t.Errorf("%s summary %+v, want untouched", sb.ID, got)
		}
	}
}

func TestLivenessFailsAClaimWhoseRecordIsGone(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.mu.Lock()
	delete(eng.vms, sb.VMName)
	eng.mu.Unlock()

	m.recoverVMM(t.Context(), sb)

	if got, _ := m.Sandbox(sb.ID); got.Failed != "vm record gone" {
		t.Errorf("failed %q, want vm record gone", got.Failed)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrFailed) {
		t.Errorf("wake without a record: %v, want ErrFailed", err)
	}
}

func TestLivenessLeavesALapsedLeaseToReap(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	gone := mustClaim(t, m, testKey)
	kept, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{OnExpire: types.ExpireArchive})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	m.mu.Lock()
	gone.Deadline = time.Now().Add(-time.Second)
	kept.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	killVMM(eng, gone.VMName)
	killVMM(eng, kept.VMName)

	runLiveness(t, m)

	if !slices.Equal(eng.starts, []string{kept.VMName}) {
		t.Errorf("starts %v, want only the archive-on-expire claim booted so reap can archive it", eng.starts)
	}
	if got, _ := m.Sandbox(gone.ID); got.Failed != "" || got.Restarts != 0 {
		t.Errorf("lapsed claim %+v, want it left to reap", got)
	}
}

func TestRestartFailedRefusesAReleasedClaim(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.vmmRestart = types.VMMRestartNone
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)
	m.recoverVMM(t.Context(), sb)
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if err := m.restartFailed(t.Context(), sb); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("restart of a released claim: %v, want ErrUnknownSandbox", err)
	}
	if len(eng.starts) != 0 {
		t.Errorf("starts %v, want a released VM never booted", eng.starts)
	}
}

func TestResyncRecordsADownEgressLaneTap(t *testing.T) {
	eng := newFakeEngine()
	m := egressManager(t, eng, config.PoolSpec{PoolKey: egKey, Egress: egPolicy})
	sb := &types.Sandbox{ID: "sb_down", VMName: "sbx-down", Key: egKey}
	m.mu.Lock()
	m.claimed[sb.ID] = sb
	m.mu.Unlock()
	live := map[string]types.VMRecord{"sbx-down": {
		Config: types.VMConfig{Name: "sbx-down"}, State: "stopped",
		NetworkConfigs: []types.VMNetConfig{{TAP: "tap-down"}},
	}}
	m.sweep = func(map[string]bool) error { return nil }

	m.resyncEgress(t.Context(), live, map[string]bool{})

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb {
		t.Fatal("down egress-lane claim quarantined, want it kept for the liveness pass")
	}
	if m.egressTaps[sb.ID] != "tap-down" {
		t.Errorf("egress tap %q, want tap-down recorded so release unlocks it", m.egressTaps[sb.ID])
	}
}

func TestColdBootBlocker(t *testing.T) {
	egressKey := testKey
	egressKey.Net = types.NetEgress
	tests := []struct {
		name  string
		sb    *types.Sandbox
		found bool
		want  string
	}{
		{"plain", &types.Sandbox{Key: testKey}, true, ""},
		{"no record", &types.Sandbox{Key: testKey}, false, "vm record gone"},
		{"egress lane", &types.Sandbox{Key: egressKey}, true, "cannot cold-boot"},
		{"volumes", &types.Sandbox{Key: testKey, Volumes: []types.Volume{{Name: "data"}}}, true, "volume mounts"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coldBootBlocker(tt.sb, tt.found)
			if (tt.want == "") != (got == "") || !strings.Contains(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExpiredFailedClaimIsDestroyedNotArchived(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.vmmRestart = types.VMMRestartNone
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{OnExpire: types.ExpireArchive})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	killVMM(eng, sb.VMName)
	m.recoverVMM(t.Context(), sb)
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()

	m.reapOnce(t.Context())

	waitFor(t, func() bool { return eng.removed(sb.VMName) })
	if len(eng.hibernates) != 0 {
		t.Errorf("hibernates %v, want no archive attempt on a failed claim", eng.hibernates)
	}
}

func TestReconcileKeepsRunningClaimsTheHostLost(t *testing.T) {
	for _, tt := range []struct {
		policy       types.VMMRestart
		wantFailed   string
		wantRestarts int
	}{
		{"", "", 1},
		{types.VMMRestartNone, "vmm exited", 0},
	} {
		t.Run(string(tt.policy), func(t *testing.T) {
			eng := newFakeEngine()
			eng.vms["sbx-lost-1"] = "/vsock/lost"
			eng.stopped["sbx-lost-1"] = true
			dataDir := t.TempDir()
			claims := map[string]*types.Sandbox{
				"sb_lost": {ID: "sb_lost", VMName: "sbx-lost-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/lost"},
			}
			if err := newClaimStore(dataDir, false).save(claims); err != nil {
				t.Fatalf("setup: %v", err)
			}
			m, err := NewManager(t.Context(), &config.Config{DataDir: dataDir, VMMRestart: tt.policy}, eng, testSecrets(t))
			if err != nil {
				t.Fatalf("setup manager: %v", err)
			}

			if err := m.Reconcile(t.Context()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if slices.Contains(eng.removes, "sbx-lost-1") {
				t.Fatal("reconcile swept the lost claim's VM")
			}
			runLiveness(t, m)

			got, ok := m.Sandbox("sb_lost")
			if !ok || got.Failed != tt.wantFailed || got.Restarts != tt.wantRestarts {
				t.Errorf("summary %+v (listed %v), want failed %q restarts %d", got, ok, tt.wantFailed, tt.wantRestarts)
			}
		})
	}
}

func runLiveness(t *testing.T, m *Manager) {
	t.Helper()
	m.livenessOnce(t.Context())
	waitFor(t, func() bool { return !m.livenessSweep.Load() })
}

func killVMM(eng *fakeEngine, name string) {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	eng.stopped[name] = true
}
