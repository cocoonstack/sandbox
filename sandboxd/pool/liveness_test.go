package pool

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
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

func TestWakeOfAFailedClaimRearmsEgressAfterARestart(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	t.Setenv("GH_TOKEN", "s3cr3t")
	cfg := &config.Config{
		DataDir: t.TempDir(), Bridges: []string{"sbxbr0"}, EgressCA: writeTestEgressCA(t), VMMRestart: types.VMMRestartNone,
		Pools: []config.PoolSpec{{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}}},
	}
	first, err := NewManager(t.Context(), cfg, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	sb := mustClaim(t, first, testKey)
	door := engine.EgressSocketPath(sb.VsockSocket)
	killVMM(eng, sb.VMName)
	first.recoverVMM(t.Context(), sb)
	first.disarmEgress(sb.ID, false)

	second, err := NewManager(t.Context(), cfg, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("restarted manager: %v", err)
	}
	if err = second.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if conn, dialErr := net.Dial("unix", door); dialErr == nil {
		_ = conn.Close()
		t.Fatal("a failed claim's door is served after the restart; the wake below would not prove the re-arm")
	}
	if err = second.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Wake: %v", err)
	}

	conn, err := net.Dial("unix", door)
	if err != nil {
		t.Fatalf("egress door after the wake: %v", err)
	}
	_ = conn.Close()
}

func TestRestartKeepsAnEgressDoorThatSurvivedTheVMM(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}})
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)

	m.recoverVMM(t.Context(), sb)

	conn, err := net.Dial("unix", engine.EgressSocketPath(sb.VsockSocket))
	if err != nil {
		t.Fatalf("egress door after the restart: %v", err)
	}
	_ = conn.Close()
}

func TestRestartRedeliversTheGuestEnv(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"MODE": {Value: "prod"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	eng.mu.Lock()
	delete(eng.guestEnvs, sb.VsockSocket)
	eng.mu.Unlock()
	killVMM(eng, sb.VMName)

	m.recoverVMM(t.Context(), sb)

	eng.mu.Lock()
	defer eng.mu.Unlock()
	if doc := eng.guestEnvs[sb.VsockSocket]; !strings.Contains(doc, "MODE=") {
		t.Errorf("guest env after the restart = %q, want the claim's entry written again", doc)
	}
}

func TestRestartThatCannotDeliverTheEnvFailsAndSpendsTheBudget(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Env: types.Env{"MODE": {Value: "prod"}}})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	killVMM(eng, sb.VMName)
	eng.mu.Lock()
	eng.guestEnvErr = errors.New("guest write refused")
	eng.mu.Unlock()

	assertRestartFailsUntilBudget(t, m, eng, sb)
}

func TestRestartThatCannotArmTheEgressDoorFailsAndSpendsTheBudget(t *testing.T) {
	eng := newFakeEngine()
	eng.sockRoot = sockRoot(t)
	m := egressManager(t, eng, config.PoolSpec{PoolKey: testKey, Egress: &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}})
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)
	if err := os.Chmod(eng.sockRoot, 0o500); err != nil {
		t.Fatalf("lock the door directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(eng.sockRoot, 0o700) })

	assertRestartFailsUntilBudget(t, m, eng, sb)
}

func TestDaemonExitEventRecoversWithoutAList(t *testing.T) {
	eng := newFakeEngine()
	eng.vmEvents = make(chan fakeVMEvent)
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go m.watchVMMs(ctx)
	eng.vmEvents <- fakeVMEvent{sync: []engine.VMStatus{{Name: sb.VMName, Live: true}}}
	waitFor(t, m.vmmEvents.Load)
	lists := eng.listCalls()

	killVMM(eng, sb.VMName)
	eng.vmEvents <- fakeVMEvent{change: engine.VMChange{Kind: "MODIFIED", VM: engine.VMStatus{Name: sb.VMName}}}

	waitFor(t, func() bool { got, _ := m.Sandbox(sb.ID); return got.Restarts == 1 })
	runLiveness(t, m)
	if got := eng.listCalls(); got != lists {
		t.Errorf("vm list ran %d times with the stream live, want none", got-lists)
	}
}

func TestDaemonSyncRecoversAClaimTheSnapshotShowsDown(t *testing.T) {
	eng := newFakeEngine()
	eng.vmEvents = make(chan fakeVMEvent)
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	killVMM(eng, sb.VMName)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go m.watchVMMs(ctx)

	eng.vmEvents <- fakeVMEvent{sync: []engine.VMStatus{{Name: sb.VMName}}}

	waitFor(t, func() bool { got, _ := m.Sandbox(sb.ID); return got.Restarts == 1 })
}

func TestPollBackstopsTheDaemonStream(t *testing.T) {
	eng := newFakeEngine()
	eng.vmEvents = make(chan fakeVMEvent)
	m := newTestManager(t, eng)
	mustClaim(t, m, testKey)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go m.watchVMMs(ctx)
	eng.vmEvents <- fakeVMEvent{sync: []engine.VMStatus{}}
	waitFor(t, m.vmmEvents.Load)
	lists := eng.listCalls()

	m.lastVMPoll.Store(time.Now().Add(-2 * livenessBackstop).UnixNano())
	runLiveness(t, m)
	if got := eng.listCalls(); got != lists+1 {
		t.Errorf("vm list ran %d times past the backstop, want 1", got-lists)
	}
	close(eng.vmEvents)
	waitFor(t, func() bool { return !m.vmmEvents.Load() })
	runLiveness(t, m)
	if got := eng.listCalls(); got != lists+2 {
		t.Errorf("vm list ran %d times after the stream dropped, want the poll back", got-lists-1)
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

// assertRestartFailsUntilBudget runs recoveries against a guest setup step that always fails.
func assertRestartFailsUntilBudget(t *testing.T, m *Manager, eng *fakeEngine, sb *types.Sandbox) {
	t.Helper()
	for attempt := range vmmRestartBudget {
		m.recoverVMM(t.Context(), sb)
		got, _ := m.Sandbox(sb.ID)
		if got.Restarts != 0 {
			t.Fatalf("attempt %d published a restart %+v whose guest setup failed", attempt+1, got)
		}
		if last := attempt == vmmRestartBudget-1; (got.Failed == reasonColdBootFailed) != last {
			t.Fatalf("attempt %d: failed %q", attempt+1, got.Failed)
		}
	}
	eng.mu.Lock()
	stops := len(eng.stops)
	eng.mu.Unlock()
	if stops != vmmRestartBudget {
		t.Errorf("stops %d, want every failed restart's VM stopped", stops)
	}
	if events := usageEventsOf(t, m, sb.ID); strings.Contains(events, "vmm_restart") {
		t.Errorf("events %s record a restart that never completed", events)
	}
}

func runLiveness(t *testing.T, m *Manager) {
	t.Helper()
	m.livenessOnce(t.Context())
	waitFor(t, func() bool { return !m.livenessSweep.Load() })
}

type fakeVMEvent struct {
	sync   []engine.VMStatus
	change engine.VMChange
}

func killVMM(eng *fakeEngine, name string) {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	eng.stopped[name] = true
}
