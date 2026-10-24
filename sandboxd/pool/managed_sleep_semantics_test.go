package pool

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type missingHookEngine struct{ *sleepTestEngine }

func (e *missingHookEngine) RunVolumeHook(ctx context.Context, sock string, call engine.VolumeHookCall) error {
	_ = e.sleepTestEngine.RunVolumeHook(ctx, sock, call)
	return &engine.VolumeHookExit{Code: 127, Output: "/usr/local/bin/volume-hook: not found"}
}

func (e *sleepTestEngine) hookCalls(action string) int {
	e.sleepMu.Lock()
	defer e.sleepMu.Unlock()
	n := 0
	for _, call := range e.hooks {
		if call.Action == action {
			n++
		}
	}
	return n
}

func requireExec(t *testing.T, m *Manager, sb *types.Sandbox) {
	t.Helper()
	sock, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if release != nil {
		release()
	}
	if err != nil || sock == "" {
		t.Fatalf("exec refused: socket=%q err=%v", sock, err)
	}
}

func TestManagedHibernatePrepareFailureKeepsClaimRunning(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	m.eng = &missingHookEngine{sleepTestEngine: e}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("hibernate with a missing guest hook reported success")
	}
	if len(e.hibernates) != 0 {
		t.Fatalf("captured after a failed prepare hook: %v", e.hibernates)
	}
	resumes := e.hookCalls("resume")
	requireExec(t, m, sb)
	if got := e.hookCalls("resume"); got != resumes {
		t.Fatalf("exec after a failed hibernate re-ran the resume hook (%d -> %d)", resumes, got)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("second hibernate with a missing guest hook reported success")
	}
	if got := e.hookCalls("prepare"); got != 2 {
		t.Fatalf("prepare ran %d times over two hibernates, want 2", got)
	}
	requireExec(t, m, sb)
}

func TestManagedWakeSurvivesFailedResumeHook(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	e.hookErr = "resume"
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("a failed resume hook after a verified mount failed the wake: %v", err)
	}
	requireExec(t, m, sb)
}

func TestManagedWakeIgnoresOutboundPolicyChange(t *testing.T) {
	m, _, sb, cfg := newSleepTest(t)
	sb.EgressClass = "desk"
	requireSleep(t, m, sb)
	next := *cfg
	next.Pools = slices.Clone(cfg.Pools)
	next.Pools[0].Egress = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	next.EgressClasses = []config.EgressClass{{Name: "desk", Egress: next.Pools[0].Egress}}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatal(err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("an outbound policy published while asleep stranded the claim: %v", err)
	}
	requireExec(t, m, sb)
}

// failedRestoreOnce models cocoon quarantining a partially populated restore as state "error"; a later restore from "error" succeeds.
type failedRestoreOnce struct {
	*sleepTestEngine
	failed, errState bool
}

func (e *failedRestoreOnce) Restore(ctx context.Context, name, snap string) (string, error) {
	if !e.failed {
		e.failed, e.errState = true, true
		e.mu.Lock()
		e.restores = append(e.restores, name)
		e.mu.Unlock()
		e.sleepMu.Lock()
		e.generation++
		e.reason = "error"
		e.sleepMu.Unlock()
		return "", errors.New("restore: populate failed")
	}
	e.errState = false
	return e.sleepTestEngine.Restore(ctx, name, snap)
}

func (e *failedRestoreOnce) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	rec, found, err := e.sleepTestEngine.Inspect(ctx, name)
	if e.errState {
		rec.State = "error"
	}
	return rec, found, err
}

func TestManagedWakeRetriesFailedRestore(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	m.eng = &failedRestoreOnce{sleepTestEngine: e}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("a failed restore reported success")
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("a failed restore did not leave the claim hibernated: %v", err)
	}
	requireExec(t, m, sb)
	if len(e.restores) != 2 {
		t.Fatalf("restores = %v, want the failed attempt and one retry", e.restores)
	}
}

// generationGapEngine advances cocoon's transition generation by more than one per hibernate and restore.
type generationGapEngine struct{ *sleepTestEngine }

func (e *generationGapEngine) Hibernate(ctx context.Context, name, snap string) error {
	err := e.sleepTestEngine.Hibernate(ctx, name, snap)
	e.sleepMu.Lock()
	e.generation += 3
	e.sleepMu.Unlock()
	return err
}

func (e *generationGapEngine) Restore(ctx context.Context, name, snap string) (string, error) {
	sock, err := e.sleepTestEngine.Restore(ctx, name, snap)
	e.sleepMu.Lock()
	e.generation += 5
	e.sleepMu.Unlock()
	return sock, err
}

func TestManagedSleepToleratesGenerationGaps(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	m.eng = &generationGapEngine{sleepTestEngine: e}
	requireSleep(t, m, sb)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("wake refused a restored VM whose generation skipped values: %v", err)
	}
	requireExec(t, m, sb)
}

type liveVolumeFloorEngine struct{ *sleepTestEngine }

func (e *liveVolumeFloorEngine) CheckLiveVolumeSupport(context.Context) error {
	return &engine.LiveVolumeUnsupported{Version: "v0.7.1"}
}

func TestManagedWakeRefusesBelowLiveVolumeFloor(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	m.eng = &liveVolumeFloorEngine{sleepTestEngine: e}
	err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token})
	if _, ok := errors.AsType[*engine.LiveVolumeUnsupported](err); !ok || len(e.restores) != 0 {
		t.Fatalf("wake below the live volume floor: err=%v restores=%d", err, len(e.restores))
	}
}

func TestManagedWakeRecordsWakeLatency(t *testing.T) {
	m, _, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if c := m.Counters(); c.Wakes != 1 || c.WakeNanos == 0 {
		t.Fatalf("managed wake counted wakes=%d wake_nanos=%d", c.Wakes, c.WakeNanos)
	}
}

func TestManagedSleepHookFailureReportsTheHook(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	e.hookErr = "prepare"
	err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
	ve := requireVolumeStatus(t, err, 409)
	if ve.Code != "volume_hook_failed" || !strings.Contains(ve.Message, "prepare") || !strings.Contains(ve.Message, "hook failed") {
		t.Fatalf("failed prepare hook = %+v, want 409 volume_hook_failed naming the action and the hook output", ve)
	}
}

type hookTransportFailEngine struct{ *sleepTestEngine }

func (e *hookTransportFailEngine) RunVolumeHook(context.Context, string, engine.VolumeHookCall) error {
	return errors.New("dial unix /var/lib/cocoon/run/cloudhypervisor/abc/vsock.sock: connect: connection refused")
}

func TestManagedSleepHookTransportFailureStaysPrivate(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	m.eng = &hookTransportFailEngine{sleepTestEngine: e}
	err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token})
	if err == nil {
		t.Fatal("hibernate over an unreachable guest hook reported success")
	}
	if v, ok := errors.AsType[*VolumeError](err); ok && (v.Code == "volume_hook_failed" || strings.Contains(v.Message, "/var/lib/cocoon")) {
		t.Fatalf("host transport failure reached the tenant as %d %s %q", v.Status, v.Code, v.Message)
	}
}

func TestManagedWakeOverLostBackingNeedsRecovery(t *testing.T) {
	m, _, sb, _ := newSleepTest(t)
	requireSleep(t, m, sb)
	path, _ := m.managedPath("data")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ve := requireVolumeStatus(t, m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}), 503)
	if ve.Code != "volume_recovery_required" {
		t.Fatalf("wake over a lost backing = %+v, want volume_recovery_required, which carries no Retry-After", ve)
	}
	m.mu.Lock()
	got := m.claimed[sb.ID]
	m.mu.Unlock()
	if got == nil || got.VolumeSleep == nil {
		t.Fatal("a refused wake must leave the claim hibernated")
	}
}
