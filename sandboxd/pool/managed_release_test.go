package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/archives/archivestest"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (f *fakeEngine) vmAlive(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, alive := f.vms[name]
	return alive
}

func (f *fakeEngine) captures() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.hibernates)
}

func requireManagedClaimable(t *testing.T, m *Manager, name string) {
	t.Helper()
	sb, err := m.ClaimProvision(t.Context(), testKey, managedOptions(name))
	if err != nil {
		t.Fatalf("volume %s is not claimable: %v", name, err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
}

func reopenSleepTest(t *testing.T, cfg *config.Config, e *sleepTestEngine) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), cfg, e, testSecrets(t), testBackends(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestManagedReleaseOnMetastoreNodeReturns(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	m.archives = archivestest.NewMem()
	done := make(chan error, 1)
	go func() { done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("release of a sleep-capable managed claim on a metastore node did not return")
	}
	if e.vmAlive(sb.VMName) {
		t.Fatal("release returned with the VM still present")
	}
	requireManagedClaimable(t, m, "data")
}

func TestManagedReleasePreemptsInFlightSleep(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	e.hookEntered = make(chan struct{}, 4)
	e.hookContinue = make(chan struct{})
	hibernated := make(chan error, 1)
	go func() { hibernated <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case <-e.hookEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("hibernate did not reach the prepare hook")
	}
	released := make(chan error, 1)
	go func() { released <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-released:
		if err != nil {
			close(e.hookContinue)
			t.Fatalf("release: %v", err)
		}
	case <-time.After(3 * time.Second):
		close(e.hookContinue)
		t.Fatal("release waited for the in-flight prepare hook")
	}
	alive := e.vmAlive(sb.VMName)
	close(e.hookContinue)
	if alive {
		t.Fatal("release returned with the VM still present")
	}
	if err := <-hibernated; err == nil {
		t.Fatal("a hibernate preempted by release reported success")
	}
	if n := e.captures(); n != 0 {
		t.Fatalf("%d captures followed a preempting release", n)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, managedReaderOptions("data")); !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Fatalf("ro claim after a release that preempted prepare = %v, want the dirty marker kept", err)
	}
	requireManagedClaimable(t, m, "data")
}

func managedReaderOptions(name string) ClaimOptions {
	return ClaimOptions{Tenant: "t1", Volumes: []types.Volume{{Name: name, Mode: types.VolumeModeRO, Mount: "/" + name}}}
}

func TestManagedSleepingClaimTeardownLeavesVolumeClean(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "expiry"}[expiry], func(t *testing.T) {
			m, _, sb, _ := newSleepTest(t)
			requireSleep(t, m, sb)
			if expiry {
				m.mu.Lock()
				sb.Deadline = time.Now().Add(-time.Minute)
				m.mu.Unlock()
				m.reapOnce(t.Context())
			} else if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
			reader, err := m.ClaimProvision(t.Context(), testKey, managedReaderOptions("data"))
			if err != nil {
				t.Fatalf("ro claim after tearing down a cleanly detached sleeping writer: %v", err)
			}
			if err := m.Release(t.Context(), reader.ID, Cred{Token: reader.Token}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedExpiryScheduledOnce(t *testing.T) {
	e := newManagedEngine()
	m := openManagedTest(t, managedConfig(t), e)
	createManagedTest(t, m, "data")
	managed, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Tenant: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	managed.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	// a wake, sleep or stuck device call owns the managed claim's Transition
	managed.Transition.Lock()
	for range cap(m.maintSem) {
		m.reapOnce(t.Context())
	}
	waitFor(t, func() bool { return len(m.maintSem) == cap(m.maintSem) || !e.vmAlive(managed.VMName) })
	m.mu.Lock()
	plain.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	m.reapOnce(t.Context())
	reaped := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && !reaped; time.Sleep(20 * time.Millisecond) {
		reaped = !e.vmAlive(plain.VMName)
	}
	managed.Transition.Unlock()
	waitFor(t, func() bool { return !e.vmAlive(managed.VMName) && len(m.maintSem) == 0 })
	if !reaped {
		t.Fatal("an ordinary expired claim was not reaped while one expired managed claim's Transition was held")
	}
	if c := m.Counters(); c.Reaps != 2 {
		t.Fatalf("reaps = %d over one expired managed and one expired ordinary claim, want 2", c.Reaps)
	}
	requireManagedClaimable(t, m, "data")
}

func TestFailedManagedReleaseKeepsStreamsUsable(t *testing.T) {
	m, _, sb, _ := newSleepTest(t)
	if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("a release whose claims write failed reported success")
	}
	if err := os.Remove(m.store.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	if _, err := m.AgentSocket(sb.ID, sb.Token); err != nil {
		t.Fatalf("rolled-back release refused the passive socket: %v", err)
	}
	sock, fenced, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if release != nil {
		defer release()
	}
	if err != nil || sock == "" {
		t.Fatalf("rolled-back release refused exec: socket=%q err=%v", sock, err)
	}
	select {
	case <-fenced:
		t.Fatal("rolled-back release handed out a pre-fenced stream")
	default:
	}
}

type resumeHookEngine struct {
	*sleepTestEngine
	onResume func()
}

func (e *resumeHookEngine) RunVolumeHook(ctx context.Context, sock string, call engine.VolumeHookCall) error {
	if call.Action == "resume" && e.onResume != nil {
		e.onResume()
	}
	return e.sleepTestEngine.RunVolumeHook(ctx, sock, call)
}

func TestManagedWakeFinalWriteFailureStaysAwake(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	tmp := m.store.path + ".tmp"
	m.eng = &resumeHookEngine{sleepTestEngine: e, onResume: func() { _ = os.Mkdir(tmp, 0o700) }}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("a failed final claims write after a verified mount failed the wake: %v", err)
	}
	requireExec(t, m, sb)
	if n := e.hookCalls("resume"); n != 1 {
		t.Fatalf("resume hook ran %d times, want 1", n)
	}
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := reopenSleepTest(t, cfg, e)
	requireExec(t, recovered, sb)
	if n := e.hookCalls("resume"); n != 1 {
		t.Fatalf("claims.json did not converge to awake: resume hook ran %d times after restart, want 1", n)
	}
}

func TestManagedWakeCrashBeforeClearReplaysResumeOnce(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	requireSleep(t, m, sb)
	files := []string{m.store.path, filepath.Join(cfg.ManagedVolumeDir, "registry.json")}
	saved := map[string][]byte{}
	m.eng = &resumeHookEngine{sleepTestEngine: e, onResume: func() {
		for _, path := range files {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Error(err)
			}
			saved[path] = raw
		}
	}}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	for path, raw := range saved {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recovered := reopenSleepTest(t, cfg, e)
	requireExec(t, recovered, sb)
	if n := e.hookCalls("resume"); n != 2 {
		t.Fatalf("resume hook ran %d times across the crash, want one replay (2)", n)
	}
	if len(e.restores) != 1 {
		t.Fatalf("restores = %v, want the original restore only", e.restores)
	}
}
