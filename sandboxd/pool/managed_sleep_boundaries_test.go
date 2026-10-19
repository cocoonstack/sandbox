package pool

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
)

func TestManagedSleepFailedClaimRefusesDataPlane(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			if !enabled {
				m.managedSleepHook = nil
			}
			e.mu.Lock()
			e.stopped[sb.VMName] = true
			e.mu.Unlock()
			m.recoverVMM(t.Context(), sb)
			if sb.Failed == "" {
				t.Fatal("missing VMM did not fail the mounted claim")
			}
			sock, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
			if release != nil {
				release()
			}
			if !errors.Is(err, ErrFailed) || sock != "" {
				t.Fatalf("failed claim exposed data plane: socket=%q err=%v", sock, err)
			}
		})
	}
}

func TestManagedSleepOptInPreservesRunningEgress(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			cfg := managedConfig(t)
			cfg.Networks = []string{"test-network"}
			if enabled {
				cfg.ManagedVolumeHibernate = &config.ManagedVolumeHibernate{HookArgv: []string{"/hook"}, HookTimeoutSeconds: 1}
			}
			e := newManagedEngine()
			m := openManagedTest(t, cfg, e)
			createManagedTest(t, m, "data")
			opts := managedOptions("data")
			opts.Volumes[0].AttachOnly = false
			sb, err := m.ClaimProvision(t.Context(), egKey, opts)
			if err != nil {
				t.Fatal(err)
			}
			sock, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
			if release != nil {
				release()
			}
			if err != nil || sock != sb.VsockSocket {
				t.Fatalf("running egress claim lost data plane: socket=%q err=%v", sock, err)
			}
		})
	}
}

func TestManagedSleepRefusesEffectiveOutboundBeforeEffects(t *testing.T) {
	for _, armed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "working proxy"}[armed], func(t *testing.T) {
			m, e, sb, _ := newSleepTest(t)
			editEgress(t, m, func(cfg *config.Config) {
				cfg.Pools[0].Egress = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
				cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: cfg.Pools[0].Egress}}
			})
			sb.EgressClass = "desk"
			sock := filepath.Join(outboundtest.SockRoot(t), "v")
			sb.VsockSocket = sock
			e.mu.Lock()
			e.vms[sb.VMName] = sock
			e.mu.Unlock()
			if armed {
				if err := m.out.ArmProxy(t.Context(), sb); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { m.out.Disarm(sb.ID, false) })
				requireProxyDoor(t, sock)
			}
			before := slices.Clone(e.volumeOps)
			if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("effective outbound policy accepted for sleep: %v", err)
			}
			if armed {
				requireProxyDoor(t, sock)
			}
			h, _ := m.managedHold(sb.ID)
			if h.Sleep != nil || len(e.hooks) != 0 || len(e.snapshots) != 0 || !slices.Equal(before, e.volumeOps) {
				t.Fatal("outbound refusal had lifecycle effects")
			}
			got, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
			if release != nil {
				release()
			}
			if err != nil || got != sock {
				t.Fatalf("outbound refusal lost running guest: socket=%q err=%v", got, err)
			}
		})
	}
}

func requireProxyDoor(t *testing.T, sock string) {
	t.Helper()
	c, err := net.DialTimeout("unix", engine.EgressSocketPath(sock), time.Second)
	if err != nil {
		t.Fatalf("outbound proxy unavailable: %v", err)
	}
	c.Close()
}

func TestManagedSleepPolicyReloadKeepsWakePaused(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	sb.EgressClass = "desk"
	requireSleep(t, m, sb)
	next := *cfg
	next.Pools = slices.Clone(cfg.Pools)
	next.Pools[0].Egress = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	next.EgressClasses = []config.EgressClass{{Name: "desk", Egress: next.Pools[0].Egress}}
	if _, err := m.ReloadConfig(t.Context(), &next); err != nil {
		t.Fatal(err)
	}
	before := slices.Clone(e.volumeOps)
	hooks := len(e.hooks)
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("wake accepted new outbound policy: %v", err)
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("policy-blocked wake exposed guest: %v", err)
	}
	if len(e.restores) != 0 || len(e.hooks) != hooks || !slices.Equal(before, e.volumeOps) {
		t.Fatal("policy-blocked wake touched guest")
	}
	cleared := next
	cleared.EgressClasses = nil
	if _, err := m.ReloadConfig(t.Context(), &cleared); err != nil {
		t.Fatal(err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
}

type sleepUnmountGate struct {
	*sleepTestEngine
	entered, resume chan struct{}
}

func (e *sleepUnmountGate) UnmountVolume(ctx context.Context, sock, mount string) error {
	close(e.entered)
	<-e.resume
	return e.sleepTestEngine.UnmountVolume(ctx, sock, mount)
}

func TestManagedSleepTeardownRefusesRenewal(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "expiry"}[expiry], func(t *testing.T) {
			m, e, sb, cfg := newSleepTest(t)
			gate := &sleepUnmountGate{sleepTestEngine: e, entered: make(chan struct{}), resume: make(chan struct{})}
			m.eng = gate
			done := make(chan error, 1)
			if expiry {
				m.mu.Lock()
				sb.Deadline = time.Now().Add(-time.Minute)
				m.mu.Unlock()
				m.reapOnce(t.Context())
			} else {
				go func() { done <- m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
			}
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				close(gate.resume)
				t.Fatal("release did not reach teardown")
			}
			_, renewErr := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, "")
			rows, loadErr := newClaimStore(cfg.DataDir, true).load()
			_, heldDuringTeardown := m.managedHold(sb.ID)
			close(gate.resume)
			if !expiry {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { _, held := m.managedHold(sb.ID); return !held })
			if !errors.Is(renewErr, ErrUnknownSandbox) {
				t.Errorf("teardown accepted renewal it discarded: %v", renewErr)
			}
			if loadErr != nil || rows[sb.ID] != nil {
				t.Errorf("claim still durable during guest teardown: rows=%v err=%v", rows, loadErr)
			}
			if !heldDuringTeardown {
				t.Fatal("backing ownership freed before guest teardown")
			}
		})
	}
}

func TestManagedSleepExpiryHonorsRenewalWhileTransitionBusy(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	sb.Transition.Lock()
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Minute)
	m.mu.Unlock()
	before := slices.Clone(e.volumeOps)
	m.reapOnce(t.Context())
	waitFor(t, func() bool { return len(m.maintSem) > 0 })
	deadline, err := m.Renew(t.Context(), sb.ID, Cred{Token: sb.Token}, time.Hour, "")
	sb.Transition.Unlock()
	waitFor(t, func() bool { return len(m.maintSem) == 0 })
	if err != nil {
		t.Fatal(err)
	}
	rows, err := newClaimStore(cfg.DataDir, true).load()
	if err != nil || rows[sb.ID] == nil || !rows[sb.ID].Deadline.Equal(deadline) {
		t.Fatalf("expiry discarded accepted renewal: rows=%v err=%v", rows, err)
	}
	if _, ok := m.Sandbox(sb.ID); !ok || !slices.Equal(before, e.volumeOps) {
		t.Fatal("expiry tore down renewed claim")
	}
}

func TestManagedSleepReleasePersistenceFailureKeepsGuestAndHold(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	before := slices.Clone(e.volumeOps)
	path := m.store.path
	m.store.path = filepath.Join(t.TempDir(), "missing", "claims.json")
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("failed durable deletion reported success")
	}
	m.store.path = path
	if _, ok := m.Sandbox(sb.ID); !ok || !slices.Equal(before, e.volumeOps) {
		t.Fatal("failed durable deletion dropped claim or touched guest")
	}
	if _, held := m.managedHold(sb.ID); !held {
		t.Fatal("failed durable deletion freed backing ownership")
	}
	m.managedSleepHook = nil
	sock, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if release != nil {
		release()
	}
	if err == nil || sock != "" {
		t.Fatalf("retiring claim exposed guest with feature disabled: socket=%q err=%v", sock, err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSleepAllowsIneffectiveOutboundPolicy(t *testing.T) {
	for _, noEgress := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing tenant policy", true: "no egress claim"}[noEgress], func(t *testing.T) {
			m, _, sb, _ := newSleepTest(t)
			editEgress(t, m, func(cfg *config.Config) {
				cfg.Pools[0].Egress = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
				if noEgress {
					cfg.EgressClasses = []config.EgressClass{{Name: "desk", Egress: cfg.Pools[0].Egress}}
				}
			})
			sb.EgressClass, sb.NoEgress = "desk", noEgress
			requireSleep(t, m, sb)
			if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedSleepPolicyReloadDuringResumeKeepsGuestPaused(t *testing.T) {
	m, e, sb, cfg := newSleepTest(t)
	sb.EgressClass = "desk"
	requireSleep(t, m, sb)
	e.hookEntered = make(chan struct{}, 1)
	e.hookContinue = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case <-e.hookEntered:
	case <-time.After(time.Second):
		close(e.hookContinue)
		t.Fatal("wake did not reach resume hook")
	}
	next := *cfg
	next.Pools = slices.Clone(cfg.Pools)
	next.Pools[0].Egress = &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}
	next.EgressClasses = []config.EgressClass{{Name: "desk", Egress: next.Pools[0].Egress}}
	_, reloadErr := m.ReloadConfig(t.Context(), &next)
	close(e.hookContinue)
	wakeErr := <-done
	if reloadErr != nil {
		t.Fatal(reloadErr)
	}
	if !errors.Is(wakeErr, ErrVolumeCapture) {
		t.Fatalf("resume published readiness after policy reload: %v", wakeErr)
	}
	if _, err := m.AgentSocket(sb.ID, sb.Token); !errors.Is(err, ErrPaused) {
		t.Fatalf("policy-blocked resume exposed guest: %v", err)
	}
	cleared := next
	cleared.EgressClasses = nil
	if _, err := m.ReloadConfig(t.Context(), &cleared); err != nil {
		t.Fatal(err)
	}
	if err := m.Wake(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if len(e.restores) != 1 {
		t.Fatal("policy retry replayed memory restore")
	}
}
