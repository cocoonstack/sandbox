package pool

import (
	"context"
	"errors"
	"net"
	"os"
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
			if len(e.hooks) != 0 || len(e.snapshots) != 0 || !slices.Equal(before, e.volumeOps) {
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
			heldDuringTeardown := volumeHoldersOf(m, "data") != (volumeHolders{})
			close(gate.resume)
			if !expiry {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
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

func TestManagedSleepReleasePersistenceFailureKeepsGuestAndHold(t *testing.T) {
	m, e, sb, _ := newSleepTest(t)
	before := slices.Clone(e.volumeOps)
	if err := os.Mkdir(m.store.path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("failed durable deletion reported success")
	}
	if err := os.Remove(m.store.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Sandbox(sb.ID); !ok || !slices.Equal(before, e.volumeOps) {
		t.Fatal("failed durable deletion dropped claim or touched guest")
	}
	_, err := m.ClaimProvision(t.Context(), testKey, managedOptions("data"))
	requireVolumeStatus(t, err, 409)
	m.managedSleepHook = nil
	sock, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if release != nil {
		release()
	}
	if err != nil || sock == "" {
		t.Fatalf("failed pre-rename deletion must leave the active guest available: socket=%q err=%v", sock, err)
	}
	waitFor(t, func() bool { return !m.store.recommitting.Load() && m.store.synced() })
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
