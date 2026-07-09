package pool

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestHibernateWakeCycle(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("repeat Hibernate: %v", err)
	}
	if len(eng.hibernates) != 1 {
		t.Errorf("engine hibernated %d times, want 1 (idempotent)", len(eng.hibernates))
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d, want 1", g.Hibernated)
	}
	if got, _ := newClaimStore(m.dataDir).load(); got[sb.ID] == nil || got[sb.ID].HibernateSnap == "" {
		t.Error("hibernation flag not persisted")
	}
	if !strings.HasPrefix(eng.hibernates[0], hibernatePrefix) {
		t.Errorf("snapshot name %q, want %s prefix", eng.hibernates[0], hibernatePrefix)
	}

	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("WakeAgentSocket: %v", err)
	}
	if sock != sb.VsockSocket {
		t.Errorf("sock %q, want %q", sock, sb.VsockSocket)
	}
	if len(eng.restores) != 1 {
		t.Errorf("engine restored %d times, want 1", len(eng.restores))
	}
	if !slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Errorf("snapRemoves=%v, want consumed snapshot dropped", eng.snapRemoves)
	}
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated count %d after wake, want 0", g.Hibernated)
	}

	// Awake sandbox: the fast path must not touch the engine again.
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("fast-path WakeAgentSocket: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("fast path restored again: %d", len(eng.restores))
	}
}

func TestWakeFailureKeepsHibernated(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	eng.restoreErr = errors.New("restore boom")
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("WakeAgentSocket succeeded despite restore failure")
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d, want 1 (wake failed, snapshot kept)", g.Hibernated)
	}
	if len(eng.snapRemoves) != 0 {
		t.Errorf("snapRemoves=%v, want none after failed wake", eng.snapRemoves)
	}
}

func TestReleaseHibernatedDropsSnapshot(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	if err := m.Release(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !slices.Contains(eng.removes, sb.VMName) {
		t.Errorf("removes=%v, want %s", eng.removes, sb.VMName)
	}
	if !slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Errorf("snapRemoves=%v, want the hibernate snapshot", eng.snapRemoves)
	}
}

func TestReleaseMidHibernateDropsOrphanSnapshot(t *testing.T) {
	eng := newFakeEngine()
	eng.hibernateStall = make(chan struct{})
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	hibErr := make(chan error, 1)
	go func() { hibErr <- m.Hibernate(t.Context(), sb.ID, sb.Token) }()
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.hibernates) == 1
	})

	// Release lands while the engine transition is in flight: the claim is
	// gone before Hibernate can commit, so its snapshot must not leak.
	if err := m.Release(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	close(eng.hibernateStall)
	if err := <-hibErr; !errors.Is(err, ErrUnknownSandbox) {
		t.Fatalf("Hibernate: %v, want ErrUnknownSandbox", err)
	}
	if !slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Errorf("snapRemoves=%v, want the orphaned snapshot dropped", eng.snapRemoves)
	}
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated count %d, want 0", g.Hibernated)
	}
}

func TestReconcileAdoptsHibernated(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-hibernated-1"] = "/vsock/hib"
	eng.stopped["sbx-hibernated-1"] = true
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_hib": {
			ID: "sb_hib", VMName: "sbx-hibernated-1", Key: testKey, Token: "tok",
			HibernateSnap: hibernatePrefix + "hibernated-1",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir, config.PoolSpec{PoolKey: testKey, Warm: 1})

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if slices.Contains(eng.removes, "sbx-hibernated-1") {
		t.Error("reconcile destroyed a hibernated claim's VM")
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated count %d after reconcile, want 1", g.Hibernated)
	}
	if _, err := m.WakeAgentSocket(t.Context(), "sb_hib", "tok"); err != nil {
		t.Fatalf("wake after reconcile: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("restores=%v, want the adopted claim woken", eng.restores)
	}
}
