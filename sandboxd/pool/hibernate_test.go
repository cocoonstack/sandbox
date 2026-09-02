package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestHibernatePersistFailureSurfacesAndConverges(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	broken := filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.path = broken

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success despite a persist failure")
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate retry reported success while the journal is unwritable")
	}
	if len(eng.hibernates) != 0 {
		t.Fatalf("engine hibernated %d times before the intent could persist, want 0", len(eng.hibernates))
	}
	if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate retry after heal: %v", err)
	}
	got, err := (&claimStore{path: broken}).load()
	if err != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap == "" || got[sb.ID].PendingSnap != "" {
		t.Fatalf("journal after healed retry: %+v, %v", got[sb.ID], err)
	}
}

func TestWakePersistFailureSurfaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("Hibernate: %v", err)
		}
		snap := eng.hibernates[0]
		broken := filepath.Join(t.TempDir(), "gone", "claims.json")
		m.store.path = broken

		if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
			t.Fatal("wake reported success despite a persist failure")
		}
		if eng.snapRemoved(snap) {
			t.Error("wake dropped the snapshot the lagging journal references")
		}
		if sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil || sock == "" {
			t.Fatalf("fast-path wake of the awake VM: %q, %v", sock, err)
		}
		if eng.snapRemoved(snap) {
			t.Error("fast path dropped the snapshot while the journal still lags")
		}
		if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
			t.Fatalf("heal store dir: %v", err)
		}

		waitFor(t, func() bool {
			if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
				t.Fatalf("fast-path wake: %v", err)
			}
			return eng.snapRemoved(snap)
		})
		got, loadErr := (&claimStore{path: broken}).load()
		if loadErr != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" {
			t.Fatalf("journal after converge: %+v, %v", got[sb.ID], loadErr)
		}
	})
}

func TestHibernateAmbiguousErrorTreatedAsDone(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("cli timeout")
	eng.hibernateErrCompletes = true

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v, want nil (the snapshot landed despite the error)", err)
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Errorf("hibernated %d, want 1", g.Hibernated)
	}
	got, _ := newClaimStore(m.dataDir).load()
	if got[sb.ID] == nil || got[sb.ID].HibernateSnap == "" || got[sb.ID].PendingSnap != "" {
		t.Fatalf("journal %+v, want HibernateSnap set + PendingSnap clear", got[sb.ID])
	}
}

func TestHibernateVerifiedFailureClearsIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("cli failed")

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success despite a verified failure")
	}
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated %d, want 0 (still running)", g.Hibernated)
	}
	got, _ := newClaimStore(m.dataDir).load()
	if got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" || got[sb.ID].PendingSnap != "" {
		t.Fatalf("journal %+v, want running (no snaps)", got[sb.ID])
	}
	eng.hibernateErr = nil
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate retry: %v", err)
	}
}

func TestHibernateUnverifiableErrorKeepsIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("cli timeout")
	eng.snapListErr = errors.New("cocoon unreachable")

	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success on an unconfirmed transition")
	}
	if _, g := m.Info(); g.Hibernated != 0 {
		t.Errorf("hibernated %d, want 0 (unconfirmed, not committed)", g.Hibernated)
	}
	got, _ := newClaimStore(m.dataDir).load()
	if got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" || got[sb.ID].PendingSnap == "" {
		t.Fatalf("journal %+v, want the intent kept for Reconcile", got[sb.ID])
	}
}

func TestWakeRestoreErrorDestroysAndRetries(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	snap := eng.hibernates[0]
	eng.restoreErr = errors.New("restore timeout")

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake reported success despite a restore error")
	}
	if !eng.removed(sb.VMName) {
		t.Error("restore error left a possibly-booted VM behind")
	}
	if eng.snapRemoved(snap) {
		t.Error("restore error dropped the snapshot needed to retry")
	}
	eng.restoreErr = nil
	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil || sock == "" {
		t.Fatalf("wake retry after restore recovered: %q, %v", sock, err)
	}
	if len(eng.restores) != 2 {
		t.Errorf("restores=%d, want 2 (failed attempt + retry)", len(eng.restores))
	}
}

func TestRetryResolvesDanglingIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)

	eng.hibernateErr = errors.New("cli timeout")
	eng.hibernateErrCompletes = true
	eng.snapListErr = errors.New("cocoon unreachable")
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success on an unconfirmed transition")
	}
	realSnap := eng.hibernates[0]
	if got, _ := newClaimStore(m.dataDir).load(); got[sb.ID].PendingSnap != realSnap {
		t.Fatalf("intent %q, want the real snapshot %q kept", got[sb.ID].PendingSnap, realSnap)
	}

	eng.hibernateErr = nil
	eng.snapListErr = nil
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate retry: %v", err)
	}
	if len(eng.hibernates) != 1 {
		t.Errorf("engine hibernated %d times, want 1 (retry adopted the existing snapshot)", len(eng.hibernates))
	}
	got, _ := newClaimStore(m.dataDir).load()
	if got[sb.ID].HibernateSnap != realSnap || got[sb.ID].PendingSnap != "" {
		t.Fatalf("journal %+v, want HibernateSnap=%q + no pending", got[sb.ID], realSnap)
	}
	if n := m.Counters().Hibernates; n != 1 {
		t.Errorf("hibernates billed=%d, want 1 (the adopted transition is recorded)", n)
	}
	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
		t.Fatalf("wake after resolve: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("restores=%d, want 1 (restored from the adopted snapshot)", len(eng.restores))
	}
}

func TestWakeResolvesDanglingIntent(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("cli timeout")
	eng.hibernateErrCompletes = true
	eng.snapListErr = errors.New("cocoon unreachable")
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success on an unconfirmed transition")
	}

	eng.hibernateErr = nil
	eng.snapListErr = nil
	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil {
		t.Fatalf("wake: %v", err)
	}
	if len(eng.restores) != 1 {
		t.Errorf("restores=%d, want 1 (the intent resolved to hibernated and restored)", len(eng.restores))
	}
	if sock == "" {
		t.Error("wake returned an empty socket")
	}
	if n := m.Counters().Hibernates; n != 1 {
		t.Errorf("hibernates billed=%d, want 1 (adopt records the hibernate before the wake)", n)
	}
}

func TestResolvePendingSnapReleaseRaceDropsOrphan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)

		eng.hibernateErr = errors.New("cli timeout")
		eng.hibernateErrCompletes = true
		eng.snapListErr = errors.New("cocoon unreachable")
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
			t.Fatal("Hibernate reported success on an unconfirmed transition")
		}
		realSnap := eng.hibernates[0]

		eng.snapListErr = nil
		eng.snapListStall = make(chan struct{})
		callsBefore := eng.snapListCalls()
		wakeErr := make(chan error, 1)
		go func() {
			_, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
			wakeErr <- err
		}()
		waitFor(t, func() bool { return eng.snapListCalls() > callsBefore })
		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("release: %v", err)
		}
		close(eng.snapListStall)

		if err := <-wakeErr; !errors.Is(err, ErrUnknownSandbox) {
			t.Fatalf("wake: %v, want ErrUnknownSandbox", err)
		}
		waitFor(t, func() bool { return eng.snapRemoved(realSnap) })
	})
}

func TestResolveAdoptBillsWhenPersistFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		eng.hibernateErr = errors.New("cli timeout")
		eng.hibernateErrCompletes = true
		eng.snapListErr = errors.New("cocoon unreachable")
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
			t.Fatal("Hibernate reported success on an unconfirmed transition")
		}

		eng.snapListErr = nil
		broken := filepath.Join(t.TempDir(), "gone", "claims.json")
		m.store.path = broken
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
			t.Fatal("Hibernate reported success despite a persist failure")
		}
		if n := m.Counters().Hibernates; n != 1 {
			t.Errorf("hibernates billed=%d, want 1 (billed even while the persist lags)", n)
		}

		if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
			t.Fatalf("heal store dir: %v", err)
		}
		waitFor(t, func() bool {
			got, err := (&claimStore{path: broken}).load()
			return err == nil && got[sb.ID] != nil && got[sb.ID].HibernateSnap != ""
		})
	})
}

func TestWakeProbeFailureDestroysAndRetries(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	snap := eng.hibernates[0]
	eng.probeErr = errors.New("probe timeout")

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake reported success despite a probe timeout")
	}
	if !eng.removed(sb.VMName) {
		t.Error("probe failure left the restored VM running")
	}
	if eng.snapRemoved(snap) {
		t.Error("probe failure dropped the snapshot needed to retry")
	}
	eng.probeErr = nil
	sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
	if err != nil || sock == "" {
		t.Fatalf("wake retry after the probe recovered: %q, %v", sock, err)
	}
	if len(eng.restores) != 2 {
		t.Errorf("restores=%d, want 2 (failed attempt + retry)", len(eng.restores))
	}
}

func TestFlushClaimsClosesShutdownWindow(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	broken := filepath.Join(t.TempDir(), "gone", "claims.json")
	m.store.path = broken

	if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err == nil {
		t.Fatal("wake reported success despite a persist failure")
	}
	if err := os.MkdirAll(filepath.Dir(broken), 0o750); err != nil {
		t.Fatalf("heal store dir: %v", err)
	}
	if err := m.FlushClaims(); err != nil {
		t.Fatalf("FlushClaims: %v", err)
	}
	got, err := (&claimStore{path: broken}).load()
	if err != nil || got[sb.ID] == nil || got[sb.ID].HibernateSnap != "" {
		t.Fatalf("journal after flush: %+v, %v", got[sb.ID], err)
	}
}

func TestHibernateWakeCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)

		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("Hibernate: %v", err)
		}
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
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
		hibSnap := eng.hibernates[0]
		if !strings.HasPrefix(hibSnap, hibernatePrefix) {
			t.Errorf("snapshot name %q, want %s prefix", hibSnap, hibernatePrefix)
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
		waitFor(t, func() bool { return eng.snapRemoved(hibSnap) })
		if _, g := m.Info(); g.Hibernated != 0 {
			t.Errorf("hibernated count %d after wake, want 0", g.Hibernated)
		}

		if _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Fatalf("fast-path WakeAgentSocket: %v", err)
		}
		if len(eng.restores) != 1 {
			t.Errorf("fast path restored again: %d", len(eng.restores))
		}
	})
}

func TestWakeDropsSnapshotOffReturnPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
			t.Fatalf("hibernate: %v", err)
		}
		eng.snapRemoveStall = make(chan struct{})

		sock, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token)
		if err != nil {
			t.Fatalf("wake: %v", err)
		}
		if sock == "" {
			t.Error("wake returned an empty socket")
		}
		if n := eng.snapRemoveCount(); n != 0 {
			t.Errorf("snapshot removed %d times before wake returned, want it off the return path", n)
		}
		close(eng.snapRemoveStall)
		waitFor(t, func() bool { return eng.snapRemoveCount() == 1 })
	})
}

func TestWakeFailureKeepsHibernated(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
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
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
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
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.hibernateStall = make(chan struct{})
		m := newTestManager(t, eng)
		sb := mustClaim(t, m, testKey)

		hibErr := make(chan error, 1)
		go func() { hibErr <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
		waitFor(t, func() bool {
			eng.mu.Lock()
			defer eng.mu.Unlock()
			return len(eng.hibernates) == 1
		})

		if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
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
	})
}

func TestReconcileAdoptsJournaledIntent(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-lagged-1"] = "/vsock/lagged"
	eng.stopped["sbx-lagged-1"] = true
	eng.snapshots = append(eng.snapshots, hibernatePrefix+"lagged-1-abc123")
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_lag": {
			ID: "sb_lag", VMName: "sbx-lagged-1", Key: testKey, Token: "tok",
			VsockSocket: "/vsock/lagged", PendingSnap: hibernatePrefix + "lagged-1-abc123",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, g := m.Info(); g.Hibernated != 1 {
		t.Fatalf("hibernated count %d after reconcile, want the intent adopted", g.Hibernated)
	}
	if eng.snapRemoved(hibernatePrefix + "lagged-1-abc123") {
		t.Error("reconcile swept the wake image it adopted")
	}
	if _, err := m.WakeAgentSocket(t.Context(), "sb_lag", "tok"); err != nil {
		t.Fatalf("wake adopted claim: %v", err)
	}
}

func TestReconcileIgnoresStaleOrphanSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.vms["sbx-orphan-1"] = "/vsock/orphan"
		eng.stopped["sbx-orphan-1"] = true
		eng.snapshots = append(eng.snapshots, hibernatePrefix+"orphan-1-old111")
		dataDir := t.TempDir()
		claims := map[string]*types.Sandbox{
			"sb_orp": {ID: "sb_orp", VMName: "sbx-orphan-1", Key: testKey, Token: "tok", VsockSocket: "/vsock/orphan"},
		}
		if err := newClaimStore(dataDir).save(claims); err != nil {
			t.Fatalf("setup: %v", err)
		}
		m := newTestManagerAt(t, eng, dataDir)

		if err := m.Reconcile(t.Context()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if _, g := m.Info(); g.Claimed != 0 {
			t.Errorf("claimed %d, want the intent-less claim dropped", g.Claimed)
		}
		waitFor(t, func() bool { return eng.snapRemoved(hibernatePrefix + "orphan-1-old111") })
	})
}

func TestReconcileDropsIntentWithoutImage(t *testing.T) {
	eng := newFakeEngine()
	eng.vms["sbx-noimg-1"] = "/vsock/noimg"
	eng.stopped["sbx-noimg-1"] = true
	dataDir := t.TempDir()
	claims := map[string]*types.Sandbox{
		"sb_no": {
			ID: "sb_no", VMName: "sbx-noimg-1", Key: testKey, Token: "tok",
			VsockSocket: "/vsock/noimg", PendingSnap: hibernatePrefix + "noimg-1-abc123",
		},
	}
	if err := newClaimStore(dataDir).save(claims); err != nil {
		t.Fatalf("setup: %v", err)
	}
	m := newTestManagerAt(t, eng, dataDir)

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, g := m.Info(); g.Claimed != 0 {
		t.Errorf("claimed %d, want the imageless intent dropped", g.Claimed)
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
