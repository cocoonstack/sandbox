package pool

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	interruptedVM   = "sbx-cut-1"
	interruptedHib  = hibernatePrefix + "cut-1-abc123"
	interruptedSave = forkPrefix + "cut-1-abc123"
)

func TestReconcileSettlesAnInterruptedCapture(t *testing.T) {
	for _, tt := range []struct {
		name          string
		pending, save string
		listed        []string
		wantStarts    int
		wantStops     int
		wantHibernate bool
	}{
		{name: "hibernate persisted, VMM alive", pending: interruptedHib, listed: []string{interruptedHib}, wantStops: 1, wantHibernate: true},
		{name: "hibernate never persisted", pending: interruptedHib, wantStarts: 1},
		{name: "save never adopts a listed snapshot", save: interruptedSave, listed: []string{interruptedSave}, wantStarts: 1},
		{name: "save never persisted", save: interruptedSave, wantStarts: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m, sb := reconcileInterrupted(t, eng, tt.pending, tt.save, tt.listed)
			waitSettled(t, sb)
			if want := []string{tt.pending + tt.save}; !slices.Equal(eng.captureWaits, want) {
				t.Errorf("waited on %q, want %q", eng.captureWaits, want)
			}
			if len(eng.starts) != tt.wantStarts || len(eng.stops) != tt.wantStops {
				t.Errorf("starts %q stops %q, want %d and %d", eng.starts, eng.stops, tt.wantStarts, tt.wantStops)
			}
			if got := sb.HibernateSnap == tt.pending && tt.pending != ""; got != tt.wantHibernate {
				t.Errorf("hibernate snap %q, want adopted %v", sb.HibernateSnap, tt.wantHibernate)
			}
			if sb.Failed != "" {
				t.Errorf("failed %q", sb.Failed)
			}
			if tt.wantHibernate && eng.snapRemoved(tt.pending) {
				t.Error("the only copy of guest memory was removed")
			}
			if tt.wantHibernate && !strings.Contains(usageEventsOf(t, m, sb.ID), "hibernate") {
				t.Error("the adopted hibernate was not billed")
			}
			if row := journaled(t, m)[sb.ID]; row.PendingSnap != "" || row.SaveSnap != "" {
				t.Errorf("journal keeps intents %q %q after the settle", row.PendingSnap, row.SaveSnap)
			}
		})
	}
}

func TestReconcileAdoptsAHibernateTheVMMLeftInError(t *testing.T) {
	eng := newFakeEngine()
	eng.vmStates = map[string]string{interruptedVM: "error"}
	_, sb := reconcileInterrupted(t, eng, interruptedHib, "", []string{interruptedHib})
	if sb.HibernateSnap != interruptedHib || len(eng.starts) != 0 || eng.snapRemoved(interruptedHib) {
		t.Errorf("snap %q starts %q, want the snapshot adopted, kept and no start", sb.HibernateSnap, eng.starts)
	}
}

func TestInterruptedCaptureDoesNothingUntilTheCLIExits(t *testing.T) {
	eng := newFakeEngine()
	eng.captureStall = make(chan struct{})
	m, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
	if sb.Transition.TryLock() {
		t.Fatal("the claim is free before the capture CLI exited")
	}
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.captureWaits) == 1
	})
	eng.mu.Lock()
	starts, stops := len(eng.starts), len(eng.stops)
	eng.mu.Unlock()
	if starts != 0 || stops != 0 {
		t.Errorf("%d starts and %d stops while the capture CLI may hold the VM", starts, stops)
	}
	if row := journaled(t, m)[sb.ID]; row.PendingSnap != interruptedHib {
		t.Errorf("journal intent %q, want it kept until settled", row.PendingSnap)
	}
	close(eng.captureStall)
	waitSettled(t, sb)
	if len(eng.starts) != 1 {
		t.Errorf("starts %q after the CLI exited, want one", eng.starts)
	}
}

func TestInterruptedCaptureNeverStartsAVMThatStoppedMeanwhile(t *testing.T) {
	eng := newFakeEngine()
	eng.captureHook = func() {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		eng.vmStates[interruptedVM] = "stopped"
	}
	m, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
	waitSettled(t, sb)
	if len(eng.starts) != 0 {
		t.Errorf("starts %q on a stopped VM: a silent cold boot", eng.starts)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb.PendingSnap != "" {
		t.Errorf("intent %q survived", sb.PendingSnap)
	}
}

func TestInterruptedCaptureDoesNotStopAVMThatStoppedMeanwhile(t *testing.T) {
	eng := newFakeEngine()
	eng.captureHook = func() {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		eng.vmStates[interruptedVM] = "stopped"
	}
	_, sb := reconcileInterrupted(t, eng, interruptedHib, "", []string{interruptedHib})
	waitSettled(t, sb)
	if len(eng.stops) != 0 || len(eng.starts) != 0 || sb.HibernateSnap != interruptedHib {
		t.Errorf("stops %q starts %q snap %q, want the snapshot adopted and the VM left alone", eng.stops, eng.starts, sb.HibernateSnap)
	}
}

func TestInterruptedCaptureDropsASaveSnapshotListedAfterTheSweep(t *testing.T) {
	eng := newFakeEngine()
	eng.captureHook = func() {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		eng.snapshots = append(eng.snapshots, interruptedSave)
	}
	_, sb := reconcileInterrupted(t, eng, "", interruptedSave, nil)
	waitSettled(t, sb)
	if !eng.snapRemoved(interruptedSave) {
		t.Error("the interrupted save left its snapshot behind")
	}
	if len(eng.starts) != 1 || len(eng.stops) != 0 || sb.HibernateSnap != "" {
		t.Errorf("starts %q stops %q snap %q: a save's snapshot is never the wake image", eng.starts, eng.stops, sb.HibernateSnap)
	}
}

func TestInterruptedCaptureKeepsTheIntentWhenStopFails(t *testing.T) {
	eng := newFakeEngine()
	eng.stopErr = errors.New("ops lock busy")
	m, sb := reconcileInterrupted(t, eng, interruptedHib, "", []string{interruptedHib})
	waitSettled(t, sb)
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb.PendingSnap != interruptedHib || sb.HibernateSnap != "" || eng.snapRemoved(interruptedHib) {
		t.Errorf("intent %q snap %q, want the intent and its snapshot kept, not adopted", sb.PendingSnap, sb.HibernateSnap)
	}
}

func TestAWakeAfterAFailedSettleResumesTheGuestBeforeClearingTheIntent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.snapListErr = errors.New("cocoon unreachable")
		m, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
		time.Sleep(captureResumeBudget + 2*captureResumeRetry)
		synctest.Wait()
		if row := journaled(t, m)[sb.ID]; row.PendingSnap != interruptedHib || len(eng.starts) != 0 {
			t.Fatalf("intent %q starts %q, want the intent kept after the settle gave up", row.PendingSnap, eng.starts)
		}
		eng.mu.Lock()
		eng.snapListErr = nil
		eng.mu.Unlock()
		if _, _, release, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); err != nil {
			t.Fatalf("wake: %v", err)
		} else {
			release()
		}
		if !slices.Equal(eng.starts, []string{interruptedVM}) {
			t.Errorf("starts %q, want the paused guest resumed before the intent goes", eng.starts)
		}
		if row := journaled(t, m)[sb.ID]; row.PendingSnap != "" {
			t.Errorf("intent %q kept after the guest resumed", row.PendingSnap)
		}
	})
}

func TestInterruptedCaptureRetriesWhileTheDumpRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.startFails = 3
		_, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
		time.Sleep(4 * captureResumeRetry)
		synctest.Wait()
		if len(eng.starts) != 4 || sb.Failed != "" {
			t.Errorf("starts %d failed %q, want 4 and not failed", len(eng.starts), sb.Failed)
		}
	})
}

func TestInterruptedCaptureFailsTheClaim(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*fakeEngine)
	}{
		{"past its budget", func(eng *fakeEngine) { eng.startFails = 1 << 30 }},
		{"guest silent after the start", func(eng *fakeEngine) { eng.probeErr = errors.New("never ready") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				tt.edit(eng)
				_, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
				time.Sleep(captureResumeBudget + 2*captureResumeRetry)
				synctest.Wait()
				if !strings.Contains(sb.Failed, "did not resume") {
					t.Errorf("failed %q, want the resume failure", sb.Failed)
				}
			})
		})
	}
}

func TestInterruptedCaptureKeepsItsIntentWhenStopping(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(*fakeEngine, context.CancelFunc)
	}{
		{"engine stopping", func(eng *fakeEngine, _ context.CancelFunc) { eng.startErr = engine.ErrStopping }},
		{"context canceled mid retry", func(eng *fakeEngine, cancel context.CancelFunc) {
			eng.startFails = 1 << 30
			eng.startHook = cancel
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tt.run(eng, cancel)
			m, sb := reconcileInterruptedCtx(ctx, t, eng, interruptedHib, "", nil)
			waitSettled(t, sb)
			if row := journaled(t, m)[sb.ID]; row.PendingSnap != interruptedHib || sb.Failed != "" {
				t.Errorf("journal intent %q failed %q, want the intent for the next start", row.PendingSnap, sb.Failed)
			}
		})
	}
}

func TestAFailedHibernateResumesTheGuest(t *testing.T) {
	for _, tt := range []struct {
		name      string
		listFails int
	}{
		{"list answers", 0},
		{"list fails three times", 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				m := newTestManager(t, eng)
				sb := mustClaim(t, m, testKey)
				eng.hibernateErr, eng.listFails = errors.New("signal: killed"), tt.listFails
				if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
					t.Fatal("Hibernate reported success")
				}
				time.Sleep(time.Duration(tt.listFails+1) * captureResumeRetry)
				synctest.Wait()
				if !slices.Equal(eng.starts, []string{sb.VMName}) {
					t.Errorf("starts %q, want the guest resumed", eng.starts)
				}
			})
		})
	}
}

func TestAFailedCaptureAnswersWithoutWaitingForTheGuest(t *testing.T) {
	eng := newFakeEngine()
	eng.captureStall = make(chan struct{})
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("signal: killed")
	done := make(chan error, 1)
	go func() { done <- m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Hibernate reported success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the API waited on a capture that is still exiting")
	}
	close(eng.captureStall)
	waitStarted(t, eng, 1)
	waitSettled(t, sb)
}

func TestAFailedHibernateNeverStartsAStoppedVM(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr = errors.New("signal: killed")
	eng.captureHook = func() {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		eng.stopped[sb.VMName] = true
	}
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Hibernate reported success")
	}
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.captureWaits) == 1
	})
	waitSettled(t, sb)
	if len(eng.starts) != 0 {
		t.Errorf("starts %q on a stopped VM: a silent cold boot", eng.starts)
	}
}

func TestAFailedHibernateThatPersistedStopsTheVMMAndAdopts(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.hibernateErr, eng.hibernateErrCompletes, eng.hibernateLeavesVMM = errors.New("signal: killed"), true, true
	if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}
	if len(eng.starts) != 0 || !slices.Equal(eng.stops, []string{sb.VMName}) || sb.HibernateSnap == "" {
		t.Errorf("starts %q stops %q snap %q, want the VMM stopped, the snapshot adopted and no start", eng.starts, eng.stops, sb.HibernateSnap)
	}
}

func TestAFailedCaptureKeepsItsIntentWhileSandboxdStops(t *testing.T) {
	for _, tt := range []struct {
		name    string
		capture func(context.Context, *Manager, *types.Sandbox, *fakeEngine) error
		intent  func(*types.Sandbox) string
	}{
		{"hibernate", func(ctx context.Context, m *Manager, sb *types.Sandbox, eng *fakeEngine) error {
			eng.hibernateErr = errors.New("signal: killed")
			return m.Hibernate(ctx, sb.ID, Cred{Token: sb.Token})
		}, func(row *types.Sandbox) string { return row.PendingSnap }},
		{"save", func(ctx context.Context, m *Manager, sb *types.Sandbox, eng *fakeEngine) error {
			eng.snapSaveErr = errors.New("signal: killed")
			sb.Transition.Lock()
			defer sb.Transition.Unlock()
			_, _, err := m.sourceSnap(ctx, sb)
			return err
		}, func(row *types.Sandbox) string { return row.SaveSnap }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng)
			sb := mustClaim(t, m, testKey)
			eng.startErr = engine.ErrStopping
			if err := tt.capture(t.Context(), m, sb, eng); err == nil {
				t.Fatal("the capture reported success")
			}
			waitStarted(t, eng, 1)
			waitSettled(t, sb)
			if err := m.FlushClaims(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if tt.intent(journaled(t, m)[sb.ID]) == "" || sb.Failed != "" {
				t.Errorf("journal lost the intent or failed the claim: %q", sb.Failed)
			}
		})
	}
}

func TestASuccessfulSaveClearsItsIntentOnDisk(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if _, _, err := m.sourceSnap(t.Context(), sb); err != nil {
		t.Fatalf("sourceSnap: %v", err)
	}
	if row := journaled(t, m)[sb.ID]; row.SaveSnap != "" {
		t.Errorf("journal keeps save intent %q, so every restart settles a healthy guest", row.SaveSnap)
	}
}

func TestASaveJournalsItsIntentAndAFailureResumesTheGuest(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	sb := mustClaim(t, m, testKey)
	eng.snapSaveStall = make(chan struct{})
	eng.snapSaveErr = errors.New("signal: killed")
	done := make(chan error, 1)
	go func() {
		sb.Transition.Lock()
		defer sb.Transition.Unlock()
		_, _, err := m.sourceSnap(t.Context(), sb)
		done <- err
	}()
	waitFor(t, func() bool { return journaled(t, m)[sb.ID].SaveSnap != "" })
	close(eng.snapSaveStall)
	if err := <-done; err == nil {
		t.Fatal("sourceSnap reported success")
	}
	waitStarted(t, eng, 1)
	waitFor(t, func() bool { return journaled(t, m)[sb.ID].SaveSnap == "" })
}

func TestAWakeWhoseGuestDoesNotResumeAnswersFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.snapListErr = errors.New("cocoon unreachable")
		m, sb := reconcileInterrupted(t, eng, interruptedHib, "", nil)
		time.Sleep(captureResumeBudget + 2*captureResumeRetry)
		synctest.Wait()
		eng.mu.Lock()
		eng.snapListErr, eng.probeErr = nil, errors.New("never ready")
		eng.mu.Unlock()
		if _, _, _, err := m.WakeAgentSocket(t.Context(), sb.ID, sb.Token); !errors.Is(err, ErrFailed) {
			t.Fatalf("wake: %v, want ErrFailed", err)
		}
		if err := m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}); !errors.Is(err, ErrFailed) {
			t.Errorf("hibernate: %v, want ErrFailed", err)
		}
	})
}

func TestASettleWithNoIntentLeftKeepsTheReaderSafeHibernate(t *testing.T) {
	m, sb := reconcileInterrupted(t, newFakeEngine(), "", "", nil)
	m.mu.Lock()
	sb.ReaderSafe = true
	m.mu.Unlock()
	sb.Transition.Lock()
	m.settleInterrupted(t.Context(), sb)
	if !sb.ReaderSafe {
		t.Error("a settle with nothing to settle dropped ReaderSafe")
	}
}

func TestReconcileWithoutASnapshotListSettlesAStoppedHibernate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		listed []string
		want   string
	}{
		{"snapshot persisted", []string{interruptedHib}, interruptedHib},
		{"snapshot never persisted", nil, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				eng := newFakeEngine()
				eng.vmStates = map[string]string{interruptedVM: "stopped"}
				eng.snapListErr = errors.New("cocoon unreachable")
				_, sb := reconcileInterrupted(t, eng, interruptedHib, "", tt.listed)
				eng.mu.Lock()
				eng.snapListErr = nil
				eng.mu.Unlock()
				time.Sleep(2 * captureResumeRetry)
				synctest.Wait()
				if sb.HibernateSnap != tt.want || sb.PendingSnap != "" {
					t.Errorf("hibernate snap %q intent %q, want %q and no intent", sb.HibernateSnap, sb.PendingSnap, tt.want)
				}
			})
		})
	}
}

func TestACaptureSettlesTheIntentAFailedHibernateLeft(t *testing.T) {
	for _, tt := range []struct {
		name    string
		capture func(m *Manager, sb *types.Sandbox) error
	}{
		{"fork", func(m *Manager, sb *types.Sandbox) error {
			_, err := m.Fork(t.Context(), sb.ID, Cred{Token: sb.Token}, 1, 0, "", "")
			return err
		}},
		{"checkpoint", func(m *Manager, sb *types.Sandbox) error {
			_, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "", "")
			return err
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eng := newFakeEngine()
			m := newTestManager(t, eng)
			sb := mustClaim(t, m, testKey)
			eng.mu.Lock()
			eng.snapshots = append(eng.snapshots, interruptedHib)
			eng.mu.Unlock()
			m.mu.Lock()
			sb.PendingSnap = interruptedHib
			m.mu.Unlock()
			if err := tt.capture(m, sb); err != nil {
				t.Fatalf("capture: %v", err)
			}
			if len(eng.snapSaves) != 0 || sb.HibernateSnap != interruptedHib {
				t.Errorf("saves %q hibernate snap %q, want the listed hibernate adopted and no save of the paused guest", eng.snapSaves, sb.HibernateSnap)
			}
		})
	}
}

func waitStarted(t *testing.T, eng *fakeEngine, n int) {
	t.Helper()
	waitFor(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.starts) >= n
	})
}

func waitSettled(t *testing.T, sb *types.Sandbox) {
	t.Helper()
	waitFor(t, func() bool {
		if !sb.Transition.TryLock() {
			return false
		}
		sb.Transition.Unlock()
		return true
	})
}

func reconcileInterrupted(t *testing.T, eng *fakeEngine, pending, save string, listed []string) (*Manager, *types.Sandbox) {
	t.Helper()
	return reconcileInterruptedCtx(t.Context(), t, eng, pending, save, listed)
}

func reconcileInterruptedCtx(ctx context.Context, t *testing.T, eng *fakeEngine, pending, save string, listed []string) (*Manager, *types.Sandbox) {
	t.Helper()
	eng.vms[interruptedVM] = "/vsock/cut"
	if eng.vmStates == nil {
		eng.vmStates = map[string]string{}
	}
	eng.snapshots = append(eng.snapshots, listed...)
	dataDir := t.TempDir()
	seedClaims(t, dataDir, map[string]*types.Sandbox{
		"sb_cut": {ID: "sb_cut", VMName: interruptedVM, Key: testKey, Token: "tok", VsockSocket: "/vsock/cut", PendingSnap: pending, SaveSnap: save},
	})
	m := newTestManagerAt(t, eng, dataDir)
	if err := m.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m, m.claimed["sb_cut"]
}
