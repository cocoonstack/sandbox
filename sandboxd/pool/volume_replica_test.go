package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveWriterConvergesThroughReplicaScans(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			m, c := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			if pending {
				m.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
				if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
					t.Fatal(err)
				}
				m.archives = c.reg
			} else {
				replicateNow(t, m, sb)
			}
			writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
			var view types.SandboxVolumesResponse
			var err error
			for cycle := range 5 {
				view, err = m.SetVolumes(t.Context(), sb.ID, "", writer)
				if err == nil {
					if len(view.Volumes) != 1 || sb.HasReplica() {
						t.Fatalf("writer exposed alongside replica: %+v, replica=%t", view, sb.HasReplica())
					}
				}
				t.Logf("cycle %d: wanted=%+v applied=%+v states=%+v replica=%t synced=%t error=%v", cycle, view.VolumesWant, view.Volumes, view.States, sb.HasReplica(), m.store.synced(), err)
				m.replicas = map[string]replicaState{sb.ID: {tried: time.Now().Add(-2 * time.Duration(replicateEvery) * time.Second)}}
				m.replicateOnce(t.Context())
				waitFor(t, func() bool { return !m.replicating.Load() })
			}
			if err != nil {
				t.Fatalf("writer never converged after five replica scans: %+v, %v", view, err)
			}
		})
	}
}

func TestReplicaRetirementKeepsTombstoneUntilClaimCommit(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	ck := sb.ReplicaCk
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected claims write failure") }
	sb.Transition.Lock()
	err := m.dropReplica(t.Context(), sb)
	sb.Transition.Unlock()
	if err == nil {
		t.Error("retirement acknowledged a failed claim commit")
	}
	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || !rec.Consumed || !ckExists(t, m, ck) {
		t.Fatalf("failed commit lost recovery tombstone/checkpoint: %+v, exists=%t, err=%v", rec, ok, err)
	}
	m.reconcileOwnRow(t.Context(), rec, time.Now())
	if rec, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok || !rec.Consumed {
		t.Fatalf("sweep erased unacknowledged retirement: %+v, exists=%t, err=%v", rec, ok, err)
	}
	restarted := c.restart(t, m)
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("restart dropped a claim after failed retirement commit")
	}
	restarted.retireReplica(t.Context(), restored)
	if restored.HasReplica() {
		t.Fatal("restart did not finish retirement")
	}
}

func TestReplicaRecoveryDoesNotRepublishVolumeIntent(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	// Reconstruct the durable state a pending writer leaves before replica retirement.
	sb.VolumesWant = []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if err := m.store.commit(m.store.set(sb)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.reg.Consume(t.Context(), sb.ID, sb.ArchiveEpoch); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !ok || !rec.Consumed {
		t.Fatalf("setup: %+v, %t, %v", rec, ok, err)
	}
	m.reconcileOwnRow(t.Context(), rec, time.Now())
	if rec, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok || !rec.Consumed {
		t.Fatalf("recovery republished desired writer: %+v, %t, %v", rec, ok, err)
	}
}

func TestLiveWriterIntentSurvivesRestartBeforeRetirement(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	persisted, err := newClaimStore(m.dataDir, false).load()
	if err != nil || !slices.Equal(persisted[sb.ID].VolumesWant, writer) || len(persisted[sb.ID].Volumes) != 0 {
		t.Fatalf("durable intent: %+v, %v", persisted, err)
	}
	// Consume without acknowledging it locally, then restart before the next scan.
	if _, err = c.reg.Consume(t.Context(), sb.ID, sb.ArchiveEpoch); err != nil {
		t.Fatal(err)
	}
	restarted := restartReplicaVolumeManager(t, m, c)
	restored, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("pending writer lost after restart")
	}
	view, err := restarted.GetVolumes(sb.ID, "")
	if err != nil || len(view.VolumesWant) != 1 || len(view.Volumes) != 0 || view.Converging || view.States[0].Error != ErrReplicaPresent.Error() {
		t.Fatalf("restart view: %+v, %v", view, err)
	}
	t.Logf("restart: %+v replica=%t synced=%t", view, restored.HasReplica(), restarted.store.synced())
	if _, err = restarted.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: writer}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("second writer after restart: %v", err)
	}
	rec, _, _ := c.reg.Get(t.Context(), sb.ID)
	restarted.reconcileOwnRow(t.Context(), rec, time.Now())
	if rec, _, err = c.reg.Get(t.Context(), sb.ID); err != nil || !rec.Consumed {
		t.Fatalf("sweep rearmed replica: %+v, %v", rec, err)
	}
	restarted.replicateOnce(t.Context())
	waitFor(t, func() bool { return !restarted.replicating.Load() })
	view, err = restarted.GetVolumes(sb.ID, "")
	if err != nil || restored.HasReplica() || len(view.Volumes) != 0 || view.Converging {
		t.Fatalf("retirement unexpectedly attached: %+v, %v", view, err)
	}
	t.Logf("retired: %+v replica=%t synced=%t", view, restored.HasReplica(), restarted.store.synced())
	if view, err = restarted.SetVolumes(t.Context(), sb.ID, "", writer); err != nil || len(view.Volumes) != 1 {
		t.Fatalf("explicit retry: %+v, %v", view, err)
	}
	t.Logf("attached on retry: %+v replica=%t", view, restored.HasReplica())
}

func TestLiveWriterRetirementDiagnosticsSurviveRetry(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	m.archives = &hookRegistry{Registry: c.reg, loseConsumeReply: true}
	m.retireReplica(t.Context(), sb)
	for range 2 {
		view, err := m.SetVolumes(t.Context(), sb.ID, "", writer)
		if !errors.Is(err, ErrReplicaPresent) || len(view.Volumes) != 0 || view.States[0].Error != volumeReplicaUnavailable {
			t.Fatalf("retirement failure diagnostic: %+v, %v", view, err)
		}
		t.Logf("retire failure and retry: %+v replica=%t synced=%t", view, sb.HasReplica(), m.store.synced())
	}
	m.archives = c.reg
	m.retireReplica(t.Context(), sb)
	if view, err := m.SetVolumes(t.Context(), sb.ID, "", writer); err != nil || len(view.Volumes) != 1 || view.States[0].Error != "" {
		t.Fatalf("retry after registry recovery: %+v, %v", view, err)
	}
}

func TestLiveWriterFailedIntentDoesNotApply(t *testing.T) {
	for _, cancelAfterWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelAfterWrite), func(t *testing.T) {
			m, _ := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			replicateNow(t, m, sb)
			eng := m.eng.(*fakeEngine)
			before := eng.volumeOpsLog()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			m.store.recommitting.Store(true)
			m.store.replaceFile = func(raw []byte) error {
				if cancelAfterWrite {
					if err := m.store.replace(raw); err != nil {
						return err
					}
					cancel()
					return nil
				}
				return errors.New("injected intent write failure")
			}
			writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
			view, err := m.SetVolumes(ctx, sb.ID, "", writer)
			if err == nil || len(view.VolumesWant) != 1 || len(view.Volumes) != 0 || !slices.Equal(before, eng.volumeOpsLog()) {
				t.Fatalf("failed intent applied: %+v, %v", view, err)
			}
			t.Logf("failed/canceled intent: %+v replica=%t synced=%t error=%v", view, sb.HasReplica(), m.store.synced(), err)
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: writer}); !errors.Is(err, ErrVolumeBusy) {
				t.Fatalf("failure lost reservation: %v", err)
			}
			m.store.replaceFile = nil
			if _, err = m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
				t.Fatalf("remove canceled/failed target: %v", err)
			}
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("remove retained intent: %+v", got)
			}
		})
	}
}

func TestLiveWriterWaitsForRetirementCommitAcknowledgment(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	before := m.eng.(*fakeEngine).volumeOpsLog()
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected retirement persistence failure") }
	m.retireReplica(t.Context(), sb)
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil || sb.HasReplica() || m.store.synced() || len(view.States) != 1 || view.States[0].Error != ErrVolumePendingCommit.Error() {
		t.Fatalf("unacknowledged clear: %+v, %v", view, err)
	}
	t.Logf("unacknowledged retirement: %+v replica=%t synced=%t", view, sb.HasReplica(), m.store.synced())
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", writer); err == nil {
		t.Fatal("failed persistence acknowledged attachment")
	}
	if !slices.Equal(before, m.eng.(*fakeEngine).volumeOpsLog()) {
		t.Fatal("writer exposed before retirement persisted")
	}
	rec, _, err := c.reg.Get(t.Context(), sb.ID)
	if err != nil || !rec.Consumed {
		t.Fatalf("retirement tombstone: %+v, %v", rec, err)
	}
	m.reconcileOwnRow(t.Context(), rec, time.Now())
	if _, ok, err := c.reg.Get(t.Context(), sb.ID); err != nil || !ok {
		t.Fatalf("sweep erased pending retirement: %t, %v", ok, err)
	}
	// Any newer whole-claim snapshot can acknowledge the clear; no special retry ticket is needed.
	m.store.replaceFile = nil
	if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{}); err != nil {
		t.Fatal(err)
	}
	m.reconcileOwnRow(t.Context(), rec, time.Now())
	if view, err = m.SetVolumes(t.Context(), sb.ID, "", writer); err != nil || len(view.Volumes) != 1 {
		t.Fatalf("retry after newer acknowledgment: %+v, %v", view, err)
	}
	t.Logf("newer snapshot acknowledged retirement: %+v replica=%t synced=%t", view, sb.HasReplica(), m.store.synced())
}

func TestLiveWriterApplyGuardRequiresAcknowledgedRetirement(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	// Pause a request after its target commit, before its physical-apply check.
	ctx, run, err := m.beginVolumeRun(t.Context(), sb, "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.finishVolumeRun(sb, run)
	if err = m.prepareVolumeTarget(ctx, sb, run, writer, nil, nil); err != nil {
		t.Fatal(err)
	}
	before := m.eng.(*fakeEngine).volumeOpsLog()
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected retirement clear failure") }
	m.retireReplica(t.Context(), sb)
	if sb.HasReplica() || m.store.synced() {
		t.Fatal("expected memory clear with unacknowledged snapshot")
	}
	if err = m.checkVolumeApply(ctx, sb, run); !errors.Is(err, ErrVolumePendingCommit) {
		t.Fatalf("physical apply guard passed unacknowledged clear: %v", err)
	}
	if !slices.Equal(before, m.eng.(*fakeEngine).volumeOpsLog()) {
		t.Fatal("device call before acknowledgment")
	}
	t.Logf("apply boundary: want=%+v applied=%+v replica=%t synced=%t error=%v", sb.VolumesWant, sb.Volumes, sb.HasReplica(), m.store.synced(), err)
	m.store.replaceFile = nil
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if err = m.checkVolumeApply(ctx, sb, run); err != nil {
		t.Fatalf("acknowledged clear still blocked: %v", err)
	}
}

func TestReplicaConsumedRowPurgeWaitsForSnapshot(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(fmt.Sprintf("claimed=%t", claimed), func(t *testing.T) {
			m, c := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			replicateNow(t, m, sb)
			m.store.recommitting.Store(true)
			m.store.replaceFile = func([]byte) error { return errors.New("injected clear failure") }
			m.retireReplica(t.Context(), sb)
			if !claimed {
				m.mu.Lock()
				delete(m.claimed, sb.ID)
				m.store.del(sb.ID)
				m.mu.Unlock()
			}
			rec, ok, err := c.reg.Get(t.Context(), sb.ID)
			if err != nil || !ok || !rec.Consumed {
				t.Fatalf("setup: %+v, %t, %v", rec, ok, err)
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || !ok {
				t.Fatalf("sweep dropped unacknowledged row: %t, %v", ok, err)
			}
			m.store.replaceFile = nil
			if err = m.store.commit(m.store.mark()); err != nil {
				t.Fatal(err)
			}
			m.reconcileOwnRow(t.Context(), rec, time.Now())
			if _, ok, err = c.reg.Get(t.Context(), sb.ID); err != nil || ok {
				t.Fatalf("sweep retained acknowledged row: %t, %v", ok, err)
			}
		})
	}
}

func TestLiveWriterWaitingIntentCanRelease(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return volumeHoldersOf(m, "data") == (volumeHolders{}) })
	if _, ok := m.byID(sb.ID); ok {
		t.Fatal("released pending claim remains")
	}
	if rec, ok, err := c.reg.Get(t.Context(), sb.ID); err != nil || ok && !rec.Consumed {
		t.Fatalf("released replica still adoptable: %+v, %t, %v", rec, ok, err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: writer}); err != nil {
		t.Fatalf("released intent retained writer admission: %v", err)
	}
}

func TestLiveWriterIntentIsDroppedWhenReplicaMoves(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}); !errors.Is(err, ErrReplicaPresent) {
		t.Fatal(err)
	}
	c.expire("a")
	other := c.node(t, "b", t.TempDir(), keepAnHour)
	if adopted, err := other.AdoptArchived(t.Context(), sb.ID, sb.Token); err != nil || !adopted {
		t.Fatalf("adopt: %t, %v", adopted, err)
	}
	m.retireReplica(t.Context(), sb)
	if _, ok := m.byID(sb.ID); ok {
		t.Fatal("old owner kept a pending writer after replica moved")
	}
	moved, ok := other.byID(sb.ID)
	if !ok || len(moved.VolumesWant) != 0 || len(moved.Volumes) != 0 {
		t.Fatalf("old replica carried new volume intent: %+v", moved)
	}
}

func TestLiveWriterRecordsIntentBeforeRetiringReplica(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, attachOnly := range []bool{false, true} {
			for _, patch := range []bool{false, true} {
				t.Run(fmt.Sprintf("pending=%t/attach_only=%t/patch=%t", pending, attachOnly, patch), func(t *testing.T) {
					m, c := newReplicaVolumeManager(t)
					sb := mustClaim(t, m, testKey)
					if pending {
						m.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
						if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
							t.Fatal(err)
						}
					} else {
						replicateNow(t, m, sb)
					}
					row, exists, err := c.reg.Get(t.Context(), sb.ID)
					if err != nil || !exists || row.Consumed || !sb.HasReplica() {
						t.Fatalf("replica setup failed: %+v, %t, %v", row, exists, err)
					}
					eng := m.eng.(*fakeEngine)
					before := eng.volumeOpsLog()
					writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: attachOnly}}
					var view types.SandboxVolumesResponse
					if patch {
						view, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: writer})
					} else {
						view, err = m.SetVolumes(t.Context(), sb.ID, "", writer)
					}
					if !errors.Is(err, ErrReplicaPresent) {
						t.Fatalf("writer admitted while replica is recoverable: %v", err)
					}
					if len(view.Volumes) != 0 || len(view.VolumesWant) != 1 || view.Converging || !slices.Equal(before, eng.volumeOpsLog()) || volumeHoldersOf(m, "data").writers != 1 {
						t.Fatalf("refused writer changed devices or holds: %+v", view)
					}
					if next, ok, getErr := c.reg.Get(t.Context(), sb.ID); getErr != nil || !ok || next.Checkpoint != row.Checkpoint || next.Consumed {
						t.Fatalf("refusal changed replica: %+v, %t, %v", next, ok, getErr)
					}
					m.archives = c.reg
					m.retireReplica(t.Context(), sb)
					if sb.HasReplica() {
						t.Fatal("retirement did not settle the replica")
					}
					if _, err = m.SetVolumes(t.Context(), sb.ID, "", writer); err != nil {
						t.Fatalf("writer after replica retirement: %v", err)
					}
				})
			}
		}
	}
}

func TestReplicaPublicationRefusesVolumeAdmittedDuringExport(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	stalled := &stallingStore{Store: m.ckpts, published: make(chan string), release: make(chan struct{})}
	m.ckpts = stalled
	result := make(chan error, 1)
	go func() { result <- m.replicate(t.Context(), sb) }()
	ck := <-stalled.published
	release := sync.OnceFunc(func() { close(stalled.release) })
	t.Cleanup(release)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	view, err := m.SetVolumes(t.Context(), sb.ID, "", writer)
	if err != nil || len(view.Volumes) != 1 {
		t.Fatalf("writer during checkpoint publication: %+v, %v", view, err)
	}
	release()
	if err = <-result; !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("stale replica registered after writer admission: %v", err)
	}
	if _, ok, getErr := c.reg.Get(t.Context(), sb.ID); getErr != nil || ok || sb.HasReplica() || ckExists(t, m, ck) {
		t.Fatalf("stale replica remains recoverable: row=%t, claim=%t, checkpoint=%t, err=%v", ok, sb.HasReplica(), ckExists(t, m, ck), getErr)
	}
	if got := volumeHoldersOf(m, "data"); got.writers != 1 {
		t.Fatalf("refusing replica lost writer hold: %+v", got)
	}
	if err = m.replicate(t.Context(), sb); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("snapshot admission while writer is held: %v", err)
	}
}

func TestReplicaCaptureRefusesPendingLiveWriter(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	eng := m.eng.(*fakeEngine)
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	eng.liveVolumeHook = func(verb string) error {
		if verb == "attach" {
			close(entered)
			<-resume
		}
		return nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}})
		result <- err
	}()
	<-entered
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 1 || !view.Converging {
		t.Fatalf("writer did not reach pending attachment: %+v, %v", view, err)
	}
	if err = m.replicate(t.Context(), sb); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("captured pending writer: %v", err)
	}
	if _, ok, getErr := c.reg.Get(t.Context(), sb.ID); getErr != nil || ok || len(eng.snapSaves) != 0 {
		t.Fatalf("pending writer produced snapshot or row: snapshots=%v, row=%t, err=%v", eng.snapSaves, ok, getErr)
	}
	release()
	if err = <-result; err != nil {
		t.Fatalf("pending writer did not complete: %v", err)
	}
}

func TestReplicaAllowsLiveReadOnlyAndCleanup(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	reader := []types.Volume{{Name: "data"}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", reader); err != nil {
		t.Fatalf("reader with replica: %v", err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", reader); err != nil {
		t.Fatalf("held reader retry: %v", err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
		t.Fatalf("reader detach with replica: %v", err)
	}
}

func newReplicaVolumeManager(t *testing.T) (*Manager, *failoverCluster) {
	t.Helper()
	c := newReplicaCluster(t)
	spec := archivePool(keepAnHour)
	spec.ReplicateSeconds = c.replicate
	cfg := &config.Config{
		APIToken: "root", DataDir: t.TempDir(), CheckpointDir: c.ckDir,
		Pools: []config.PoolSpec{spec}, Mesh: &config.MeshConfig{NodeID: "a"},
		Volumes: []config.VolumeSpec{{Name: "data", Path: writeVolumeImage(t, "data", "original"), Writable: true}},
	}
	backends := testBackends(t, cfg)
	backends.Archives = c.reg
	m, err := NewManager(t.Context(), cfg, newFakeEngine(), testSecrets(t), backends)
	if err != nil {
		t.Fatal(err)
	}
	c.renew(t, "a")
	m.liftFence()
	return m, c
}

func restartReplicaVolumeManager(t *testing.T, old *Manager, c *failoverCluster) *Manager {
	t.Helper()
	spec := archivePool(keepAnHour)
	spec.ReplicateSeconds = c.replicate
	cfg := &config.Config{APIToken: "root", DataDir: old.dataDir, CheckpointDir: c.ckDir, Pools: []config.PoolSpec{spec}, Mesh: &config.MeshConfig{NodeID: old.nodeID}, Volumes: []config.VolumeSpec{{Name: "data", Path: old.catalog.entries["data"].disk.Path, Writable: true}}}
	backends := testBackends(t, cfg)
	backends.Archives = c.reg
	m, err := NewManager(t.Context(), cfg, old.eng, testSecrets(t), backends)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.liftFence()
	return m
}
