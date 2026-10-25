package pool

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

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

func TestReplicaUnpublishedIntentSurvivesFailedClearAndRestart(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	m.archives = &hookRegistry{Registry: c.reg, failPut: true}
	if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatal(err)
	}
	pending := sb.ReplicaPending
	if pending == "" || !ckExists(t, m, pending) {
		t.Fatal("missing unpublished checkpoint")
	}
	m.archives = c.reg
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected pending clear failure") }
	m.retireReplica(t.Context(), sb)
	persisted, err := newClaimStore(m.dataDir, false).load()
	if err != nil || persisted[sb.ID].ReplicaPending != pending || len(persisted[sb.ID].VolumesWant) != 0 {
		t.Fatalf("failure did not retain old disk state: %+v, %v", persisted, err)
	}
	if !ckExists(t, m, pending) {
		t.Error("failed pending clear deleted the checkpoint used to distinguish an unpublished intent from adoption")
	}
	t.Logf("failed unpublished clear: memory_pending=%q durable_pending=%q checkpoint_exists=%t wanted=%+v synced=%t", sb.ReplicaPending, persisted[sb.ID].ReplicaPending, ckExists(t, m, pending), persisted[sb.ID].VolumesWant, m.store.synced())
	restarted := restartReplicaVolumeManager(t, m, c)
	restored, ok := restarted.byID(sb.ID)
	if !ok {
		t.Fatal("restart mistook a failed pending clear for adoption and discarded the claim")
	}
	if len(restored.VolumesWant) != 0 || len(restored.Volumes) != 0 {
		t.Fatalf("restart lost pending writer: %+v", restored)
	}
	restarted.retireReplica(t.Context(), restored)
	if restored.HasReplica() {
		t.Fatal("restart failed to retire unpublished replica")
	}
}

func TestReplicaPendingCheckpointCleanupWaitsForAcknowledgment(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	m.archives = &hookRegistry{Registry: c.reg, failPut: true}
	if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatal(err)
	}
	pending := sb.ReplicaPending
	m.archives = c.reg
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected pending clear failure") }
	m.retireReplica(t.Context(), sb)
	if sb.ReplicaPending != "" || m.store.synced() {
		t.Fatal("expected memory clear before snapshot acknowledgment")
	}
	if err := m.deleteArchiveCk(t.Context(), pending); err == nil {
		t.Fatal("orphan delete ignored unacknowledged claim")
	}
	m.retryArchiveDeletes(t.Context())
	if !ckExists(t, m, pending) || !m.deleteMarked(pending) {
		t.Fatal("cleanup discarded pending checkpoint or retry marker")
	}
	t.Logf("cleanup deferred: checkpoint=%s exists=%t marker=%t synced=%t", pending, ckExists(t, m, pending), m.deleteMarked(pending), m.store.synced())
	m.store.replaceFile = nil
	if err := m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	other := mustClaim(t, m, testKey)
	m.mu.Lock()
	other.LeaseSeconds++
	m.store.set(other)
	m.mu.Unlock()
	m.retryArchiveDeletes(t.Context())
	if ckExists(t, m, pending) || m.deleteMarked(pending) {
		t.Fatal("acknowledged cleanup leaked checkpoint or retry marker")
	}
	t.Logf("cleanup acknowledged: exists=%t marker=%t synced=%t", ckExists(t, m, pending), m.deleteMarked(pending), m.store.synced())
}

func TestReplicaSettledCheckpointCleanupRetriesAfterFailedCommit(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	previous := sb.ReplicaCk
	m.archives = &hookRegistry{Registry: c.reg, losePutReply: true}
	if err := m.replicate(t.Context(), sb); !errors.Is(err, ErrArchiveStoreDown) {
		t.Fatal(err)
	}
	pending := sb.ReplicaPending
	m.archives = c.reg
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected settled intent failure") }
	sb.Transition.Lock()
	err := m.settleReplicaIntent(t.Context(), sb)
	sb.Transition.Unlock()
	if err == nil || sb.ReplicaPending != "" || sb.ReplicaCk != pending || m.store.synced() {
		t.Fatalf("settle state: %+v, %v", sb, err)
	}
	if !ckExists(t, m, previous) || !m.deleteMarked(previous) {
		t.Fatal("failed settle lost old checkpoint or its queued cleanup")
	}
	m.retryArchiveDeletes(t.Context())
	if !ckExists(t, m, previous) {
		t.Fatal("old checkpoint deleted before clear acknowledgment")
	}
	m.store.replaceFile = nil
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	m.retryArchiveDeletes(t.Context())
	if ckExists(t, m, previous) || m.deleteMarked(previous) || !ckExists(t, m, pending) {
		t.Fatal("acknowledged settle failed to clean old checkpoint or removed current checkpoint")
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

func TestLiveWriterRefusesExistingReplica(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, attachOnly := range []bool{false, true} {
			for _, patch := range []bool{false, true} {
				t.Run(fmt.Sprintf("pending=%t/attach_only=%t/patch=%t", pending, attachOnly, patch), func(t *testing.T) {
					m, c := newReplicaVolumeManager(t)
					disableReplication(m)
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
					if len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || view.Converging || !slices.Equal(before, eng.volumeOpsLog()) || volumeHoldersOf(m, "data").writers != 0 {
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
	disableReplication(m)
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
	disableReplication(m)
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

func disableReplication(m *Manager) {
	view := *m.view.Load()
	view.poolReplicate = nil
	m.view.Store(&view)
}
