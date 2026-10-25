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
					return
				}
				t.Logf("cycle %d: %+v, %v", cycle, view, err)
				m.replicas = map[string]replicaState{sb.ID: {tried: time.Now().Add(-2 * time.Duration(replicateEvery) * time.Second)}}
				m.replicateOnce(t.Context())
				waitFor(t, func() bool { return !m.replicating.Load() })
			}
			t.Fatalf("writer never converged after five replica scans: %+v, %v", view, err)
		})
	}
}

func TestLiveWriterRefusesExistingReplica(t *testing.T) {
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
					if !errors.Is(err, ErrVolumeBusy) {
						t.Fatalf("writer admitted while replica is recoverable: %v", err)
					}
					if len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || !slices.Equal(before, eng.volumeOpsLog()) || volumeHoldersOf(m, "data") != (volumeHolders{}) {
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
