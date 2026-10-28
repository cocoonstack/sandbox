package pool

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestReplicaRecoveryDoesNotRepublishReadOnlyHold(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	if view, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil || len(view.Volumes) != 1 {
		t.Fatalf("read-only live attachment: %+v, %v", view, err)
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
		t.Fatalf("recovery republished read-only hold: %+v, %t, %v", rec, ok, err)
	}
}

func TestReplicaPublicationRefusesVolumeAdmittedDuringExport(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	disableReplication(m)
	sb := mustClaim(t, m, testKey)
	stalled := &stallingStore{Store: m.ckpts, published: make(chan string), release: make(chan struct{})}
	m.ckpts = stalled
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() { defer close(done); result <- m.replicate(t.Context(), sb) }()
	ck := <-stalled.published
	release := sync.OnceFunc(func() { close(stalled.release) })
	t.Cleanup(func() { release(); <-done })
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
	eng.liveVolumeHook = func(verb string) error {
		if verb == "attach" {
			close(entered)
			<-resume
		}
		return nil
	}
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}})
		result <- err
	}()
	t.Cleanup(func() { release(); <-done })
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

func TestLiveWriterPolicyAndCheckpointRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*Manager, *types.Sandbox)
		want    error
	}{
		{"replicating pool", func(*Manager, *types.Sandbox) {}, ErrReplicatingPool},
		{"source pool replicates", func(_ *Manager, sb *types.Sandbox) {
			sb.PolicySource = testKey
			sb.Key.Template = "promoted:template"
		}, ErrReplicatingPool},
		{"source pool does not replicate", func(_ *Manager, sb *types.Sandbox) {
			sb.PolicySource = testKey
			sb.PolicySource.Template = "other:template"
		}, nil},
		{"published replica", func(m *Manager, sb *types.Sandbox) { disableReplication(m); replicateNow(t, m, sb) }, ErrReplicaPresent},
		{"pending replica", func(m *Manager, sb *types.Sandbox) {
			disableReplication(m)
			sb.ReplicaPending = "pending"
			m.store.set(sb)
		}, ErrReplicaPresent},
		{"registry disabled", func(m *Manager, sb *types.Sandbox) { disableReplication(m); replicateNow(t, m, sb); m.archives = nil }, ErrReplicaStranded},
		{"archive pending without registry", func(m *Manager, sb *types.Sandbox) {
			disableReplication(m)
			sb.ArchivePending = "pending"
			m.archives = nil
		}, ErrReplicaStranded},
		{"retained only without registry", func(m *Manager, sb *types.Sandbox) {
			disableReplication(m)
			sb.ReplicaCk = "retained"
			if err := m.store.commit(m.store.set(sb)); err != nil {
				t.Fatal(err)
			}
			sb.ReplicaCk = ""
			m.store.set(sb)
			m.archives = nil
		}, ErrReplicaPresent},
		{"unrelated checkpoint", func(m *Manager, _ *types.Sandbox) {
			disableReplication(m)
			other := mustClaim(t, m, testKey)
			other.ReplicaCk = "unrelated"
			m.store.set(other)
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			tc.prepare(m, sb)
			before, ops := m.store.mark(), m.eng.(*fakeEngine).volumeOpsLog()
			view, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}})
			if !errors.Is(err, tc.want) {
				t.Fatalf("writer refusal = %v, want %v", err, tc.want)
			}
			if tc.want != nil && (len(view.VolumesWant) != 0 || len(view.Volumes) != 0 || view.Converging || m.store.mark() != before || volumeHoldersOf(m, "data") != (volumeHolders{}) || !slices.Equal(ops, m.eng.(*fakeEngine).volumeOpsLog())) {
				t.Fatalf("refusal changed claim or device: %+v", view)
			}
		})
	}
}

func TestInitialWriterAndRetiredReplicaAllowLiveWork(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", sb.Volumes); err != nil {
		t.Fatalf("held writer retry: %v", err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
		t.Fatalf("writer cleanup: %v", err)
	}
	replicateNow(t, m, sb)
	disableReplication(m)
	m.store.recommitting.Store(true)
	restore := failClaimWrites(t, m.store)
	m.retireReplica(t.Context(), sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}
	if sb.HasReplica() || !m.store.hasCheckpointReferences(sb.ID) {
		t.Fatal("failed clear lost checkpoint uncertainty")
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatalf("unacknowledged retirement: %v", err)
	}
	restore()
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", writer); err != nil {
		t.Fatalf("acknowledged retirement: %v", err)
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

func disableReplication(m *Manager) {
	view := *m.view.Load()
	view.poolReplicate = nil
	m.view.Store(&view)
}
