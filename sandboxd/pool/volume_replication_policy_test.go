package pool

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveWriterDoesNotWaitForAnotherClaimsRenewal(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	other := mustClaim(t, m, testKey)
	attached := make(chan struct{}, 1)
	eng.liveVolumeHook = func(op string) error {
		if op == "attach" {
			attached <- struct{}{}
		}
		return nil
	}
	resume := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	renewed := make(chan error, 1)
	var writes atomic.Int32
	m.store.replaceFile = func(raw []byte) error {
		switch writes.Add(1) {
		case 1:
			if err := m.store.replace(raw); err != nil {
				return err
			}
			before := m.store.mark().seq
			go func() {
				_, err := m.Renew(t.Context(), other.ID, Cred{Token: other.Token}, time.Hour, "")
				renewed <- err
			}()
			deadline := time.Now().Add(3 * time.Second)
			for m.store.mark().seq == before {
				if time.Now().After(deadline) {
					return errors.New("renew did not stage its unrelated claim")
				}
				time.Sleep(time.Millisecond)
			}
			return nil
		case 2:
			<-resume
		}
		return m.store.replace(raw)
	}
	changed := make(chan error, 1)
	go func() {
		_, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}})
		changed <- err
	}()
	finished := false
	select {
	case <-attached:
	case err := <-changed:
		finished = true
		t.Errorf("unrelated renewal rejected writable attach: %v", err)
	case <-time.After(3 * time.Second):
		t.Error("unrelated renewal prevented writable device application")
	}
	release()
	if !finished {
		if err := <-changed; err != nil {
			t.Errorf("writable attach after renewal: %v", err)
		}
	}
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil || len(view.Volumes) != 1 {
		t.Fatalf("writer did not attach: %+v, %v", view, err)
	}
}

func TestArchiveCheckpointCleanupIgnoresUnrelatedClaimWrites(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	checkpoint, _, err := m.publishCheckpoint(t.Context(), sb, store.CheckpointID(randHex(8)), "orphan", "", true)
	if err != nil {
		t.Fatal(err)
	}
	ck := checkpoint.ID
	other := mustClaim(t, m, testKey)
	m.mu.Lock()
	other.LeaseSeconds++
	m.store.set(other)
	m.mu.Unlock()
	if m.store.synced() {
		t.Fatal("expected an unrelated unacknowledged claim write")
	}
	if err := m.deleteArchiveCk(t.Context(), ck); err != nil {
		t.Fatalf("unrelated claim delayed checkpoint cleanup: %v", err)
	}
	if ckExists(t, m, ck) || m.deleteMarked(ck) {
		t.Fatal("unreferenced checkpoint or delete marker remained")
	}
}

func TestLiveWriterRequiresNonReplicatingPool(t *testing.T) {
	for _, patch := range []bool{false, true} {
		for _, attachOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("patch=%t/attach_only=%t", patch, attachOnly), func(t *testing.T) {
				m, _ := newReplicaVolumeManager(t)
				sb := mustClaim(t, m, testKey)
				before := m.store.mark()
				beforeOps := m.eng.(*fakeEngine).volumeOpsLog()
				t.Logf("before live mutation: snapshot=%+v device_ops=%v", before, beforeOps)
				want := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: attachOnly}}
				var view types.SandboxVolumesResponse
				var err error
				if patch {
					view, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: want})
				} else {
					view, err = m.SetVolumes(t.Context(), sb.ID, "", want)
				}
				if !errors.Is(err, ErrReplicatingPool) {
					t.Fatalf("replicating pool accepted live writer: %+v, %v", view, err)
				}
				if sb.HasReplica() || len(view.VolumesWant) != 0 || len(view.Volumes) != 0 || view.Converging || m.store.mark() != before || volumeHoldersOf(m, "data") != (volumeHolders{}) || !slices.Equal(beforeOps, m.eng.(*fakeEngine).volumeOpsLog()) {
					t.Fatalf("policy refusal changed claim, device, or reservation: %+v snapshot=%+v->%+v ops=%v->%v", view, before, m.store.mark(), beforeOps, m.eng.(*fakeEngine).volumeOpsLog())
				}
			})
		}
	}
}

func TestInitialWriterRemainsAvailableInReplicatingPool(t *testing.T) {
	m, _ := newReplicaVolumeManager(t)
	sb, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}})
	if err != nil || len(sb.Volumes) != 1 {
		t.Fatalf("initial writer: %+v, %v", sb, err)
	}
	m.replicateOnce(t.Context())
	waitFor(t, func() bool { return !m.replicating.Load() })
	if sb.HasReplica() || len(m.eng.(*fakeEngine).snapSaves) != 0 {
		t.Fatal("replicated an initial writer")
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", sb.Volumes); err != nil {
		t.Fatalf("held writer retry refused: %v", err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
		t.Fatalf("writer cleanup refused: %v", err)
	}
}

func TestLiveWriterUsesItsPolicySource(t *testing.T) {
	for _, sourceReplicates := range []bool{false, true} {
		t.Run(fmt.Sprint(sourceReplicates), func(t *testing.T) {
			m, _ := newReplicaVolumeManager(t)
			sb := mustClaim(t, m, testKey)
			other := testKey
			other.Template = "other:template"
			m.mu.Lock()
			if sourceReplicates {
				sb.Key, sb.PolicySource = other, testKey
			} else {
				sb.PolicySource = other
			}
			m.mu.Unlock()
			_, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}})
			if sourceReplicates && !errors.Is(err, ErrReplicatingPool) || !sourceReplicates && err != nil {
				t.Fatalf("policy source replication=%t: %v", sourceReplicates, err)
			}
		})
	}
}

func TestLiveWriterWaitsOnlyForItsOwnCheckpointReferences(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	other := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	replicateNow(t, m, other)
	m = restartReplicaVolumeManager(t, m, c)
	sb, _ = m.byID(sb.ID)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) {
		t.Fatalf("restart ignored actual replica: %v", err)
	}
	m.store.recommitting.Store(true)
	m.store.replaceFile = func([]byte) error { return errors.New("injected clear failure") }
	m.retireReplica(t.Context(), sb)
	before := m.store.mark()
	if sb.HasReplica() || len(m.store.checkpointRefs(sb.ID)) == 0 {
		t.Fatal("expected an acknowledged replica reference after memory clear")
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", writer); !errors.Is(err, ErrReplicaPresent) || m.store.mark() != before {
		t.Fatalf("unacknowledged replica clear allowed writer or staged intent: %v", err)
	}
	m.store.replaceFile = nil
	if err := m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if len(m.store.checkpointRefs(other.ID)) == 0 {
		t.Fatal("unrelated replica did not survive the restart")
	}
	if view, err := m.SetVolumes(t.Context(), sb.ID, "", writer); err != nil || len(view.Volumes) != 1 {
		t.Fatalf("acknowledged retirement blocked writer: %+v, %v", view, err)
	}
}

func TestLegacyWriterIntentCanRemoveReadOnlyVolumes(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	sb.VolumesWant = append(slices.Clone(sb.Volumes), types.Volume{Name: "other", Mode: types.VolumeModeRW, AttachOnly: true})
	snap := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(snap); err != nil {
		t.Fatal(err)
	}
	c := newReplicaCluster(t)
	spec := archivePool(keepAnHour)
	spec.ReplicateSeconds = c.replicate
	cfg := &config.Config{APIToken: "root", DataDir: m.dataDir, CheckpointDir: c.ckDir, Volumes: catalog, Pools: []config.PoolSpec{spec}, Mesh: &config.MeshConfig{NodeID: "a"}}
	backends := testBackends(t, cfg)
	backends.Archives = c.reg
	var err error
	m, err = NewManager(t.Context(), cfg, eng, testSecrets(t), backends)
	if err != nil {
		t.Fatal(err)
	}
	c.renew(t, "a")
	if err = m.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.liftFence()
	got, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Remove: []string{"data"}})
	if !errors.Is(err, ErrReplicatingPool) || len(got.Volumes) != 0 || len(got.VolumesWant) != 1 || got.VolumesWant[0].Name != "other" {
		t.Fatalf("legacy writer blocked reader cleanup: %+v, %v", got, err)
	}
	if got, err = m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil || len(got.VolumesWant) != 0 || len(got.Volumes) != 0 {
		t.Fatalf("legacy writer cannot be canceled: %+v, %v", got, err)
	}
	if volumeHoldersOf(m, "other") != (volumeHolders{}) {
		t.Fatal("canceled legacy writer retained admission")
	}
}

func TestLegacyWriterApplyWaitsForItsRetirementReference(t *testing.T) {
	m, c := newReplicaVolumeManager(t)
	sb := mustClaim(t, m, testKey)
	replicateNow(t, m, sb)
	writer := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	m.mu.Lock()
	sb.VolumesWant = writer
	snap := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(snap); err != nil {
		t.Fatal(err)
	}
	m = restartReplicaVolumeManager(t, m, c)
	sb, _ = m.byID(sb.ID)
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
	m.store.replaceFile = func([]byte) error { return errors.New("injected legacy retirement failure") }
	m.retireReplica(t.Context(), sb)
	if sb.HasReplica() || len(m.store.checkpointRefs(sb.ID)) == 0 {
		t.Fatal("expected only an unacknowledged retirement reference")
	}
	if err = m.checkVolumeApply(ctx, sb, run, writer[0]); !errors.Is(err, ErrReplicaPresent) {
		t.Fatalf("legacy apply ignored its retirement reference: %v", err)
	}
	if !slices.Equal(before, m.eng.(*fakeEngine).volumeOpsLog()) {
		t.Fatal("legacy writer called the device before retirement acknowledgement")
	}
	m.store.replaceFile = nil
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if err = m.checkVolumeApply(ctx, sb, run, writer[0]); err != nil {
		t.Fatalf("acknowledged legacy retirement blocked apply: %v", err)
	}
}
