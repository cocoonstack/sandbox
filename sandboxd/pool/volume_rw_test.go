package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestWritableClaimMarksDirtyBeforeAttachAndClearsOnRelease(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	eng := newFakeEngine()
	eng.dirtyPaths = []string{path}
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})

	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	want := []types.Volume{{Name: "scratch", Mount: "/volumes/scratch", Mode: types.VolumeModeRW}}
	if !slices.Equal(sb.Volumes, want) {
		t.Errorf("applied volumes=%v, want %v", sb.Volumes, want)
	}
	if !eng.dirtyAtAttach("scratch") {
		t.Error("attach ran before the dirty marker was durable")
	}
	if specs := eng.volumeSpecs; len(specs) != 1 || !specs[0].RW {
		t.Errorf("attached specs=%+v, want one writable disk", specs)
	}
	if !slices.Equal(eng.volumeMounts, want) {
		t.Errorf("mounts=%v, want %v", eng.volumeMounts, want)
	}
	if !volumeDirty(path) {
		t.Error("live writable claim left no dirty marker")
	}

	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if volumeDirty(path) {
		t.Error("clean release left the image dirty")
	}
	if seen := eng.opsAtRemoval(sb.VMName); !slices.Contains(seen, "umount:/volumes/scratch") {
		t.Errorf("ops at removal=%v, want the unmount already done", seen)
	}
	if seen := eng.dirtyAtRemoval(sb.VMName); !slices.Contains(seen, path) {
		t.Errorf("markers at removal=%v, want the image still marked until the VM is gone", seen)
	}
	if got := eng.syncCount(); got != 0 {
		t.Errorf("guest syncs=%d, want none when every unmount succeeded", got)
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after release=%+v, want empty", holders)
	}
}

func TestClaimRejectsWriteOnReadOnlyEntry(t *testing.T) {
	readOnly := writeVolumeImage(t, "data.img", "data")
	writable := writeVolumeImage(t, "scratch.img", "scratch")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "data", Path: readOnly},
		{Name: "scratch", Path: writable, Writable: true},
	})

	_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}})
	if !errors.Is(err, ErrBadVolume) || !strings.Contains(err.Error(), "not writable") {
		t.Errorf("error=%v, want a not-writable ErrBadVolume", err)
	}
	if ops := eng.volumeOpsLog(); len(ops) != 0 {
		t.Errorf("rejected claim ran %v", ops)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch"}}); err != nil {
		t.Errorf("read-only claim of a writable entry: %v", err)
	}
	if volumeDirty(writable) {
		t.Error("read-only claim marked the image dirty")
	}
}

func TestDirtyVolumeBlocksReadersUntilWriterRecovers(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	if err := markVolumeDirty(path); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	readOnly := []types.Volume{{Name: "scratch"}}

	_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", readOnly)
	if !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Errorf("read-only claim of a dirty image: %v, want ErrVolumeNeedsRecovery", err)
	}
	if ops := eng.volumeOpsLog(); len(ops) != 0 {
		t.Errorf("refused claim ran %v", ops)
	}
	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
	if err != nil {
		t.Fatalf("writable recovery claim: %v", err)
	}
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", readOnly); err != nil {
		t.Errorf("read-only claim after recovery: %v", err)
	}
}

func TestDirtyVolumeRefusesWarmClaimBeforeTakingAVM(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	if err := markVolumeDirty(path); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}
	eng := newFakeEngine()
	m := newVolumePoolManager(t, eng, t.TempDir(), []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	warm := &types.Sandbox{VMName: "sbx-warm", Key: testKey, VsockSocket: "/vsock/warm"}
	m.pools[testKey].warm = append(m.pools[testKey].warm, warm)

	_, err := m.ClaimWarm(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch"}})
	if !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Fatalf("read-only warm claim of a dirty image: %v, want ErrVolumeNeedsRecovery", err)
	}
	m.mu.Lock()
	warmLeft := len(m.pools[testKey].warm)
	m.mu.Unlock()
	if warmLeft != 1 || eng.removed(warm.VMName) {
		t.Errorf("warm pool has %d VMs and removes=%v, want the warm VM untouched", warmLeft, eng.removedNames())
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after the refusal=%+v, want empty", holders)
	}
}

func TestConfirmVolumesCleanCatchesMarkerAfterAdmission(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	readOnly := []types.Volume{{Name: "scratch"}}

	// The reader resolves a clean image; a writable claim then fails between
	// that resolve and admission, leaving its marker but no hold behind.
	resolved, err := m.resolveVolumes(testKey, "", readOnly)
	if err != nil {
		t.Fatalf("resolveVolumes: %v", err)
	}
	if markErr := markVolumeDirty(path); markErr != nil {
		t.Fatalf("mark dirty: %v", markErr)
	}
	m.mu.Lock()
	reserveErr := m.reserveVolumes(appliedVolumes(resolved))
	m.mu.Unlock()
	if reserveErr != nil {
		t.Fatalf("reserve once the writer released: %v", reserveErr)
	}

	if cleanErr := m.confirmVolumesClean(resolved); !errors.Is(cleanErr, ErrVolumeNeedsRecovery) {
		t.Errorf("reader over a marker that landed after resolve: %v, want ErrVolumeNeedsRecovery", cleanErr)
	}
	m.unreserveVolumes(appliedVolumes(resolved))

	writable, err := m.resolveVolumes(testKey, "", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
	if err != nil {
		t.Fatalf("resolveVolumes writable: %v", err)
	}
	if cleanErr := m.confirmVolumesClean(writable); cleanErr != nil {
		t.Errorf("writable recovery claim over a dirty image: %v, want it admitted", cleanErr)
	}
}

func TestVolumeAdmissionMatrix(t *testing.T) {
	for _, tt := range []struct {
		name          string
		first, second string
		wantErr       error
	}{
		{"writer excludes writer", types.VolumeModeRW, types.VolumeModeRW, ErrVolumeBusy},
		// Admission answers first: a live writer is a busy conflict, never the
		// recovery verdict its own write-ahead marker would otherwise suggest.
		{"writer excludes reader", types.VolumeModeRW, "", ErrVolumeBusy},
		{"reader excludes writer", "", types.VolumeModeRW, ErrVolumeBusy},
		{"readers share", "", "", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := writeVolumeImage(t, "scratch.img", "data")
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
			second := []types.Volume{{Name: "scratch", Mode: tt.second}}

			first, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch", Mode: tt.first}})
			if err != nil {
				t.Fatalf("first claim: %v", err)
			}
			before := eng.volumeOpsLog()
			_, err = m.ClaimProvision(t.Context(), testKey, 0, "", "", second)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("concurrent read-only claim: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("second claim: %v, want %v", err, tt.wantErr)
			}
			if ops := eng.volumeOpsLog(); !slices.Equal(ops, before) {
				t.Errorf("refused claim ran %v, want nothing past %v", ops, before)
			}
			if err := m.Release(t.Context(), first.ID, Cred{Token: first.Token}); err != nil {
				t.Fatalf("release first: %v", err)
			}
			if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", second); err != nil {
				t.Errorf("claim after release: %v", err)
			}
		})
	}
}

func TestReserveVolumesExcludesReaderUnderWriter(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adoptVolumes([]types.Volume{{Name: "scratch", Mount: "/volumes/scratch", Mode: types.VolumeModeRW}})

	if err := m.reserveVolumes([]types.Volume{{Name: "scratch", Mount: "/volumes/scratch"}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("read-only reservation under a writer: %v, want ErrVolumeBusy", err)
	}
	if holders := m.volumeAdmission["scratch"]; holders != (volumeHolders{writers: 1}) {
		t.Errorf("registry after the refusal=%+v, want the writer alone", holders)
	}
}

func TestVolumeAdmissionRefusalHoldsNothing(t *testing.T) {
	held := writeVolumeImage(t, "held.img", "held")
	free := writeVolumeImage(t, "free.img", "free")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "held", Path: held, Writable: true},
		{Name: "free", Path: free, Writable: true},
	})
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "held", Mode: types.VolumeModeRW}}); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	_, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{
		{Name: "free", Mode: types.VolumeModeRW},
		{Name: "held", Mode: types.VolumeModeRW},
	})
	if !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("mixed claim: %v, want ErrVolumeBusy", err)
	}
	if holders := volumeHoldersOf(m, "free"); holders != (volumeHolders{}) {
		t.Errorf("registry for the free name=%+v, want empty", holders)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "free", Mode: types.VolumeModeRW}}); err != nil {
		t.Errorf("claim of the free name after a refusal: %v", err)
	}
}

func TestVolumeAdmissionReleasedAfterSetupFailure(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	eng := newFakeEngine()
	eng.mountVolumeErr = errors.New("mount failed")
	m := newVolumePoolManager(t, eng, t.TempDir(), []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	writable := []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}

	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable); err == nil {
		t.Fatal("ClaimProvision succeeded")
	}
	if ops := eng.volumeOpsLog(); slices.ContainsFunc(ops, func(op string) bool { return strings.HasPrefix(op, "umount:") }) {
		t.Errorf("setup failure quiesced a claim that was never handed out: %v", ops)
	}
	if !volumeDirty(path) {
		t.Error("setup failure cleared the dirty marker")
	}
	if _, err := m.ClaimWarm(t.Context(), testKey, 0, "", "", writable); !errors.Is(err, ErrNoWarm) {
		t.Fatalf("warm claim after a failed setup: %v, want ErrNoWarm", err)
	}
	eng.mountVolumeErr = nil
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable); err != nil {
		t.Errorf("claim after a failed setup: %v", err)
	}
}

func TestRollbackQuiescesWritableVolumesBeforeRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := writeVolumeImage(t, "scratch.img", "data")
		eng := newFakeEngine()
		eng.vms["sbx-rw"] = "/vsock/rw"
		m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
		if err := markVolumeDirty(path); err != nil {
			t.Fatalf("mark dirty: %v", err)
		}
		sb := &types.Sandbox{
			ID: "sb_rw", VMName: "sbx-rw", Key: testKey, VsockSocket: "/vsock/rw",
			Volumes: []types.Volume{{Name: "scratch", Mount: "/volumes/scratch", Mode: types.VolumeModeRW}},
		}
		m.mu.Lock()
		m.claimed[sb.ID] = sb
		m.adoptVolumes(sb.Volumes)
		m.mu.Unlock()

		m.rollbackClaim(t.Context(), []*types.Sandbox{sb})
		waitFor(t, m.store.synced)

		if seen := eng.opsAtRemoval(sb.VMName); !slices.Contains(seen, "umount:/volumes/scratch") {
			t.Errorf("ops at removal=%v, want the unmount already done", seen)
		}
		if volumeDirty(path) {
			t.Error("rollback left the image dirty after a clean unmount")
		}
		if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
			t.Errorf("registry after rollback=%+v, want empty", holders)
		}
	})
}

func TestReapQuiescesWritableVolumesBeforeRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := writeVolumeImage(t, "scratch.img", "data")
		eng := newFakeEngine()
		m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "", "", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
		if err != nil {
			t.Fatalf("ClaimProvision: %v", err)
		}
		m.mu.Lock()
		sb.Deadline = time.Now().Add(-time.Second)
		m.mu.Unlock()

		m.reapOnce(t.Context())
		waitFor(t, func() bool { return volumeHoldersOf(m, "scratch") == volumeHolders{} })

		if seen := eng.opsAtRemoval(sb.VMName); !slices.Contains(seen, "umount:/volumes/scratch") {
			t.Errorf("ops at removal=%v, want the unmount already done", seen)
		}
		if volumeDirty(path) {
			t.Error("reap left the image dirty after a clean unmount")
		}
	})
}

func TestQuiesceFailureSyncsOnceAndKeepsMarkers(t *testing.T) {
	first := writeVolumeImage(t, "first.img", "first")
	second := writeVolumeImage(t, "second.img", "second")
	eng := newFakeEngine()
	eng.unmountVolumeErr = errors.New("umount: target is busy")
	m := newVolumeManager(t, eng, []config.VolumeSpec{
		{Name: "first", Path: first, Writable: true},
		{Name: "second", Path: second, Writable: true},
	})
	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{
		{Name: "first", Mode: types.VolumeModeRW},
		{Name: "second", Mode: types.VolumeModeRW},
	})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}

	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := eng.syncCount(); got != 1 {
		t.Errorf("guest syncs=%d, want exactly one fallback for the whole quiesce", got)
	}
	if !volumeDirty(first) || !volumeDirty(second) {
		t.Error("failed unmounts cleared a dirty marker")
	}
	for _, name := range []string{"first", "second"} {
		if holders := volumeHoldersOf(m, name); holders != (volumeHolders{}) {
			t.Errorf("registry for %s after a failed unmount=%+v, want empty", name, holders)
		}
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "first"}}); !errors.Is(err, ErrVolumeNeedsRecovery) {
		t.Errorf("read-only claim after a failed unmount: %v, want ErrVolumeNeedsRecovery", err)
	}
}

func TestPendingRemovalHoldsVolumesUntilDrained(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}})
	writable := []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}
	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable)
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	eng.removeErrFor = sb.VMName // survives removal: the VM still holds the image

	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err == nil {
		t.Fatal("Release reported success for a VM that survived removal")
	}
	if !volumeDirty(path) {
		t.Error("marker cleared while the VM still holds the image")
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("claim against a surviving VM: %v, want ErrVolumeBusy", err)
	}

	eng.removeErrFor = ""
	m.retryRemovals(t.Context()).Wait()

	if volumeDirty(path) {
		t.Error("drained removal left the image dirty")
	}
	if holders := volumeHoldersOf(m, "scratch"); holders != (volumeHolders{}) {
		t.Errorf("registry after the drain=%+v, want empty", holders)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", writable); err != nil {
		t.Errorf("claim after the drain: %v", err)
	}
}

func TestQuiesceBudgetScalesWithMounts(t *testing.T) {
	for _, tt := range []struct {
		mounts int
		want   time.Duration
	}{
		{1, 2 * engine.VolumeCallTimeout},
		{2, 3 * engine.VolumeCallTimeout},
		{4, volumeQuiesceMax},
		{8, volumeQuiesceMax},
	} {
		if got := quiesceBudget(tt.mounts); got != tt.want {
			t.Errorf("quiesceBudget(%d)=%s, want %s", tt.mounts, got, tt.want)
		}
	}
}

func TestVolumeDirtyFailsClosed(t *testing.T) {
	image := filepath.Join(t.TempDir(), "scratch.img")
	if err := os.WriteFile(image, []byte("data"), 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	if volumeDirty(image) {
		t.Error("image without a marker reads dirty")
	}
	if err := markVolumeDirty(image); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}
	if !volumeDirty(image) {
		t.Error("marked image reads clean")
	}
	// The marker of an image behind a non-directory parent cannot be read at
	// all; an unreadable marker must never open the image to readers.
	if !volumeDirty(filepath.Join(image, "nested.img")) {
		t.Error("unreadable marker reads clean")
	}
}

func TestReconcileRebuildsVolumeAdmission(t *testing.T) {
	path := writeVolumeImage(t, "scratch.img", "data")
	dataDir := t.TempDir()
	catalog := []config.VolumeSpec{{Name: "scratch", Path: path, Writable: true}}
	eng := newFakeEngine()
	m := newVolumeManagerAt(t, eng, dataDir, catalog)
	sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "", "", []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}

	m2 := newVolumeManagerAt(t, eng, dataDir, catalog)
	if err := m2.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if holders := volumeHoldersOf(m2, "scratch"); holders != (volumeHolders{writers: 1}) {
		t.Errorf("adopted registry=%+v, want one writer", holders)
	}
	writable := []types.Volume{{Name: "scratch", Mode: types.VolumeModeRW}}
	if _, err := m2.ClaimProvision(t.Context(), testKey, 0, "", "", writable); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("claim against an adopted writer: %v, want ErrVolumeBusy", err)
	}
	if err := m2.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatalf("release adopted claim: %v", err)
	}
	if _, err := m2.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{{Name: "scratch"}}); err != nil {
		t.Errorf("claim after releasing the adopted writer: %v", err)
	}
}

func TestWritableDiscoveryAndUsageEvent(t *testing.T) {
	readOnly := writeVolumeImage(t, "data.img", "data")
	writable := writeVolumeImage(t, "scratch.img", "scratch")
	m := newVolumeManager(t, newFakeEngine(), []config.VolumeSpec{
		{Name: "data", Path: readOnly},
		{Name: "scratch", Path: writable, Writable: true},
	})

	want := []types.VolumeInfo{
		{Name: "data", DefaultMount: "/volumes/data", SizeBytes: int64(len("data")), Available: true, Nodes: 1},
		{Name: "scratch", DefaultMount: "/volumes/scratch", SizeBytes: int64(len("scratch")), Available: true, Nodes: 1, Writable: true},
	}
	if got := m.Volumes("", nil); !slices.Equal(got, want) {
		t.Errorf("catalog=%+v, want %+v", got, want)
	}

	sb, err := m.ClaimProvision(t.Context(), testKey, 0, "", "", []types.Volume{
		{Name: "data"},
		{Name: "scratch", Mode: types.VolumeModeRW},
	})
	if err != nil {
		t.Fatalf("ClaimProvision: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(m.dataDir, "usage.jsonl"))
	if err != nil {
		t.Fatalf("read usage journal: %v", err)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var event usageEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode usage event: %v", err)
		}
		if event.Event != "claim" || event.ID != sb.ID {
			continue
		}
		if !slices.Equal(event.Volumes, []string{"data", "scratch"}) || !slices.Equal(event.VolumesRW, []string{"scratch"}) {
			t.Errorf("claim event volumes=%v rw=%v, want [data scratch] and [scratch]", event.Volumes, event.VolumesRW)
		}
		return
	}
	t.Fatal("claim usage event not found")
}

func newVolumeManagerAt(t *testing.T, eng *fakeEngine, dataDir string, volumes []config.VolumeSpec) *Manager {
	t.Helper()
	m, err := NewManager(t.Context(), &config.Config{DataDir: dataDir, Volumes: volumes}, eng, testSecrets(t))
	if err != nil {
		t.Fatalf("setup manager: %v", err)
	}
	return m
}

func volumeHoldersOf(m *Manager, name string) volumeHolders {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volumeAdmission[name]
}
