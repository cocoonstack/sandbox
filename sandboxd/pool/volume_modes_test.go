package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestLiveVolumeFourModesAndMixedRoundtrip(t *testing.T) {
	eng := newFakeEngine()
	catalog := []config.VolumeSpec{}
	request := []types.Volume{{Name: "mounted"}, {Name: "writer", Mode: types.VolumeModeRW}, {Name: "raw", AttachOnly: true}, {Name: "raw-writer", Mode: types.VolumeModeRW, AttachOnly: true}}
	for _, v := range request {
		catalog = append(catalog, config.VolumeSpec{Name: v.Name, Path: writeVolumeImage(t, v.Name, "seed"), Writable: true})
	}
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	view, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: request})
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Volumes) != 4 || len(eng.volumeMounts) != 2 {
		t.Fatalf("mixed modes: view=%+v mounts=%+v", view, eng.volumeMounts)
	}
	for _, v := range view.Volumes {
		if v.AttachOnly != (v.Mount == "") {
			t.Fatalf("lost canonical shape: %+v", v)
		}
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var echoed types.SandboxVolumesResponse
	if err = json.Unmarshal(raw, &echoed); err != nil {
		t.Fatal(err)
	}
	before := slices.Clone(eng.volumeOpsLog())
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", echoed.VolumesWant); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(before, eng.volumeOpsLog()) {
		t.Fatal("GET-to-PUT roundtrip changed devices")
	}
	for _, change := range []types.Volume{{Name: "raw", Mount: "/new"}, {Name: "raw-writer", AttachOnly: true}, {Name: "writer", Mount: "/other", Mode: types.VolumeModeRW}} {
		if _, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{change}}); !errors.Is(err, ErrBadVolume) {
			t.Fatalf("held shape change accepted: %+v, %v", change, err)
		}
	}
	legacy := slices.Clone(view.VolumesWant)
	for i := range legacy {
		legacy[i].AttachOnly = false
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", legacy); err != nil {
		t.Fatalf("legacy held omission: %v", err)
	}
	assertLiveJournal(t, m, sb)
	restarted := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err = restarted.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.GetVolumes(sb.ID, "")
	if err != nil || !slices.Equal(recovered.Volumes, view.Volumes) {
		t.Fatalf("mixed restart: %+v, %v", recovered, err)
	}
	if _, err = restarted.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "raw-writer", AttachOnly: true}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart lost writer exclusion: %v", err)
	}
	if _, err = restarted.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
		t.Fatal(err)
	}
	for _, spec := range catalog {
		if volumeDirty(spec.Path) || volumeHoldersOf(restarted, spec.Name) != (volumeHolders{}) {
			t.Fatalf("detach leaked marker or hold: %s", spec.Name)
		}
	}
}

func TestLiveVolumeLegacyAttachOnlyRetryAfterFailedDetach(t *testing.T) {
	for _, mode := range []string{"", types.VolumeModeRW} {
		for _, method := range []string{"put", "patch"} {
			t.Run(fmt.Sprintf("%s/%s", map[string]string{"": "ro", types.VolumeModeRW: "rw"}[mode], method), func(t *testing.T) {
				eng := newFakeEngine()
				m := newVolumeManager(t, eng, liveCatalog(t))
				sb := mustClaim(t, m, testKey)
				want := []types.Volume{{Name: "data", Mode: mode, AttachOnly: true}}
				if _, err := m.SetVolumes(t.Context(), sb.ID, "", want); err != nil {
					t.Fatal(err)
				}
				eng.detachVolumeErr = errors.New("host detach unavailable")
				view, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
				if err == nil || len(view.VolumesWant) != 0 || !slices.Equal(view.Volumes, want) {
					t.Fatalf("failed detach lost applied device: %+v, %v", view, err)
				}
				eng.detachVolumeErr = nil
				before := slices.Clone(eng.volumeOpsLog())
				change := func(volumes []types.Volume) (types.SandboxVolumesResponse, error) {
					if method == "patch" {
						return m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: volumes})
					}
					return m.SetVolumes(t.Context(), sb.ID, "", volumes)
				}
				if _, err = change([]types.Volume{{Name: "data", Mode: mode, Mount: "/changed"}}); !errors.Is(err, ErrBadVolume) {
					t.Fatalf("held attachment changed shape: %v", err)
				}
				if !slices.Equal(before, eng.volumeOpsLog()) {
					t.Fatal("rejected shape change operated on the device")
				}
				legacy := slices.Clone(want)
				legacy[0].AttachOnly = false
				view, err = change(legacy)
				if err != nil {
					t.Fatalf("legacy echo of held attach-only volume rejected: %v", err)
				}
				if !slices.Equal(view.VolumesWant, want) || !slices.Equal(view.Volumes, want) || view.Converging {
					t.Fatalf("legacy retry did not retain the applied device: %+v", view)
				}
				before = slices.Clone(eng.volumeOpsLog())
				if _, err = change(legacy); err != nil {
					t.Fatalf("settled legacy retry: %v", err)
				}
				if !slices.Equal(before, eng.volumeOpsLog()) {
					t.Fatalf("settled retry changed the retained device: %v -> %v", before, eng.volumeOpsLog())
				}
			})
		}
	}
}

func TestLiveVolumeLegacyAttachOnlyPatchAfterFailedCommit(t *testing.T) {
	eng := newFakeEngine()
	m := newVolumeManager(t, eng, liveCatalog(t))
	sb := mustClaim(t, m, testKey)
	want := []types.Volume{{Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", want); err != nil {
		t.Fatal(err)
	}
	m.store.recommitting.Store(true)
	tmp := m.store.path + ".tmp"
	eng.liveVolumeHook = func(verb string) error {
		if verb == "detach" {
			return os.Mkdir(tmp, 0o700)
		}
		return nil
	}
	view, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
	eng.liveVolumeHook = nil
	if err == nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 || !view.Converging {
		t.Fatalf("failed detach commit did not retain a recovery reference: %+v, %v", view, err)
	}
	if err = os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	legacy := []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}
	view, err = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: legacy})
	if err != nil {
		t.Fatalf("legacy echo of recovery-only attach-only volume rejected: %v", err)
	}
	if !slices.Equal(view.VolumesWant, want) || !slices.Equal(view.Volumes, want) || view.Converging {
		t.Fatalf("legacy retry lost recovery attachment shape: %+v", view)
	}
}

func TestLiveWritableDetachRequiresCleanEvidence(t *testing.T) {
	for _, proof := range []bool{false, true} {
		t.Run(fmt.Sprint(proof), func(t *testing.T) {
			base := newFakeEngine()
			catalog := liveCatalog(t)
			m := newVolumeManager(t, base, catalog)
			m.eng = &detachProofEngine{fakeEngine: base, proof: proof}
			sb := mustClaim(t, m, testKey)
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}); err != nil {
				t.Fatal(err)
			}
			path := catalog[0].Path
			if !volumeDirty(path) || !base.attachDirty["data"] {
				t.Fatal("writer was attached before dirty marker")
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
				t.Fatal(err)
			}
			if volumeDirty(path) == proof {
				t.Fatalf("marker disagrees with clean evidence: proof=%t dirty=%t", proof, volumeDirty(path))
			}
			_, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data"}}})
			if proof && err != nil || !proof && !errors.Is(err, ErrVolumeNeedsRecovery) {
				t.Fatalf("reader admission after detach: %v", err)
			}
		})
	}
}

func TestLiveWritableFailedHostDetachKeepsMarkerAndHold(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}); err != nil {
		t.Fatal(err)
	}
	eng.detachVolumeErr = errors.New("host removal uncertain")
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("failed host detach reported success")
	}
	if !volumeDirty(catalog[0].Path) || volumeHoldersOf(m, "data").writers != 1 {
		t.Fatal("failed host detach lost dirty marker or writer")
	}
	if _, err := m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "uncertain", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("capture after uncertain detach: %v", err)
	}
	eng.detachVolumeErr = nil
	m.eng = &detachProofEngine{fakeEngine: eng}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
		t.Fatal(err)
	}
	if !volumeDirty(catalog[0].Path) {
		t.Fatal("absent-device retry invented clean evidence")
	}
}

func TestLiveWritableMarkerClearFailureRemainsConservative(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}); err != nil {
		t.Fatal(err)
	}
	marker := volumeDirtyPath(catalog[0].Path)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(marker, "block"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
		t.Fatal(err)
	}
	if !volumeDirty(catalog[0].Path) || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("failed marker deletion changed physical retirement or reported clean")
	}
}

type detachProofEngine struct {
	*fakeEngine
	proof bool
}

func (e *detachProofEngine) DetachVolume(ctx context.Context, vm, sock, name, mount string) (engine.VolumeDetachResult, error) {
	_, err := e.fakeEngine.DetachVolume(ctx, vm, sock, name, mount)
	if err != nil {
		return engine.VolumeDetachResult{}, err
	}
	return engine.VolumeDetachResult{NormalUnmount: e.proof}, nil
}

func TestLiveWritableUncertainAdditionClearsOnlyAfterDetach(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	eng.mountVolumeErr = errors.New("mount response lost")
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}); err == nil {
		t.Fatal("expected unknown mount result")
	}
	if len(sb.Volumes) != 0 || len(sb.VolumesWant) != 1 || !volumeDirty(catalog[0].Path) {
		t.Fatal("pending writable attachment lost intent or marker")
	}
	eng.mountVolumeErr = nil
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
		t.Fatal(err)
	}
	if volumeDirty(catalog[0].Path) || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatal("uncertain addition cleanup retained marker or hold")
	}
}

func TestLiveVolumeModesKeepRecoveryAfterDetachWriteFailure(t *testing.T) {
	for _, v := range []types.Volume{{Name: "data"}, {Name: "data", Mode: types.VolumeModeRW}, {Name: "data", AttachOnly: true}, {Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}} {
		t.Run(fmt.Sprintf("rw=%t/raw=%t", v.RW(), v.AttachOnly), func(t *testing.T) {
			eng := newFakeEngine()
			catalog := liveCatalog(t)
			m := newVolumeManager(t, eng, catalog)
			sb := mustClaim(t, m, testKey)
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{v}); err != nil {
				t.Fatal(err)
			}
			m.store.recommitting.Store(true)
			tmp := m.store.path + ".tmp"
			eng.liveVolumeHook = func(verb string) error {
				if verb == "detach" {
					return os.Mkdir(tmp, 0o700)
				}
				return nil
			}
			view, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
			eng.liveVolumeHook = nil
			if err == nil || len(view.Volumes) != 0 {
				t.Fatalf("detach result failure: %+v, %v", view, err)
			}
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
				t.Fatalf("writer passed unretired %v: %v", v, err)
			}
			if _, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "unretired", ""); !errors.Is(err, ErrVolumeCapture) {
				t.Fatalf("capture passed unretired %v: %v", v, err)
			}
			if err = os.Remove(tmp); err != nil {
				t.Fatal(err)
			}
			restarted := newVolumeManagerAt(t, eng, m.dataDir, catalog)
			if err = restarted.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err = restarted.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err != nil {
				t.Fatal(err)
			}
			if _, err = restarted.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
				t.Fatalf("retired reference still blocks writer: %v", err)
			}
		})
	}
}

func TestLiveWritableReleaseWaitsForLateDetachBeforeNewWriter(t *testing.T) {
	base := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, base, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}); err != nil {
		t.Fatal(err)
	}
	eng := &gatedDetachEngine{fakeEngine: base, entered: make(chan struct{}), resume: make(chan struct{})}
	m.eng = eng
	changed := make(chan error, 1)
	go func() { _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); changed <- err }()
	<-eng.entered
	resumed := false
	defer func() {
		if !resumed {
			close(eng.resume)
			<-changed
		}
	}()
	if !volumeDirty(catalog[0].Path) || volumeHoldersOf(m, "data").writers != 1 {
		t.Fatal("host result released marker or hold before consumption")
	}
	if !m.mu.TryLock() {
		t.Fatal("host call holds manager mutex")
	}
	m.mu.Unlock()
	if !sb.Transition.TryLock() {
		t.Fatal("host call holds transition mutex")
	}
	sb.Transition.Unlock()
	if err := m.Release(t.Context(), sb.ID, Cred{Token: sb.Token}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("writer overtook canceled detach worker: %v", err)
	}
	close(eng.resume)
	resumed = true
	if err := <-changed; err == nil {
		t.Fatal("released claim consumed late detach as successful mutation")
	}
	m.retryRemovals(t.Context()).Wait()
	m.eng = base
	if _, err := m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
		t.Fatal(err)
	}
	m.retryRemovals(t.Context()).Wait()
	if !volumeDirty(catalog[0].Path) {
		t.Fatal("old cleanup erased new writer marker")
	}
}

type gatedDetachEngine struct {
	*fakeEngine
	entered chan struct{}
	resume  chan struct{}
}

func (e *gatedDetachEngine) DetachVolume(ctx context.Context, vm, sock, name, mount string) (engine.VolumeDetachResult, error) {
	result, err := e.fakeEngine.DetachVolume(ctx, vm, sock, name, mount)
	close(e.entered)
	<-e.resume
	return result, err
}

func TestLiveVolumeModesRetryFailedIntentAndAttachWrites(t *testing.T) {
	for _, stage := range []string{"intent", "attached"} {
		for _, v := range []types.Volume{{Name: "data"}, {Name: "data", Mode: types.VolumeModeRW}, {Name: "data", AttachOnly: true}, {Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}} {
			t.Run(fmt.Sprintf("%s/rw=%t/raw=%t", stage, v.RW(), v.AttachOnly), func(t *testing.T) {
				eng := newFakeEngine()
				m := newVolumeManager(t, eng, liveCatalog(t))
				sb := mustClaim(t, m, testKey)
				m.store.recommitting.Store(true)
				fail := func([]byte) error { return errors.New("claim write unavailable") }
				if stage == "intent" {
					m.store.replaceFile = fail
				} else {
					eng.liveVolumeHook = func(verb string) error {
						if verb == "attach" {
							m.store.replaceFile = fail
						}
						return nil
					}
				}
				if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{v}); err == nil {
					t.Fatal("failed claim write reported success")
				}
				if len(sb.VolumesWant) != 1 || sb.VolumesWant[0].AttachOnly != v.AttachOnly {
					t.Fatal("failed write lost per-entry intent")
				}
				if stage == "intent" && len(eng.volumeSpecs) != 0 {
					t.Fatal("failed intent reached device call")
				}
				eng.liveVolumeHook = nil
				m.store.replaceFile = nil
				if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{v}); err != nil {
					t.Fatalf("same target failed to recover: %v", err)
				}
				assertLiveJournal(t, m, sb)
				holds := volumeHoldersOf(m, "data")
				if holds.readers+holds.writers != 1 {
					t.Fatalf("retry duplicated hold: %+v", holds)
				}
			})
		}
	}
}

func TestLiveVolumeModesCanceledAttachRetainsRetryableIntent(t *testing.T) {
	for _, v := range []types.Volume{{Name: "data"}, {Name: "data", Mode: types.VolumeModeRW}, {Name: "data", AttachOnly: true}, {Name: "data", Mode: types.VolumeModeRW, AttachOnly: true}} {
		t.Run(fmt.Sprintf("rw=%t/raw=%t", v.RW(), v.AttachOnly), func(t *testing.T) {
			base := newFakeEngine()
			m := newVolumeManager(t, base, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			eng := newGatedVolumeEngine(base)
			m.eng = eng
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := m.SetVolumes(ctx, sb.ID, "", []types.Volume{v}); done <- err }()
			<-eng.entered
			cancel()
			close(eng.resume)
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled attachment outcome: %v", err)
			}
			if len(sb.VolumesWant) != 1 || volumeHoldersOf(m, "data").readers+volumeHoldersOf(m, "data").writers != 1 {
				t.Fatal("canceled attachment dropped intent or hold")
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{v}); err != nil {
				t.Fatalf("canceled request retry: %v", err)
			}
			assertLiveJournal(t, m, sb)
		})
	}
}
