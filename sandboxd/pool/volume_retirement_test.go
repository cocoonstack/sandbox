package pool

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

func TestUnacknowledgedDetachKeepsRecoveryReference(t *testing.T) {
	eng := newFakeEngine()
	m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
	spec := types.VolumeRegistration{Path: writeVolumeImage(t, "home.img", "private"), Writable: true, Tenants: []string{"acme"}}
	if err := m.RegisterVolume(t.Context(), "home", spec); err != nil {
		t.Fatal(err)
	}
	want := []types.Volume{{Name: "home", Mount: "/home"}}
	for _, caller := range []string{"", "acme"} {
		if _, err := m.SetVolumes(t.Context(), sb.ID, caller, want); err != nil {
			t.Fatal(err)
		}
	}
	unblock := detachWithoutCommit(t, m, eng, sb.ID, "acme")
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil || claims[sb.ID] == nil || !slices.Equal(claims[sb.ID].Volumes, want) {
		t.Fatalf("old durable attachment missing: %+v, %v", claims, err)
	}
	view, err := m.GetVolumes(sb.ID, "")
	if err != nil || len(view.States) != 1 || view.States[0].State != types.VolumeDetaching || !view.Converging {
		t.Fatalf("unretired view: %+v, %v", view, err)
	}
	writer := ClaimOptions{Volumes: []types.Volume{{Name: "home", Mode: types.VolumeModeRW}}}
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"writer", errOf(m.ClaimProvision(t.Context(), testKey, writer)), ErrVolumeBusy},
		{"checkpoint", errOf(m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "unretired", "")), ErrVolumeCapture},
		{"hibernate", m.Hibernate(t.Context(), sb.ID, Cred{Token: sb.Token}), ErrVolumeCapture},
		{"unregister", m.UnregisterVolume(t.Context(), "home"), ErrVolumeRegistrationConflict},
		{"rebind", m.RegisterVolume(t.Context(), "home", types.VolumeRegistration{Path: writeVolumeImage(t, "different", "bytes")}), ErrVolumeRegistrationConflict},
		{"alias", m.RegisterVolume(t.Context(), "same-file", spec), ErrVolumeRegistrationConflict},
		{"reshape", errOf(m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "home", Mount: "/different"}})), ErrBadVolume},
	} {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s with unretired reference = %v, want %v", tc.name, tc.err, tc.want)
		}
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "acme", nil); err == nil {
		t.Fatal("empty retry acknowledged an unretired file")
	}
	unblock()
	setVolumeTenants(m, "home", "other")
	before := len(eng.volumeSpecs)
	_, putErr := m.SetVolumes(t.Context(), sb.ID, "acme", want)
	_, patchErr := m.PatchVolumes(t.Context(), sb.ID, "acme", types.VolumePatch{Add: want})
	if !errors.Is(putErr, ErrVolumeUnavailable) || !errors.Is(patchErr, ErrVolumeUnavailable) || len(eng.volumeSpecs) != before {
		t.Fatalf("recovery-only reference authorized a fresh attachment: put=%v patch=%v", putErr, patchErr)
	}
	setVolumeTenants(m, "home", "acme")
	for range 2 {
		if _, err = m.SetVolumes(t.Context(), sb.ID, "acme", want); err != nil {
			t.Fatal(err)
		}
		if got := volumeHoldersOf(m, "home"); got != (volumeHolders{readers: 1}) {
			t.Fatalf("own retry counts: %+v", got)
		}
	}
	detachWithoutCommit(t, m, eng, sb.ID, "acme")()
	if err = m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	_, putErr = m.SetVolumes(t.Context(), sb.ID, "", want)
	_, patchErr = m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "data"}}})
	if !errors.Is(putErr, ErrTenantRemoved) || !errors.Is(patchErr, ErrTenantRemoved) || volumeHoldersOf(m, "home") != (volumeHolders{}) {
		t.Fatalf("recovery-only retry bypassed tenant removal: put=%v patch=%v", putErr, patchErr)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
		t.Fatalf("retirement after tenant removal: %v", err)
	}
	if err = claimAndRelease(t, m, writer.Volumes...); err != nil {
		t.Fatalf("acknowledged detach kept the reader exclusion: %v", err)
	}
	if err = errors.Join(m.UnregisterVolume(t.Context(), "home"), m.RegisterVolume(t.Context(), "same-file", spec)); err != nil {
		t.Fatalf("retired name or path stayed reserved: %v", err)
	}
}

func TestPossibleClaimIntroductionRemainsProtected(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil {
		t.Fatal(err)
	}
	restoreWrites := failClaimWrites(t, m.store)
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err == nil {
		t.Fatal("expected failed intent write")
	}
	claims[sb.ID].VolumesWant = []types.Volume{{Name: "data", Mount: types.DefaultVolumeMount("data"), Mode: types.VolumeModeRO}}
	raw, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	// model an intent whose rename succeeded before an uncertain directory sync
	if err = utils.WriteFileSync(m.store.path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("expected empty snapshot error")
	}
	if len(sb.Volumes) != 0 || len(sb.VolumesWant) != 0 || volumeHoldersOf(m, "data") != (volumeHolders{}) {
		t.Fatalf("memory not empty: %+v", sb)
	}
	writer := ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}
	if _, err = m.ClaimProvision(t.Context(), testKey, writer); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("writer admitted after uncertain introduction: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "uncertain-introduction", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Fatalf("capture admitted: %v", err)
	}
	restoreWrites()
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, writer); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart admitted writer: %v", err)
	}
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if _, err = m.ClaimProvision(t.Context(), testKey, writer); err != nil {
		t.Fatalf("successful whole snapshot did not retire attempted row: %v", err)
	}
}

func TestClaimRecoveryReferencesRetireOnlyOnAcknowledgedWrite(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", Volumes: []types.Volume{{Name: "data", Mount: "/data"}}}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	restoreWrites := failClaimWrites(t, s)
	for i := range 200 {
		sb.Token = fmt.Sprint(i)
		sb.Volumes[0].Mount = []string{"/data", "/other"}[i%2]
		if err := s.commit(s.set(sb)); err == nil {
			t.Fatal("expected failed snapshot")
		}
	}
	refs := s.recoveryVolumesFor(sb.ID)
	refs[0].Name = "caller-copy"
	if got := s.recoveryVolumesFor(sb.ID); len(got) != 2 || got[0].Name != "data" {
		t.Fatalf("recovery references duplicated or shared: %+v", got)
	}
	restoreWrites()
	s = newClaimStore(filepath.Dir(s.path), false)
	if _, err := s.load(); err != nil {
		t.Fatal(err)
	}
	s.reset(map[string]*types.Sandbox{})
	if got := s.recoveryVolumesFor(sb.ID); len(got) != 1 || got[0].Name != "data" {
		t.Fatalf("reset erased the file reference: %+v", got)
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.hasRecoveryVolumes(sb.ID) {
		t.Fatal("empty snapshot did not retire file references")
	}
}

func TestClaimSnapshotAcknowledgesCapturedRowsBeforeNewerReplacement(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	applied := []types.Volume{{Name: "data", Mount: "/data"}}
	wanted := []types.Volume{{Name: "wanted", Mount: "/wanted"}}
	sb := &types.Sandbox{ID: "owner", Volumes: slices.Clone(applied), VolumesWant: slices.Clone(wanted)}
	old := s.set(sb)
	release := captureClaimWrite(t, s, old, sb.ID)
	newer := s.del(sb.ID)
	sb.Volumes[0].Name = "mutated-applied"
	sb.VolumesWant[0].Name = "mutated-wanted"
	raw := release()
	if !s.pending(newer) {
		t.Fatal("captured snapshot acknowledged newer deletion")
	}
	if refs := s.recoveryVolumesFor(sb.ID); !slices.Equal(refs, append(slices.Clone(applied), wanted...)) {
		t.Fatalf("captured row not acknowledged: %+v", refs)
	}
	var persisted map[string]*types.Sandbox
	err := json.Unmarshal(raw, &persisted)
	if err != nil || persisted[sb.ID] == nil || !slices.Equal(persisted[sb.ID].Volumes, applied) || !slices.Equal(persisted[sb.ID].VolumesWant, wanted) {
		t.Fatalf("captured write: %+v, %v", persisted, err)
	}
	if err = s.commit(newer); err != nil {
		t.Fatal(err)
	}
	if s.hasRecoveryVolumes(sb.ID) {
		t.Fatal("empty replacement retained old reference")
	}
	failClaimWrites(t, s)
	if err = s.commit(old); err != nil {
		t.Fatal(err)
	}
	persisted, err = newClaimStore(filepath.Dir(s.path), false).load()
	if err != nil || len(persisted) != 0 {
		t.Fatalf("old token resurrected snapshot: %+v, %v", persisted, err)
	}
}

func TestClaimRecoveryVolumeConflicts(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	reader := types.Volume{Name: "shared", Mount: "/data"}
	writer, moved := types.Volume{Name: "data", Mount: "/data", Mode: types.VolumeModeRW}, types.Volume{Name: "data", Mount: "/elsewhere", Mode: types.VolumeModeRW}
	first, second := &types.Sandbox{ID: "first", Volumes: []types.Volume{reader}}, &types.Sandbox{ID: "second", Volumes: []types.Volume{reader}}
	owner := &types.Sandbox{ID: "owner", Volumes: []types.Volume{writer}}
	if err := s.commit(s.set(first, second, owner)); err != nil {
		t.Fatal(err)
	}
	restore := failClaimWrites(t, s)
	owner.VolumesWant = []types.Volume{{Name: "other", Mount: "/other"}}
	if err := s.commit(s.set(owner)); err == nil {
		t.Fatal("expected write failure")
	}
	sharedWriter, ownerReader := reader, writer
	sharedWriter.Mode, ownerReader.Mode = types.VolumeModeRW, types.VolumeModeRO
	for _, tc := range []struct {
		volume   types.Volume
		owner    string
		conflict bool
	}{
		{reader, "", false},
		{sharedWriter, "", true},
		{sharedWriter, first.ID, true},
		{writer, owner.ID, false},
		{writer, "different", true},
		{ownerReader, owner.ID, true},
		{moved, owner.ID, true},
	} {
		if got := s.volumeConflicts(tc.volume, tc.owner); got != tc.conflict {
			t.Errorf("volume %+v owner %q conflict=%t, want %t", tc.volume, tc.owner, got, tc.conflict)
		}
	}
	s.set(&types.Sandbox{ID: owner.ID, Volumes: []types.Volume{writer}})
	restore()
	if err := s.commit(s.del(first.ID)); err != nil {
		t.Fatal(err)
	}
	if !s.referencesVolume(reader.Name) || s.hasRecoveryVolumes(first.ID) || !s.volumeConflicts(sharedWriter, first.ID) {
		t.Fatal("retiring one reader released the other")
	}
	if s.referencesVolume("other") || s.volumeConflicts(writer, owner.ID) {
		t.Fatal("successful return retained attempted attachment")
	}
	if err := s.commit(s.del(second.ID)); err != nil {
		t.Fatal(err)
	}
	if s.referencesVolume(reader.Name) || s.volumeConflicts(sharedWriter, "") {
		t.Fatal("last reader left recovery reservation")
	}
}

func TestClaimCaptureAcknowledgesBothReferenceDomains(t *testing.T) {
	for _, tc := range []struct {
		name                string
		drop                func(*types.Sandbox)
		checkpoint, volumes bool
	}{
		{"volumes", func(sb *types.Sandbox) { sb.Volumes = nil }, true, false},
		{"checkpoints", func(sb *types.Sandbox) { sb.ReplicaCk = "" }, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newClaimStore(t.TempDir(), false)
			sb := &types.Sandbox{ID: "owner", ReplicaCk: "checkpoint", Volumes: []types.Volume{{Name: "data"}}}
			release := captureClaimWrite(t, s, s.set(sb), sb.ID)
			tc.drop(sb)
			newer := s.set(sb)
			release()
			if !s.pending(newer) || !s.referencesCheckpoint("checkpoint") || !s.hasRecoveryVolumes(sb.ID) {
				t.Fatal("older capture retired newer state or its recovery references")
			}
			if err := s.commit(newer); err != nil {
				t.Fatal(err)
			}
			if s.referencesCheckpoint("checkpoint") != tc.checkpoint || s.hasRecoveryVolumes(sb.ID) != tc.volumes {
				t.Fatalf("references after newer commit: checkpoint=%t volumes=%t", s.referencesCheckpoint("checkpoint"), s.hasRecoveryVolumes(sb.ID))
			}
		})
	}
}

// detachWithoutCommit detaches every volume on the device while the claim write fails until unblock.
func detachWithoutCommit(t *testing.T, m *Manager, eng *fakeEngine, id, tenant string) (unblock func()) {
	t.Helper()
	m.store.recommitting.Store(true)
	tmp := m.store.path + ".tmp"
	eng.liveVolumeHook = func(verb string) error {
		if verb == "detach" {
			return os.Mkdir(tmp, 0o700)
		}
		return nil
	}
	view, err := m.SetVolumes(t.Context(), id, tenant, nil)
	eng.liveVolumeHook = nil
	if err == nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
		t.Fatalf("detach did not fail after device removal: %+v, %v", view, err)
	}
	return func() {
		if err := os.Remove(tmp); err != nil {
			t.Fatal(err)
		}
	}
}

func errOf(_ any, err error) error { return err }

func setVolumeTenants(m *Manager, name string, tenants ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := *m.catalog.entries[name]
	entry.tenants = tenants
	m.catalog.entries[name] = &entry
}
