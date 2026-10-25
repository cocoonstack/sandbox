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

func TestDetachedVolumeRetainsRecoveryProtectionUntilCommit(t *testing.T) {
	eng := newFakeEngine()
	catalog := liveCatalog(t)
	m := newVolumeManager(t, eng, catalog)
	sb := mustClaim(t, m, testKey)
	if _, err := m.SetVolumes(t.Context(), sb.ID, sb.Tenant, []types.Volume{{Name: "data"}}); err != nil {
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
	view, err := m.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}})
	eng.liveVolumeHook = nil
	if err == nil || len(view.Volumes) != 0 || len(view.VolumesWant) != 0 {
		t.Fatalf("detached result = %+v, %v", view, err)
	}
	claims, err := newClaimStore(m.dataDir, false).load()
	if err != nil || claims[sb.ID] == nil || len(claims[sb.ID].Volumes) != 1 {
		t.Fatalf("failed write did not leave old recovery reference: %+v, %v", claims, err)
	}
	if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Errorf("writer admitted while old claim still references detached volume: %v", err)
	}
	if _, err = m.Checkpoint(t.Context(), sb.ID, Cred{Token: sb.Token}, "unretired", ""); !errors.Is(err, ErrVolumeCapture) {
		t.Errorf("capture passed before recovery reference retired: %v", err)
	}
	if err = os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	m2 := newVolumeManagerAt(t, eng, m.dataDir, catalog)
	if err = m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart lost old claim's reader exclusion: %v", err)
	}
	if _, err = m2.PatchVolumes(t.Context(), sb.ID, sb.Tenant, types.VolumePatch{Remove: []string{"data"}}); err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
		t.Fatalf("writer still blocked after successful retirement: %v", err)
	}
}

func TestDetachedVolumeOwnRetryAndLaterAcknowledgment(t *testing.T) {
	for _, retry := range []string{"empty", "same-shape", "unrelated-snapshot"} {
		t.Run(retry, func(t *testing.T) {
			eng := newFakeEngine()
			m := newVolumeManager(t, eng, liveCatalog(t))
			sb := mustClaim(t, m, testKey)
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
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
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
				t.Fatal("detach result unexpectedly persisted")
			}
			eng.liveVolumeHook = nil
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("physical count retained after detach: %+v", got)
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
				t.Fatal("empty retry acknowledged an unretired file")
			}
			if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data", Mount: "/different"}}); !errors.Is(err, ErrBadVolume) {
				t.Fatalf("recovery-only shape changed: %v", err)
			}
			view, err := m.GetVolumes(sb.ID, "")
			if err != nil || len(view.States) != 1 || view.States[0].State != types.VolumeDetaching {
				t.Fatalf("unretired view: %+v, %v", view, err)
			}
			if err = os.Remove(tmp); err != nil {
				t.Fatal(err)
			}
			switch retry {
			case "empty":
				_, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
			case "same-shape":
				for range 2 {
					if _, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); err != nil {
						t.Fatal(err)
					}
					if got := volumeHoldersOf(m, "data"); got.readers != 1 || got.writers != 0 {
						t.Fatalf("own retry counts: %+v", got)
					}
				}
				_, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{})
			case "unrelated-snapshot":
				_, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
				t.Fatalf("retired count: %+v", got)
			}
			if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); err != nil {
				t.Fatalf("writer after acknowledgment: %v", err)
			}
		})
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
	// Model an intent whose rename succeeded before an uncertain directory sync.
	if err = utils.WriteFileSync(m.store.path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{}); err == nil {
		t.Fatal("expected empty snapshot error")
	}
	if len(sb.Volumes) != 0 || len(sb.VolumesWant) != 0 {
		t.Fatalf("memory not empty: %+v", sb)
	}
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("unexpected physical count: %+v", got)
	}
	claims, err = newClaimStore(m.dataDir, false).load()
	if err != nil || len(claims[sb.ID].VolumesWant) != 1 {
		t.Fatalf("actual recovery file: %+v, %v", claims, err)
	}
	if _, err = m.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
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
	if _, err = m2.ClaimProvision(t.Context(), testKey, ClaimOptions{Volumes: []types.Volume{{Name: "data", Mode: types.VolumeModeRW}}}); !errors.Is(err, ErrVolumeBusy) {
		t.Fatalf("restart admitted writer: %v", err)
	}
	if err = m.store.commit(m.store.mark()); err != nil {
		t.Fatal(err)
	}
	if len(m.store.recoveryVolumesFor(sb.ID)) != 0 {
		t.Fatal("successful whole snapshot did not retire attempted row")
	}
}

func TestClaimRecoveryReferencesSurviveResetAndAreCopied(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	sb := &types.Sandbox{ID: "owner", Volumes: []types.Volume{{Name: "data", Mount: "/data"}}}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	s = newClaimStore(filepath.Dir(s.path), false)
	if _, err := s.load(); err != nil {
		t.Fatal(err)
	}
	s.reset(map[string]*types.Sandbox{})
	refs := s.recoveryVolumesFor(sb.ID)
	refs[0].Name = "caller-copy"
	if got := s.recoveryVolumesFor(sb.ID); len(got) != 1 || got[0].Name != "data" {
		t.Fatalf("reset or caller erased actual file: %+v", got)
	}
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.hasRecoveryVolumes(sb.ID) {
		t.Fatal("empty snapshot did not retire file references")
	}
}

func TestClaimRecoveryAttemptsRetainOnlyDistinctReferences(t *testing.T) {
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
	if refs := s.recoveryVolumesFor(sb.ID); len(refs) != 2 {
		t.Fatalf("renewals retained duplicate history: %+v", refs)
	}
	refs := s.recoveryVolumesFor(sb.ID)
	refs[0].Name = "caller-copy"
	if got := s.recoveryVolumesFor(sb.ID); len(got) != 2 || got[0].Name != "data" {
		t.Fatalf("recovery references not copied: %+v", got)
	}
	restoreWrites()
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if len(s.recoveryVolumesFor(sb.ID)) != 1 {
		t.Fatal("successful snapshot did not replace possibility history")
	}
}

func TestRecoveryOnlyRetryRequiresNewAuthorization(t *testing.T) {
	eng := newFakeEngine()
	m, sb := newTenantVolumeManager(t, eng, liveCatalog(t))
	if _, err := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{{Name: "data"}}); err != nil {
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
	if _, err := m.SetVolumes(t.Context(), sb.ID, "acme", []types.Volume{}); err == nil {
		t.Fatal("expected failed detach result commit")
	}
	eng.liveVolumeHook = nil
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	entry := *m.catalog.entries["data"]
	entry.tenants = []string{"different-tenant"}
	m.catalog.entries["data"] = &entry
	m.mu.Unlock()
	if err := os.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", []types.Volume{{Name: "data"}}); !errors.Is(err, ErrTenantRemoved) {
		t.Fatalf("recovery-only retry bypassed tenant removal: %v", err)
	}
	if got := volumeHoldersOf(m, "data"); got != (volumeHolders{}) {
		t.Fatalf("rejected recovery retry reserved a reader: %+v", got)
	}
	if _, err := m.SetVolumes(t.Context(), sb.ID, "", nil); err != nil {
		t.Fatalf("recovery retirement after tenant removal: %v", err)
	}
	if _, err := m.PatchVolumes(t.Context(), sb.ID, "", types.VolumePatch{Add: []types.Volume{{Name: "other"}}}); !errors.Is(err, ErrTenantRemoved) {
		t.Fatalf("repair exemption admitted unrelated addition: %v", err)
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
	restoreWrites := failClaimWrites(t, s)
	defer restoreWrites()
	if err = s.commit(old); err != nil {
		t.Fatal(err)
	}
	persisted, err = newClaimStore(filepath.Dir(s.path), false).load()
	if err != nil || len(persisted) != 0 {
		t.Fatalf("old token resurrected snapshot: %+v, %v", persisted, err)
	}
}

func TestClaimSharedRecoveryVolumesRetirePerOwner(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	volume := types.Volume{Name: "shared", Mount: "/data"}
	first := &types.Sandbox{ID: "first", Volumes: []types.Volume{volume}}
	second := &types.Sandbox{ID: "second", Volumes: []types.Volume{volume}}
	writer := volume
	writer.Mode = types.VolumeModeRW
	if err := s.commit(s.set(first, second)); err != nil {
		t.Fatal(err)
	}
	if s.volumeConflicts(volume, "") || !s.volumeConflicts(writer, "") {
		t.Fatal("shared readers have incorrect admission")
	}
	if err := s.commit(s.del(first.ID)); err != nil {
		t.Fatal(err)
	}
	if !s.referencesVolume(volume.Name) || s.hasRecoveryVolumes(first.ID) || !s.hasRecoveryVolumes(second.ID) || !s.volumeConflicts(writer, first.ID) {
		t.Fatal("retiring one reader released the other")
	}
	if err := s.commit(s.del(second.ID)); err != nil {
		t.Fatal(err)
	}
	if s.referencesVolume(volume.Name) || s.volumeConflicts(writer, "") {
		t.Fatal("last reader left recovery reservation")
	}
}

func TestClaimRecoveryVolumeOwnerExemptionRequiresExactAttachment(t *testing.T) {
	s := newClaimStore(t.TempDir(), false)
	writer := types.Volume{Name: "data", Mount: "/data", Mode: types.VolumeModeRW}
	sb := &types.Sandbox{ID: "owner", Volumes: []types.Volume{writer}}
	if err := s.commit(s.set(sb)); err != nil {
		t.Fatal(err)
	}
	restore := failClaimWrites(t, s)
	sb.VolumesWant = []types.Volume{{Name: "other", Mount: "/other"}}
	if err := s.commit(s.set(sb)); err == nil {
		t.Fatal("expected write failure")
	}
	reader := writer
	reader.Mode = types.VolumeModeRO
	moved := writer
	moved.Mount = "/elsewhere"
	for _, tc := range []struct {
		volume   types.Volume
		owner    string
		conflict bool
	}{
		{writer, sb.ID, false}, {writer, "different", true}, {reader, sb.ID, true}, {moved, sb.ID, true},
	} {
		if got := s.volumeConflicts(tc.volume, tc.owner); got != tc.conflict {
			t.Errorf("volume %+v owner %q conflict=%t, want %t", tc.volume, tc.owner, got, tc.conflict)
		}
	}
	s.set(&types.Sandbox{ID: sb.ID, Volumes: []types.Volume{writer}})
	restore()
	if err := s.commit(s.mark()); err != nil {
		t.Fatal(err)
	}
	if s.referencesVolume("other") || s.volumeConflicts(writer, sb.ID) {
		t.Fatal("successful return retained attempted attachment")
	}
}

func TestClaimCaptureAcknowledgesBothReferenceDomains(t *testing.T) {
	for _, domain := range []string{"volumes", "checkpoints"} {
		t.Run(domain, func(t *testing.T) {
			s := newClaimStore(t.TempDir(), false)
			sb := &types.Sandbox{ID: "owner", ReplicaCk: "checkpoint", Volumes: []types.Volume{{Name: "data"}}}
			release := captureClaimWrite(t, s, s.set(sb), sb.ID)
			if domain == "volumes" {
				sb.Volumes = nil
			} else {
				sb.ReplicaCk = ""
			}
			newer := s.set(sb)
			release()
			if !s.pending(newer) || !s.referencesCheckpoint("checkpoint") || !s.hasRecoveryVolumes(sb.ID) {
				t.Fatal("older capture retired newer state or its recovery references")
			}
			if err := s.commit(newer); err != nil {
				t.Fatal(err)
			}
			if got := s.referencesCheckpoint("checkpoint"); got != (domain == "volumes") {
				t.Fatalf("checkpoint reference after newer commit = %t", got)
			}
			if got := s.hasRecoveryVolumes(sb.ID); got != (domain == "checkpoints") {
				t.Fatalf("volume recovery after newer commit = %t", got)
			}
		})
	}
}
