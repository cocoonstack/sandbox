package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestPromoteThenClaimClonesFromTemplate(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)

	gotKey, err := m.Promote(t.Context(), parent.ID, parent.Token, "tpl:x", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if len(eng.snapSaves) != 1 || !slices.Contains(eng.snapRemoves, eng.snapSaves[0]) {
		t.Errorf("snapSaves=%v snapRemoves=%v, want one transient snapshot dropped", eng.snapSaves, eng.snapRemoves)
	}
	key := types.PoolKey{Template: "tpl:x", Net: parent.Key.Net, Size: parent.Key.Size}
	if gotKey != key {
		t.Errorf("returned key %+v, want %+v (the parent's axes)", gotKey, key)
	}
	golden := filepath.Join(m.dataDir, "checkpoints", "tp_"+key.Hash(), "export")
	if fi, statErr := os.Stat(golden); statErr != nil || !fi.IsDir() {
		t.Fatalf("template export %s missing: %v", golden, statErr)
	}

	child, err := claimAny(t.Context(), m, key, 0)
	if err != nil {
		t.Fatalf("Claim promoted template: %v", err)
	}
	if len(eng.cloneFroms) == 0 || eng.cloneFroms[len(eng.cloneFroms)-1] != golden {
		t.Errorf("cloneFroms %v, want a clone from %s (not a cold boot)", eng.cloneFroms, golden)
	}
	if child.Key != key {
		t.Errorf("child key %+v, want %+v", child.Key, key)
	}
}

func TestPromoteHibernatedUsesWakeImage(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	if _, err := m.Promote(t.Context(), parent.ID, parent.Token, "tpl:hib", ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("snapSaves %v, want none — the hibernate image is the source", eng.snapSaves)
	}
	if slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Error("hibernate snapshot dropped by promote — the parent could never wake")
	}
	if _, err := m.WakeAgentSocket(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("wake after promote: %v", err)
	}
}

func TestPromoteValidations(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 0})
	parent := mustClaim(t, m, testKey)

	if _, err := m.Promote(t.Context(), parent.ID, parent.Token, "_bad", ""); !errors.Is(err, ErrBadKey) {
		t.Errorf("bad name: %v, want ErrBadKey", err)
	}
	if _, err := m.Promote(t.Context(), parent.ID, "wrong", "tpl:x", ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("bad token: %v, want ErrUnknownSandbox", err)
	}
	// Same template/net/size as the configured pool: the golden path would
	// collide with the pool's own.
	if _, err := m.Promote(t.Context(), parent.ID, parent.Token, testKey.Template, ""); !errors.Is(err, ErrPooledTemplate) {
		t.Errorf("pooled key: %v, want ErrPooledTemplate", err)
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("rejected promotes still snapshotted: %v", eng.snapSaves)
	}
}

func TestDeleteTemplate(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 0})
	parent := mustClaim(t, m, testKey)
	if _, err := m.Promote(t.Context(), parent.ID, parent.Token, "tpl:del", ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	key := types.PoolKey{Template: "tpl:del", Net: testKey.Net, Size: testKey.Size}

	if err := m.DeleteTemplate(t.Context(), testKey, ""); !errors.Is(err, ErrPooledTemplate) {
		t.Errorf("pooled delete: %v, want ErrPooledTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), types.PoolKey{Template: "nope", Net: testKey.Net, Size: testKey.Size}, ""); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("unknown delete: %v, want ErrUnknownTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, ""); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	if m.HasGolden(t.Context(), key) {
		t.Error("template still resolvable after delete")
	}
	// The next claim for the deleted template cold-boots instead of cloning.
	before := len(eng.colds)
	if _, err := claimAny(t.Context(), m, key, 0); err != nil {
		t.Fatalf("Claim after delete: %v", err)
	}
	if len(eng.colds) != before+1 {
		t.Errorf("colds %v, want a cold boot after the golden vanished", eng.colds)
	}
}

func TestReconcileSweepsGoldenTmpDirs(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	stale := filepath.Join(m.goldensDir(), "deadbeef.tmp")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale export staging survived reconcile: %v", err)
	}
}

func TestReconcileMigratesLegacyTemplates(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	key := types.PoolKey{Template: "tpl:legacy", Net: testKey.Net, Size: testKey.Size}
	legacy := filepath.Join(m.goldensDir(), key.Hash())
	if err := os.MkdirAll(legacy, 0o750); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "disk.img"), []byte("golden-bytes"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := m.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Errorf("legacy golden dir survived migration: %v", err)
	}
	if !m.HasGolden(t.Context(), key) {
		t.Fatal("migrated template not resolvable")
	}
	dir, release, err := m.resolveGolden(t.Context(), key)
	if err != nil || dir == "" {
		t.Fatalf("resolveGolden: %q, %v", dir, err)
	}
	defer release()
	if got, err := os.ReadFile(filepath.Join(dir, "disk.img")); err != nil || string(got) != "golden-bytes" {
		t.Errorf("migrated export: %q, %v", got, err)
	}
	if hashes := m.TemplateHashes(); !slices.Contains(hashes, key.Hash()) {
		t.Errorf("TemplateHashes %v missing migrated %s", hashes, key.Hash())
	}
}

func TestTemplateTenantScopedDelete(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, err := m.Promote(t.Context(), parent.ID, parent.Token, "tpl:tenant", "acme")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	if err := m.DeleteTemplate(t.Context(), key, "beta"); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("cross-tenant delete: %v, want ErrUnknownTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "acme"); err != nil {
		t.Errorf("own delete: %v", err)
	}
	if _, err := m.Promote(t.Context(), parent.ID, parent.Token, "tpl:tenant", "acme"); err != nil {
		t.Fatalf("re-promote: %v", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, ""); err != nil {
		t.Errorf("root delete: %v", err)
	}
}
