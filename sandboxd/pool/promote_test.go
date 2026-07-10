package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
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

// TestCommitTemplateRechecksOwnerUnderLock drives the publish-side check
// directly: two racers that both passed Promote's early check must still
// serialize on ownership at publish.
func TestCommitTemplateRechecksOwnerUnderLock(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	id := store.TemplateID(testKey.Hash())
	stage := func() string {
		t.Helper()
		staging, err := m.tpls.Stage(id)
		if err != nil {
			t.Fatalf("stage: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
			t.Fatalf("mkdir export: %v", err)
		}
		return staging
	}
	if err := m.commitTemplate(t.Context(), stage(), id, "acme"); err != nil {
		t.Fatalf("acme publish: %v", err)
	}
	if err := m.commitTemplate(t.Context(), stage(), id, "beta"); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta publish over acme: %v, want ErrTemplateOwned", err)
	}
}

type failPublishStore struct{ store.Store }

func (failPublishStore) Publish(context.Context, string, string) error {
	return errors.New("publish boom")
}

// TestMigrateRollsBackOnPublishFailure: the staged legacy export is the only
// copy; a failed publish must move it back or the next staging sweep deletes
// the template outright.
func TestMigrateRollsBackOnPublishFailure(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	legacy := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := os.MkdirAll(legacy, 0o750); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "disk.img"), []byte("golden"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	m.tpls = failPublishStore{Store: m.tpls}

	m.migrateLegacyTemplates(t.Context())
	got, err := os.ReadFile(filepath.Join(legacy, "disk.img"))
	if err != nil || string(got) != "golden" {
		t.Fatalf("legacy template lost after failed publish: %q, %v", got, err)
	}
}

// TestPromoteFailsClosedOnMetaError: an unreadable template meta must refuse
// the promote, never skip the ownership check.
func TestPromoteFailsClosedOnMetaError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	a, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := m.Promote(t.Context(), a.ID, a.Token, "shared:v1", "acme"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	key := types.PoolKey{Template: "shared:v1", Net: testKey.Net, Size: testKey.Size}
	meta := filepath.Join(m.dataDir, "checkpoints", store.TemplateID(key.Hash()), store.MetaFile)
	if err := os.Chmod(meta, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(meta, 0o600) })
	if _, err := m.Promote(t.Context(), a.ID, a.Token, "shared:v1", "beta"); err == nil {
		t.Fatal("promote succeeded despite an unreadable owner record")
	}
}

func TestPromoteRefusesCrossTenantOverwrite(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	claim := func(tenant string) *types.Sandbox {
		t.Helper()
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, tenant)
		if err != nil {
			t.Fatalf("claim %q: %v", tenant, err)
		}
		return sb
	}
	a := claim("acme")
	if _, err := m.Promote(t.Context(), a.ID, a.Token, "shared:v1", "acme"); err != nil {
		t.Fatalf("acme promote: %v", err)
	}
	b := claim("beta")
	if _, err := m.Promote(t.Context(), b.ID, b.Token, "shared:v1", "beta"); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta overwrite: %v, want ErrTemplateOwned", err)
	}
	r := claim("") // root may replace anything
	if _, err := m.Promote(t.Context(), r.ID, r.Token, "shared:v1", ""); err != nil {
		t.Errorf("root replace: %v, want ok", err)
	}
}
