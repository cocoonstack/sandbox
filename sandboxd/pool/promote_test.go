package pool

import (
	"bytes"
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

	gotKey, gotDigest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:x", "")
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
	if gotDigest == "" {
		t.Fatal("Promote returned an empty content digest")
	}
	golden, _, _, release, err := m.tpls.Fetch(t.Context(), store.TemplateID(key.Hash()))
	if err != nil {
		t.Fatalf("template export missing: %v", err)
	}
	release()

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
	if child.TemplateDigest != gotDigest {
		t.Errorf("claim template digest %q, want promoted content digest %q", child.TemplateDigest, gotDigest)
	}
	meta, err := m.tpls.ReadMeta(t.Context(), store.TemplateID(key.Hash()))
	if err != nil {
		t.Fatalf("read template metadata: %v", err)
	}
	if bytes.Contains(meta, []byte(`"content_digest"`)) {
		t.Errorf("template metadata still contains the digest: %s", meta)
	}
}

func TestRepromoteContentDigestTracksExportBytes(t *testing.T) {
	eng := newFakeEngine()
	eng.exportContent = []byte("generation one")
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)

	_, firstDigest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:digest", "")
	if err != nil {
		t.Fatalf("first Promote: %v", err)
	}
	_, secondDigest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:digest", "")
	if err != nil {
		t.Fatalf("same-byte Promote: %v", err)
	}
	if secondDigest != firstDigest {
		t.Errorf("same export bytes changed digest: %q then %q", firstDigest, secondDigest)
	}

	eng.exportContent = []byte("generation two")
	thirdKey, thirdDigest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:digest", "")
	if err != nil {
		t.Fatalf("changed-byte Promote: %v", err)
	}
	if thirdDigest == firstDigest {
		t.Errorf("changed export bytes kept digest %q", thirdDigest)
	}
	child, err := claimAny(t.Context(), m, thirdKey, 0)
	if err != nil {
		t.Fatalf("claim latest generation: %v", err)
	}
	if child.TemplateDigest != thirdDigest {
		t.Errorf("claim digest %q, want latest %q", child.TemplateDigest, thirdDigest)
	}
}

func TestPromoteHibernatedUsesWakeImage(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	if err := m.Hibernate(t.Context(), parent.ID, Cred{Token: parent.Token}); err != nil {
		t.Fatalf("Hibernate: %v", err)
	}

	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:hib", ""); err != nil {
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

	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "_bad", ""); !errors.Is(err, ErrBadKey) {
		t.Errorf("bad name: %v, want ErrBadKey", err)
	}
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: "wrong"}, "tpl:x", ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("bad token: %v, want ErrUnknownSandbox", err)
	}
	// Same template/net/size as the configured pool: the golden path would
	// collide with the pool's own.
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, testKey.Template, ""); !errors.Is(err, ErrPooledTemplate) {
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
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:del", ""); err != nil {
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
	if m.HasGolden(t.Context(), key, "") {
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

func TestResolveGoldenSkipsPromotedEgressTemplate(t *testing.T) {
	// A template promoted before the egress guard existed must never be resumed:
	// resolveGolden returns "" for the egress lane so the claim cold-boots.
	m := newTestManager(t, newFakeEngine())
	id := store.TemplateID(egKey.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err = os.MkdirAll(filepath.Join(staging, store.ExportDir), 0o750); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if _, err = m.commitTemplate(t.Context(), staging, id, ""); err != nil {
		t.Fatalf("seed template: %v", err)
	}
	golden, err := m.resolveGolden(t.Context(), egKey, "")
	if err != nil {
		t.Fatalf("resolveGolden: %v", err)
	}
	golden.release()
	if golden.dir != "" {
		t.Errorf("resolveGolden resumed a promoted egress template %q; want cold-boot", golden.dir)
	}
}

func TestTemplateTenantScopedDelete(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:tenant", "acme")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	if err := m.DeleteTemplate(t.Context(), key, "beta"); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("cross-tenant delete: %v, want ErrUnknownTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "acme"); err != nil {
		t.Errorf("own delete: %v", err)
	}
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:tenant", "acme"); err != nil {
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
	if _, err := m.commitTemplate(t.Context(), stage(), id, "acme"); err != nil {
		t.Fatalf("acme publish: %v", err)
	}
	if _, err := m.commitTemplate(t.Context(), stage(), id, "beta"); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta publish over acme: %v, want ErrTemplateOwned", err)
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
	a, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "shared:v1", "acme"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	key := types.PoolKey{Template: "shared:v1", Net: testKey.Net, Size: testKey.Size}
	meta := filepath.Join(m.dataDir, "checkpoints", store.TemplateID(key.Hash()), store.MetaFile)
	if err := os.Chmod(meta, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(meta, 0o600) })
	if _, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "shared:v1", "beta"); err == nil {
		t.Fatal("promote succeeded despite an unreadable owner record")
	}
}

func TestPromoteRefusesCrossTenantOverwrite(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	claim := func(tenant string) *types.Sandbox {
		t.Helper()
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, tenant, "", nil)
		if err != nil {
			t.Fatalf("claim %q: %v", tenant, err)
		}
		return sb
	}
	a := claim("acme")
	if _, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "shared:v1", "acme"); err != nil {
		t.Fatalf("acme promote: %v", err)
	}
	b := claim("beta")
	if _, _, err := m.Promote(t.Context(), b.ID, Cred{Token: b.Token}, "shared:v1", "beta"); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta overwrite: %v, want ErrTemplateOwned", err)
	}
	r := claim("") // root may replace anything
	if _, _, err := m.Promote(t.Context(), r.ID, Cred{Token: r.Token}, "shared:v1", ""); err != nil {
		t.Errorf("root replace: %v, want ok", err)
	}
}

func TestTemplateClaimIsTenantScoped(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	claim := func(tenant string) *types.Sandbox {
		t.Helper()
		sb, err := m.ClaimProvision(t.Context(), testKey, time.Hour, tenant, "", nil)
		if err != nil {
			t.Fatalf("claim %q: %v", tenant, err)
		}
		return sb
	}

	a := claim("acme")
	private, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("acme promote: %v", err)
	}
	r := claim("")
	shared, _, err := m.Promote(t.Context(), r.ID, Cred{Token: r.Token}, "ops-shared", "")
	if err != nil {
		t.Fatalf("root promote: %v", err)
	}

	if _, err := m.ClaimProvisionPromoted(t.Context(), private, time.Hour, "beta", "", nil); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("beta claiming acme's template: %v, want ErrUnknownTemplate", err)
	}
	for _, tc := range []struct {
		name   string
		key    types.PoolKey
		tenant string
	}{
		{"owner claims its own", private, "acme"},
		{"root claims a tenant's", private, ""},
		{"tenant claims a root template", shared, "beta"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sb, err := m.ClaimProvisionPromoted(t.Context(), tc.key, time.Hour, tc.tenant, "", nil)
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if sb.TemplateDigest == "" {
				t.Error("claim did not resolve from the promoted template")
			}
		})
	}
}

func TestHasPromotedTemplateIsTenantScoped(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	a, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	// Routing must answer what a claim would: promising beta a golden here
	// makes redirectClaim skip the peer hop and cold-boot the name as an image.
	if m.HasPromotedTemplate(t.Context(), key, "beta") {
		t.Error("beta sees acme's template as a local golden")
	}
	for _, tenant := range []string{"acme", ""} {
		if !m.HasPromotedTemplate(t.Context(), key, tenant) {
			t.Errorf("tenant %q lost its own template", tenant)
		}
	}
}

func TestTemplateHashesAreTenantScoped(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	a, err := m.ClaimProvision(t.Context(), testKey, time.Hour, "acme", "", nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	key, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	hashes := m.TemplateHashes()
	if want := types.TemplateGossipHash(key.Hash(), "acme"); !slices.Contains(hashes, want) {
		t.Errorf("gossip %v lacks the owner-scoped hash %s", hashes, want)
	}
	for name, bad := range map[string]string{
		"raw":     key.Hash(),
		"foreign": types.TemplateGossipHash(key.Hash(), "beta"),
	} {
		if slices.Contains(hashes, bad) {
			t.Errorf("gossip %v carries the %s hash — a foreign tenant could match it", hashes, name)
		}
	}
}

func TestTemplateHashesSortedForMeshCompare(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	for _, name := range []string{"tpl:a", "tpl:b", "tpl:c", "tpl:d"} {
		if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, name, ""); err != nil {
			t.Fatalf("Promote %s: %v", name, err)
		}
	}
	hashes := m.TemplateHashes()
	if len(hashes) != 4 {
		t.Fatalf("got %d hashes, want 4: %v", len(hashes), hashes)
	}
	if !slices.IsSorted(hashes) {
		t.Errorf("TemplateHashes not sorted: %v", hashes)
	}
}
