package pool

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
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
	key := tplKey("tpl:x")
	if gotKey != key {
		t.Errorf("returned key %+v, want %+v (the parent's axes)", gotKey, key)
	}
	if gotDigest == "" {
		t.Fatal("Promote returned an empty content digest")
	}
	golden, _, _, err := m.tpls.Fetch(t.Context(), store.TemplateID(key.Hash()))
	if err != nil {
		t.Fatalf("template export missing: %v", err)
	}

	child, err := claimAny(t.Context(), m, key, ClaimOptions{})
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
	child, err := claimAny(t.Context(), m, thirdKey, ClaimOptions{})
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
	hibernateClaim(t, m, parent)

	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:hib", ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("snapSaves %v, want none — the hibernate image is the source", eng.snapSaves)
	}
	if slices.Contains(eng.snapRemoves, eng.hibernates[0]) {
		t.Error("hibernate snapshot dropped by promote — the parent could never wake")
	}
	if _, _, _, err := m.WakeAgentSocket(t.Context(), parent.ID, parent.Token); err != nil {
		t.Fatalf("wake after promote: %v", err)
	}
}

func TestPromoteRejectsABadNameTokenOrPooledKeyBeforeASnapshot(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 0})
	parent := mustClaim(t, m, testKey)

	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "_bad", ""); !errors.Is(err, ErrBadKey) {
		t.Errorf("bad name: %v, want ErrBadKey", err)
	}
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: "wrong"}, "tpl:x", ""); !errors.Is(err, ErrUnknownSandbox) {
		t.Errorf("bad token: %v, want ErrUnknownSandbox", err)
	}

	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, testKey.Template, ""); !errors.Is(err, ErrPooledTemplate) {
		t.Errorf("pooled key: %v, want ErrPooledTemplate", err)
	}
	if len(eng.snapSaves) != 0 {
		t.Errorf("rejected promotes still snapshotted: %v", eng.snapSaves)
	}
}

func TestTemplateDigestGuardReadsTheSharedRecord(t *testing.T) {
	eng := newFakeEngine()
	eng.exportContent = []byte("generation one")
	m := newTestManager(t, eng)
	parent := mustClaim(t, m, testKey)
	key, stale, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:shared", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	peer := newTestManager(t, newFakeEngine())
	peer.tpls = m.tpls
	id := store.TemplateID(key.Hash())
	peer.tplSet[id] = TemplateInfo{Key: key, ContentDigest: stale}

	eng.exportContent = []byte("generation two")
	_, latest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:shared", "")
	if err != nil {
		t.Fatalf("re-Promote: %v", err)
	}
	if err := peer.SetTemplateLabels(t.Context(), key, types.Metadata{"a": "b"}, "", stale); !errors.Is(err, ErrTemplateReplaced) {
		t.Errorf("labels with the replaced digest: %v, want ErrTemplateReplaced", err)
	}
	if err := peer.DeleteTemplate(t.Context(), key, "", stale); !errors.Is(err, ErrTemplateReplaced) {
		t.Errorf("delete with the replaced digest: %v, want ErrTemplateReplaced", err)
	}
	if _, err := m.tpls.ReadMeta(t.Context(), id); err != nil {
		t.Fatalf("the newer build is gone: %v", err)
	}
	delete(peer.tplSet, id)
	if err := peer.SetTemplateLabels(t.Context(), key, types.Metadata{"a": "b"}, "", latest); err != nil {
		t.Errorf("labels with the current digest on a node that has not cached it: %v", err)
	}
	if err := peer.DeleteTemplate(t.Context(), key, "", latest); err != nil {
		t.Errorf("delete with the current digest on a node that has not cached it: %v", err)
	}
}

func TestDeleteTemplateRefusesPooledUnknownAndReplacedTemplates(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 0})
	parent := mustClaim(t, m, testKey)
	_, digest, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:del", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	key := tplKey("tpl:del")

	if err := m.DeleteTemplate(t.Context(), testKey, "", ""); !errors.Is(err, ErrPooledTemplate) {
		t.Errorf("pooled delete: %v, want ErrPooledTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), tplKey("nope"), "", ""); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("unknown delete: %v, want ErrUnknownTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "", "sha256:observed-elsewhere"); !errors.Is(err, ErrTemplateReplaced) || !m.HasGolden(t.Context(), key, "") {
		t.Errorf("delete with another digest: %v, want ErrTemplateReplaced and the template kept", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "", digest); err != nil {
		t.Fatalf("DeleteTemplate with the promote's digest: %v", err)
	}
	if m.HasGolden(t.Context(), key, "") {
		t.Error("template still resolvable after delete")
	}

	before := len(eng.colds)
	if _, err := claimAny(t.Context(), m, key, ClaimOptions{}); err != nil {
		t.Fatalf("Claim after delete: %v", err)
	}
	if len(eng.colds) != before+1 {
		t.Errorf("colds %v, want a cold boot after the golden vanished", eng.colds)
	}
}

func TestTemplateRecordLockEvictsWithTheRecord(t *testing.T) {
	m := newTestManager(t, newFakeEngine(), config.PoolSpec{PoolKey: testKey, Warm: 0})
	parent := mustClaim(t, m, testKey)
	key := tplKey("tpl:evict")
	id := store.TemplateID(key.Hash())
	base := lockCount(m)

	if _, err := claimAny(t.Context(), m, key, ClaimOptions{}); err != nil {
		t.Fatalf("Claim of an unpromoted key: %v", err)
	}
	if hasRecLock(m, id) {
		t.Error("recLocks kept an entry for a template that was never promoted")
	}
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, key.Template, ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "", ""); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	if hasRecLock(m, id) {
		t.Error("recLocks retained a lock for the deleted template")
	}
	if got := lockCount(m); got != base {
		t.Errorf("recLocks grew %d->%d over claim, promote and delete, want no net growth", base, got)
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
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale export staging survived reconcile: %v", err)
	}
}

func TestResolveGoldenNeverServesAPooledKeyFromATemplate(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	parent := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:pooled", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	m.mu.Lock()
	m.pools[key] = newPool(config.PoolSpec{PoolKey: key})
	m.mu.Unlock()
	golden, err := m.resolveGolden(t.Context(), key, "")
	if err != nil {
		t.Fatalf("resolveGolden: %v", err)
	}
	golden.release()
	if golden.dir != "" || golden.promoted {
		t.Errorf("resolveGolden served pooled key from template %q; want cold-boot until the pool golden lands", golden.dir)
	}
}

func TestResolveGoldenSkipsPromotedEgressTemplate(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	staging := stageTemplate(t, m, store.TemplateID(egKey.Hash()))
	if _, err := m.commitTemplate(t.Context(), staging, templateRecord{Key: egKey}); err != nil {
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
	parent := claimTenant(t, m, "acme")
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:tenant", "acme")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	if err := m.DeleteTemplate(t.Context(), key, "beta", ""); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("cross-tenant delete: %v, want ErrUnknownTemplate", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "acme", ""); err != nil {
		t.Errorf("own delete: %v", err)
	}
	if _, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:tenant", "acme"); err != nil {
		t.Fatalf("re-promote: %v", err)
	}
	if err := m.DeleteTemplate(t.Context(), key, "", ""); err != nil {
		t.Errorf("root delete: %v", err)
	}
}

func TestCommitTemplateRechecksOwnerUnderLock(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	id := store.TemplateID(testKey.Hash())
	if _, err := m.commitTemplate(t.Context(), stageTemplate(t, m, id), templateRecord{Key: testKey, Tenant: "acme"}); err != nil {
		t.Fatalf("acme publish: %v", err)
	}
	if _, err := m.commitTemplate(t.Context(), stageTemplate(t, m, id), templateRecord{Key: testKey, Tenant: "beta"}); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta publish over acme: %v, want ErrTemplateOwned", err)
	}
}

func TestPromoteFailsClosedOnMetaError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	a := claimTenant(t, m, "acme")
	if _, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "shared:v1", "acme"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	key := tplKey("shared:v1")
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
	a := claimTenant(t, m, "acme")
	if _, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "shared:v1", "acme"); err != nil {
		t.Fatalf("acme promote: %v", err)
	}
	b := claimTenant(t, m, "beta")
	if _, _, err := m.Promote(t.Context(), b.ID, Cred{Token: b.Token}, "shared:v1", "beta"); !errors.Is(err, ErrTemplateOwned) {
		t.Errorf("beta overwrite: %v, want ErrTemplateOwned", err)
	}
	r := claimTenant(t, m, "")
	if _, _, err := m.Promote(t.Context(), r.ID, Cred{Token: r.Token}, "shared:v1", ""); err != nil {
		t.Errorf("root replace: %v, want ok", err)
	}
}

func TestTemplateClaimIsTenantScoped(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)

	a := claimTenant(t, m, "acme")
	private, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("acme promote: %v", err)
	}
	r := claimTenant(t, m, "")
	shared, _, err := m.Promote(t.Context(), r.ID, Cred{Token: r.Token}, "ops-shared", "")
	if err != nil {
		t.Fatalf("root promote: %v", err)
	}

	if _, err := m.ClaimProvision(t.Context(), private, ClaimOptions{RequirePromoted: true, TTL: time.Hour, Tenant: "beta"}); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("beta claiming acme's template: %v, want ErrUnknownTemplate", err)
	}
	for _, tt := range []struct {
		name   string
		key    types.PoolKey
		tenant string
	}{
		{"owner claims its own", private, "acme"},
		{"root claims a tenant's", private, ""},
		{"tenant claims a root template", shared, "beta"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sb, err := m.ClaimProvision(t.Context(), tt.key, ClaimOptions{RequirePromoted: true, TTL: time.Hour, Tenant: tt.tenant})
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
	a := claimTenant(t, m, "acme")
	key, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

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
	m := tenantManager(t, t.TempDir(), tenants.TenantSpec{Name: "acme", Token: "acme-tok"})
	a := claimTenant(t, m, "acme")
	key, _, err := m.Promote(t.Context(), a.ID, Cred{Token: a.Token}, "acme-private", "acme")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if err := m.DeleteTenant(t.Context(), "acme"); err != nil {
		t.Fatalf("remove tenant: %v", err)
	}

	hashes := m.TemplateHashes()
	if want := types.TemplateGossipHash(key.Hash(), "acme"); !slices.Contains(hashes, want) {
		t.Errorf("gossip %v lacks the owner-scoped hash %s", hashes, want)
	}
	if want := types.TemplateGossipHash(key.Hash(), types.TemplateRootScope); !slices.Contains(hashes, want) {
		t.Errorf("gossip %v lacks the root-scoped hash %s", hashes, want)
	}
	for name, bad := range map[string]string{
		"raw":     key.Hash(),
		"foreign": types.TemplateGossipHash(key.Hash(), "beta"),
		"public":  types.TemplateGossipHash(key.Hash(), ""),
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
	if len(hashes) != 8 {
		t.Fatalf("got %d hashes, want 8: %v", len(hashes), hashes)
	}
	if !slices.IsSorted(hashes) {
		t.Errorf("TemplateHashes not sorted: %v", hashes)
	}
}

func TestTemplatesListWhatTheNodeHoldsAcrossARestart(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	parent := mustClaim(t, m, testKey)
	keyA, digestA, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:a", "")
	if err != nil {
		t.Fatalf("Promote tpl:a: %v", err)
	}
	acme := claimTenant(t, m, "acme")
	keyB, digestB, err := m.Promote(t.Context(), acme.ID, Cred{Token: acme.Token}, "tpl:b", "acme")
	if err != nil {
		t.Fatalf("Promote tpl:b: %v", err)
	}
	oldID := store.TemplateID(tplKey("tpl:old").Hash())
	staging := stageTemplate(t, m, oldID)
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), []byte(`{"id":"`+oldID+`","created_at":"2026-01-01T00:00:00Z"}`), 0o600); err != nil {
		t.Fatalf("write keyless meta: %v", err)
	}
	if _, err = storetest.PublishDigested(t.Context(), m.tpls, staging, oldID); err != nil {
		t.Fatalf("publish keyless record: %v", err)
	}

	before := m.Templates()
	spec, _ := testKey.Size.Spec()
	restarted := newTestManagerAt(t, eng, dir)
	got := restarted.Templates()
	if len(got) != 2 || len(before) != 2 {
		t.Fatalf("Templates before %+v, after restart %+v, want tpl:a and tpl:b", before, got)
	}
	for _, want := range []TemplateInfo{{Key: keyA, ContentDigest: digestA}, {Key: keyB, ContentDigest: digestB, Tenant: "acme"}} {
		i := slices.IndexFunc(got, func(ti TemplateInfo) bool { return ti.Key == want.Key })
		j := slices.IndexFunc(before, func(ti TemplateInfo) bool { return ti.Key == want.Key })
		if i < 0 || j < 0 || got[i].ContentDigest != want.ContentDigest || got[i].Tenant != want.Tenant ||
			got[i].CreatedAt.IsZero() || !got[i].CreatedAt.Equal(before[j].CreatedAt) ||
			got[i].CPUCount != spec.CPU || got[i].MemTotalBytes != spec.MemoryBytes {
			t.Errorf("after restart %+v, want %+v created %v on the %+v tier", got, want, before, spec)
		}
	}
	if hashes := restarted.TemplateHashes(); len(hashes) != 6 {
		t.Errorf("gossip %v, want the keyless record still routed", hashes)
	}
	if err = restarted.DeleteTemplate(t.Context(), keyA, "", ""); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	if got = restarted.Templates(); len(got) != 1 || got[0].Key != keyB {
		t.Errorf("Templates after delete %+v, want tpl:b alone", got)
	}
	pooled := newTestManagerAt(t, eng, dir, config.PoolSpec{PoolKey: keyB})
	if got = pooled.Templates(); len(got) != 0 {
		t.Errorf("Templates %+v, want none once a pool owns tpl:b's key", got)
	}
}

func TestTemplateLabelsPersistUntilTheTemplateIsRepromotedOrDeleted(t *testing.T) {
	eng := newFakeEngine()
	dir := t.TempDir()
	m := newTestManagerAt(t, eng, dir)
	parent := mustClaim(t, m, testKey)
	key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:l", "")
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	labelsOf := func(m *Manager) types.Metadata {
		t.Helper()
		i := slices.IndexFunc(m.Templates(), func(ti TemplateInfo) bool { return ti.Key == key })
		if i < 0 {
			t.Fatalf("tpl:l not listed")
		}
		return m.Templates()[i].Labels
	}
	want := types.Metadata{"v1": "sha256:aa"}
	if err = m.SetTemplateLabels(t.Context(), key, want, "", "sha256:observed-elsewhere"); !errors.Is(err, ErrTemplateReplaced) || labelsOf(m) != nil {
		t.Errorf("labels with another digest: %v, want ErrTemplateReplaced and no labels written", err)
	}
	if err = m.SetTemplateLabels(t.Context(), key, want, "", ""); err != nil {
		t.Fatalf("SetTemplateLabels: %v", err)
	}
	if got := labelsOf(m); !maps.Equal(got, want) {
		t.Errorf("labels %v, want %v", got, want)
	}
	restarted := newTestManagerAt(t, eng, dir)
	if got := labelsOf(restarted); !maps.Equal(got, want) {
		t.Errorf("labels after restart %v, want %v", got, want)
	}
	if err = m.SetTemplateLabels(t.Context(), key, types.Metadata{"x": "y"}, "acme", ""); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("a tenant labeling the operator's template: %v, want ErrUnknownTemplate", err)
	}
	if err = m.SetTemplateLabels(t.Context(), tplKey("tpl:none"), want, "", ""); !errors.Is(err, ErrUnknownTemplate) {
		t.Errorf("an unknown template: %v, want ErrUnknownTemplate", err)
	}
	if _, _, err = m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:l", ""); err != nil {
		t.Fatalf("re-promote: %v", err)
	}
	if got := labelsOf(newTestManagerAt(t, eng, dir)); got != nil {
		t.Errorf("labels %v survived a re-promote, want none", got)
	}
	if err = m.SetTemplateLabels(t.Context(), key, want, "", ""); err != nil {
		t.Fatalf("SetTemplateLabels again: %v", err)
	}
	if err = m.SetTemplateLabels(t.Context(), key, types.Metadata{}, "", ""); err != nil || labelsOf(m) != nil {
		t.Errorf("empty labels: %v, labels %v; want them cleared", err, labelsOf(m))
	}
	if err = m.SetTemplateLabels(t.Context(), key, want, "", ""); err != nil {
		t.Fatalf("SetTemplateLabels before delete: %v", err)
	}
	if err = m.DeleteTemplate(t.Context(), key, "", ""); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	if _, _, err = m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:l", ""); err != nil {
		t.Fatalf("promote after delete: %v", err)
	}
	if got := labelsOf(newTestManagerAt(t, eng, dir)); got != nil {
		t.Errorf("labels %v outlived the deleted template", got)
	}
	pooled := newTestManagerAt(t, eng, t.TempDir(), config.PoolSpec{PoolKey: key})
	if err = pooled.SetTemplateLabels(t.Context(), key, want, "", ""); !errors.Is(err, ErrPooledTemplate) {
		t.Errorf("a pooled key: %v, want ErrPooledTemplate", err)
	}
}

func TestRejectedLabelWritesLeakNoRecLock(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	base := lockCount(m)
	for _, name := range []string{"tpl:absent", "tpl:gone", "tpl:never"} {
		key := tplKey(name)
		if err := m.SetTemplateLabels(t.Context(), key, types.Metadata{"a": "1"}, "", ""); !errors.Is(err, ErrUnknownTemplate) {
			t.Errorf("labels on %q: %v, want ErrUnknownTemplate", name, err)
		}
	}
	if got := lockCount(m); got != base {
		t.Errorf("recLocks grew %d->%d over rejected label writes, want no growth", base, got)
	}
}

func TestClaimFromATemplateRunsBesideItsRepromote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newTestManager(t, newFakeEngine())
		parent := mustClaim(t, m, testKey)
		key, _, err := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:x", "")
		if err != nil {
			t.Fatalf("Promote: %v", err)
		}
		slow := &slowPrepareStore{Store: m.tpls, entered: make(chan struct{}), release: make(chan struct{})}
		m.tpls = slow
		repromoted := make(chan error, 1)
		go func() {
			_, _, promoteErr := m.Promote(t.Context(), parent.ID, Cred{Token: parent.Token}, "tpl:x", "")
			repromoted <- promoteErr
		}()
		<-slow.entered

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		claimed := make(chan error, 1)
		go func() {
			_, claimErr := claimAny(ctx, m, key, ClaimOptions{})
			claimed <- claimErr
		}()
		synctest.Wait()
		select {
		case err = <-claimed:
			if err != nil {
				t.Errorf("claim during a re-promote: %v, want a clone of the committed template", err)
			}
		default:
			t.Error("a claim with a 100 ms context was still blocked during a re-promote")
		}
		close(slow.release)
		if err = <-repromoted; err != nil {
			t.Fatalf("re-promote: %v", err)
		}
	})
}

type slowPrepareStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}
}

func (s *slowPrepareStore) PrepareDigested(ctx context.Context, staging, id, lineage string) (string, store.CommitFunc, error) {
	close(s.entered)
	<-s.release
	return s.Store.PrepareDigested(ctx, staging, id, lineage)
}

func stageTemplate(t *testing.T, m *Manager, id string) string {
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
