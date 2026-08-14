package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// templateRecord is a template's meta.json: the id carries the key hash
// (tp_<hash>), which is all resolution needs — claims re-derive the hash
// from the requested key, never the reverse. Tenant attributes the promote
// and scopes deletion; empty means the operator (root).
type templateRecord struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Promote publishes a claimed sandbox as a template under (template, parent
// net, parent size); later claims for that key clone from it. Re-promoting the
// same name replaces it, and the caller owns its lifecycle (DeleteTemplate).
func (m *Manager) Promote(ctx context.Context, id string, cred Cred, template, tenant string) (types.PoolKey, string, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return types.PoolKey{}, "", ErrUnknownSandbox
	}
	if !types.NameRe.MatchString(template) {
		return types.PoolKey{}, "", fmt.Errorf("%w: template %q must match %s", ErrBadKey, template, types.NameRe)
	}
	if hasAppliedVolumes(sb) {
		return types.PoolKey{}, "", ErrVolumeCapture
	}
	if !sb.Key.Capturable() {
		return types.PoolKey{}, "", ErrNoEgressFork
	}
	key := types.PoolKey{Template: template, Net: sb.Key.Net, Size: sb.Key.Size, Engine: sb.Key.Engine}
	if m.pooledHash(key.Hash()) {
		// A configured pool owns this key — promoting over it would
		// silently change what refills produce.
		return types.PoolKey{}, "", ErrPooledTemplate
	}
	// Fast-fail before the export; commitTemplate re-checks under the
	// template lock, closing the check-then-publish race.
	if err := m.checkTemplateOwner(ctx, store.TemplateID(key.Hash()), tenant); err != nil {
		return types.PoolKey{}, "", err
	}
	// See Fork: the transition lock pins the source snapshot, and a started
	// promote must finish even if the caller hangs up.
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	ctx = context.WithoutCancel(ctx)

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, "", fmt.Errorf("promote %s: %w", sb.ID, err)
	}
	defer cleanup()
	digest, err := m.publishTemplate(ctx, snap, key, tenant)
	if err != nil {
		return types.PoolKey{}, "", fmt.Errorf("promote %s: %w", sb.ID, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, digest, nil
}

// DeleteTemplate removes a promoted template. Configured pools are refused:
// their goldens are owned by the node config, not an API caller. A tenant
// may delete only templates it promoted — anything else answers
// ErrUnknownTemplate; root (empty tenant) deletes anything.
func (m *Manager) DeleteTemplate(ctx context.Context, key types.PoolKey, tenant string) error {
	if err := m.validate(key); err != nil {
		return err
	}
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
	raw, err := m.tpls.ReadMeta(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrUnknownTemplate
		}
		return fmt.Errorf("read template: %w", err)
	}
	if tenant != "" {
		var rec templateRecord
		if json.Unmarshal(raw, &rec) != nil || !tenantOwns(tenant, rec.Tenant) {
			return ErrUnknownTemplate
		}
	}
	if err := m.tpls.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	m.tplMu.Lock()
	delete(m.tplSet, id)
	m.tplMu.Unlock()
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	return nil
}

// TemplateHashes lists the promoted-template key hashes for the mesh's
// template gossip, from the in-memory set — the 1s gossip tick never
// touches the store backend. Sorted: the mesh's unchanged-guard compares
// order-sensitively, and map iteration order would defeat it every tick.
func (m *Manager) TemplateHashes() []string {
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for key := range m.pools {
		pooled[key.Hash()] = struct{}{}
	}
	m.mu.Unlock()
	m.tplMu.Lock()
	hashes := make([]string, 0, len(m.tplSet))
	for id := range m.tplSet {
		hash := store.TemplateHash(id)
		if _, ok := pooled[hash]; !ok {
			hashes = append(hashes, hash)
		}
	}
	m.tplMu.Unlock()
	slices.Sort(hashes)
	return hashes
}

// HasGolden reports whether this node can provision the key without a cold
// boot — a configured pool golden or a promoted template in the store. The
// tplSet answers without a store read; only a shared-store template promoted
// elsewhere after startup falls through to the backend.
func (m *Manager) HasGolden(ctx context.Context, key types.PoolKey, tenant string) bool {
	return m.HasPoolGolden(key) || m.HasPromotedTemplate(ctx, key, tenant)
}

// HasPoolGolden reports whether a configured pool can serve key from its own
// golden. That golden is what every claim on this node resolves to.
func (m *Manager) HasPoolGolden(key types.PoolKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pools[key]
	return p != nil && p.goldenDir != ""
}

// HasPromotedTemplate reports whether key resolves to a promoted template this
// tenant may claim — resolveGolden's test exactly, so routing never promises
// a golden the claim would then refuse.
func (m *Manager) HasPromotedTemplate(ctx context.Context, key types.PoolKey, tenant string) bool {
	if m.pooledHash(key.Hash()) {
		return false
	}
	id := store.TemplateID(key.Hash())
	m.tplMu.Lock()
	owner, cached := m.tplSet[id]
	m.tplMu.Unlock()
	if !cached {
		// Only a shared-store template promoted elsewhere after startup.
		raw, err := m.tpls.ReadMeta(ctx, id)
		if err != nil {
			return false
		}
		var rec templateRecord
		if json.Unmarshal(raw, &rec) != nil {
			return false
		}
		owner = rec.Tenant
	}
	return owner == "" || tenantOwns(tenant, owner)
}

// pooledHash reports whether a configured pool occupies this hash — the
// guard is on the HASH, not the key: goldens are stored by hash, so a
// colliding key would reach a pool's golden dir even though the keys differ.
func (m *Manager) pooledHash(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.pools {
		if key.Hash() == hash {
			return true
		}
	}
	return false
}

// recLock is the per-record mutation lock: template publish/delete serialize
// on it directly; checkpoints take it only for delete/fetch (their ids are
// fresh and unguessable pre-publish). A clone or wake holds it shared, so a
// delete or re-publish swap never runs under an in-flight read. Every call
// counts as a live reference — pair it with recDone (or recDoneEvict) once
// the returned lock is unlocked, so a concurrent evict can never hand a
// waiter a different mutex for the same id.
func (m *Manager) recLock(id string) *sync.RWMutex {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]++
	l, _ := m.recLocks.LoadOrStore(id, &sync.RWMutex{})
	return l.(*sync.RWMutex)
}

// recDone releases a reference taken by recLock; call after Unlock/RUnlock,
// never before — releasing early is what lets a concurrent recLock observe
// a fresh mutex while this call's lock is still logically held.
func (m *Manager) recDone(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		m.evictIfPending(id)
	}
}

// recDoneEvict is recDone for a caller that just deleted id's record: once
// the reference count drops to zero — no one else holds or awaits this id's
// lock — its recLocks slot is removed so the map can't grow per checkpoint.
// Template ids (tp_, reused on re-promote) never call this; they stay
// cached forever, bounded by the promoted set.
func (m *Manager) recDoneEvict(id string) {
	m.recLocksMu.Lock()
	defer m.recLocksMu.Unlock()
	m.recRefs[id]--
	if m.recRefs[id] <= 0 {
		delete(m.recRefs, id)
		delete(m.recEvict, id)
		m.recLocks.Delete(id)
		return
	}
	// Another holder or waiter is still on this lock; defer the eviction to
	// whichever call drops the last reference, so it is not lost here.
	m.recEvict[id] = struct{}{}
}

// evictIfPending drops id's lock entry if a delete asked for eviction while a
// reference still held it; callers hold recLocksMu with the count at zero.
func (m *Manager) evictIfPending(id string) {
	if _, ok := m.recEvict[id]; ok {
		delete(m.recEvict, id)
		m.recLocks.Delete(id)
	}
}

// checkTemplateOwner rejects publishing or deleting over another tenant's
// record; a meta failure other than absence refuses rather than fail open.
func (m *Manager) checkTemplateOwner(ctx context.Context, id, tenant string) error {
	if tenant == "" {
		return nil
	}
	raw, err := m.tpls.ReadMeta(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("read template: %w", err)
	}
	var prev templateRecord
	if json.Unmarshal(raw, &prev) != nil || !tenantOwns(tenant, prev.Tenant) {
		return ErrTemplateOwned
	}
	return nil
}

type goldenResolution struct {
	dir            string
	templateDigest string
	promoted       bool
	release        func()
}

// resolveGolden resolves a key's clone source: the configured pool's local
// golden (no release), else a promoted template fetched from the store;
// empty dir cold-boots. A cross-tenant or absent record cold-boots; a backend
// failure propagates rather than booting a template name as an image ref.
func (m *Manager) resolveGolden(ctx context.Context, key types.PoolKey, tenant string) (goldenResolution, error) {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return goldenResolution{dir: dir, release: func() {}}, nil
	}
	if key.Net == types.NetEgress {
		return goldenResolution{release: func() {}}, nil // never resume a live-captured template on the egress lane; cold-boot instead
	}
	id := store.TemplateID(key.Hash())
	l := m.recLock(id)
	l.RLock()
	dir, meta, digest, release, err := m.tpls.Fetch(ctx, id)
	if err != nil {
		l.RUnlock()
		m.recDone(id)
		if errors.Is(err, store.ErrNotFound) {
			return goldenResolution{release: func() {}}, nil
		}
		return goldenResolution{release: func() {}}, err
	}
	cleanup := func() { release(); l.RUnlock(); m.recDone(id) }
	var rec templateRecord
	if err := json.Unmarshal(meta, &rec); err != nil {
		cleanup()
		return goldenResolution{release: func() {}}, fmt.Errorf("decode template metadata: %w", err)
	}
	if rec.Tenant != "" && !tenantOwns(tenant, rec.Tenant) {
		cleanup()
		return goldenResolution{release: func() {}}, nil
	}
	return goldenResolution{
		dir:            dir,
		templateDigest: digest,
		promoted:       true,
		release:        cleanup,
	}, nil
}

// publishTemplate exports snap into the store under the key's template id.
func (m *Manager) publishTemplate(ctx context.Context, snap string, key types.PoolKey, tenant string) (string, error) {
	id := store.TemplateID(key.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		return "", fmt.Errorf("stage template: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.eng.SnapshotExport(ctx, snap, filepath.Join(staging, store.ExportDir)); err != nil {
		return "", fmt.Errorf("export template: %w", err)
	}
	return m.commitTemplate(ctx, staging, id, tenant)
}

func (m *Manager) commitTemplate(ctx context.Context, staging, id, tenant string) (string, error) {
	l := m.recLock(id)
	l.Lock()
	defer func() { l.Unlock(); m.recDone(id) }()
	if ownerErr := m.checkTemplateOwner(ctx, id, tenant); ownerErr != nil {
		return "", ownerErr
	}
	meta, err := json.Marshal(templateRecord{ID: id, Tenant: tenant, CreatedAt: time.Now()})
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return "", err
	}
	digest, err := m.tpls.PublishDigested(ctx, staging, id)
	if err != nil {
		return "", fmt.Errorf("publish template: %w", err)
	}
	m.tplMu.Lock()
	m.tplSet[id] = tenant
	m.tplMu.Unlock()
	return digest, nil
}
