// Promoted templates: goldens published through the store under the tp_
// namespace, so a shared mount or bucket serves them cluster-wide exactly
// like checkpoints. Configured pools keep their node-local goldens — the
// refill hot path never touches the store.
package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// templateRecord is a template's meta.json: the id carries the key hash
// (tp_<hash>), which is all resolution needs — claims re-derive the hash
// from the requested key, never the reverse.
type templateRecord struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

// Promote publishes a claimed sandbox as a template: its state is exported
// into the store under (template, parent net, parent size), and later
// claims for that key clone from it — provision-on-demand, no warm pool
// unless the node config adds one. Re-promoting to the same name replaces
// the template. The caller owns the template's lifecycle (DeleteTemplate).
// On a shared store root every node resolves it; on local disk it stays
// node-bound and the mesh's template gossip routes to it.
func (m *Manager) Promote(ctx context.Context, id, token, template string) (types.PoolKey, error) {
	if !templateNameRe.MatchString(template) {
		return types.PoolKey{}, fmt.Errorf("%w: template %q must match %s", ErrBadKey, template, templateNameRe)
	}
	sb, ok := m.claim(id, token)
	if !ok {
		return types.PoolKey{}, ErrUnknownSandbox
	}
	key := types.PoolKey{Template: template, Net: sb.Key.Net, Size: sb.Key.Size}
	if m.pooledHash(key.Hash()) {
		// A configured pool owns this key — promoting over it would
		// silently change what refills produce.
		return types.PoolKey{}, ErrPooledTemplate
	}
	// See Fork: the transition lock pins the source snapshot, and a started
	// promote must finish even if the caller hangs up.
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	ctx = context.WithoutCancel(ctx)

	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	defer cleanup()
	if err := m.publishTemplate(ctx, snap, key); err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, nil
}

// DeleteTemplate removes a promoted template. Configured pools are refused:
// their goldens are owned by the node config, not an API caller.
func (m *Manager) DeleteTemplate(ctx context.Context, key types.PoolKey) error {
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	id := templateID(key.Hash())
	if _, err := m.tpls.ReadMeta(ctx, id); err != nil {
		return ErrUnknownTemplate
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
// touches the store backend.
func (m *Manager) TemplateHashes() []string {
	m.tplMu.Lock()
	hashes := make([]string, 0, len(m.tplSet))
	for id := range m.tplSet {
		hashes = append(hashes, id[len("tp_"):])
	}
	m.tplMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	return sliceWithout(hashes, func(h string) bool {
		for key := range m.pools {
			if key.Hash() == h {
				return true
			}
		}
		return false
	})
}

// HasGolden reports whether this node can provision the key without a cold
// boot — a configured pool golden or a promoted template in the store.
func (m *Manager) HasGolden(ctx context.Context, key types.PoolKey) bool {
	m.mu.Lock()
	pooled := m.pools[key] != nil && m.pools[key].goldenDir != ""
	m.mu.Unlock()
	if pooled {
		return true
	}
	_, err := m.tpls.ReadMeta(ctx, templateID(key.Hash()))
	return err == nil
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

// resolveGolden resolves a key's clone source: the configured pool's local
// golden (no release), else a promoted template fetched from the store;
// empty dir cold-boots.
func (m *Manager) resolveGolden(ctx context.Context, key types.PoolKey) (string, func(), error) {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return dir, func() {}, nil
	}
	id := templateID(key.Hash())
	if _, err := m.tpls.ReadMeta(ctx, id); err != nil {
		return "", func() {}, nil // no template: cold boot
	}
	return m.tpls.Fetch(ctx, id)
}

// publishTemplate exports snap into the store under the key's template id.
func (m *Manager) publishTemplate(ctx context.Context, snap string, key types.PoolKey) error {
	id := templateID(key.Hash())
	staging, err := m.tpls.Stage(id)
	if err != nil {
		return fmt.Errorf("stage template: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err = m.eng.SnapshotExport(ctx, snap, filepath.Join(staging, store.ExportDir)); err != nil {
		return fmt.Errorf("export template: %w", err)
	}
	meta, err := json.Marshal(templateRecord{ID: id, CreatedAt: time.Now()})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
		return err
	}
	if err := m.tpls.Publish(ctx, staging, id); err != nil {
		return fmt.Errorf("publish template: %w", err)
	}
	m.tplMu.Lock()
	m.tplSet[id] = struct{}{}
	m.tplMu.Unlock()
	return nil
}

// migrateLegacyTemplates moves pre-store promoted templates
// (<goldens>/<hash> dirs not backing a pool) into the template store —
// one-time, at reconcile.
func (m *Manager) migrateLegacyTemplates(ctx context.Context) {
	logger := log.WithFunc("pool.migrateLegacyTemplates")
	entries, err := os.ReadDir(m.goldensDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		hash := e.Name()
		if !e.IsDir() || m.pooledHash(hash) || len(hash) != 32 {
			continue
		}
		id := templateID(hash)
		staging, err := m.tpls.Stage(id)
		if err != nil {
			logger.Errorf(ctx, err, "stage %s", id)
			continue
		}
		legacy := filepath.Join(m.goldensDir(), hash)
		if err := os.Rename(legacy, filepath.Join(staging, store.ExportDir)); err != nil {
			logger.Errorf(ctx, err, "move %s", legacy)
			_ = os.RemoveAll(staging)
			continue
		}
		meta, _ := json.Marshal(templateRecord{ID: id, CreatedAt: time.Now()})
		if err := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); err != nil {
			logger.Errorf(ctx, err, "meta %s", id)
			continue
		}
		if err := m.tpls.Publish(ctx, staging, id); err != nil {
			logger.Errorf(ctx, err, "publish %s", id)
			continue
		}
		m.tplMu.Lock()
		m.tplSet[id] = struct{}{}
		m.tplMu.Unlock()
		logger.Infof(ctx, "migrated legacy template %s", id)
	}
}

func templateID(hash string) string { return "tp_" + hash }

// sliceWithout filters s in place, dropping entries drop reports true for.
func sliceWithout(s []string, drop func(string) bool) []string {
	out := s[:0]
	for _, v := range s {
		if !drop(v) {
			out = append(out, v)
		}
	}
	return out
}
