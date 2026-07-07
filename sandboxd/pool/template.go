// Promoted templates and their on-disk goldens.
package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Promote publishes a claimed sandbox as a template: its state is exported
// as a golden under (template, parent net, parent size), and later claims
// for that key clone from it — provision-on-demand, no warm pool unless the
// node config adds one. Re-promoting to the same name replaces the golden.
// The caller owns the template's lifecycle (DeleteTemplate); it lives only
// on this node, so the returned key is what a cluster client needs to reach
// it again.
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
		// The same goldens/<hash> path backs a configured pool's golden —
		// promoting over it would silently change what refills produce.
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
	if err := m.exportGolden(ctx, snap, filepath.Join(m.goldensDir(), key.Hash())); err != nil {
		return types.PoolKey{}, fmt.Errorf("promote %s: %w", id, err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	m.counters.promotes.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "promote", ID: sb.ID, VMName: sb.VMName, Reference: key.Template})
	return key, nil
}

// DeleteTemplate removes a promoted template's golden. Configured pools are
// refused: their goldens are owned by the node config, not an API caller.
func (m *Manager) DeleteTemplate(key types.PoolKey) error {
	if m.pooledHash(key.Hash()) {
		return ErrPooledTemplate
	}
	dir, ok := m.goldenOnDisk(key.Hash())
	if !ok {
		return ErrUnknownTemplate
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if m.notifyTemplates != nil {
		m.notifyTemplates()
	}
	return nil
}

// TemplateHashes lists the promoted-template key hashes on disk — goldens
// not backing a configured pool — for the mesh's template gossip.
func (m *Manager) TemplateHashes() []string {
	entries, err := os.ReadDir(m.goldensDir())
	if err != nil {
		return nil
	}
	m.mu.Lock()
	pooled := make(map[string]struct{}, len(m.pools))
	for key := range m.pools {
		pooled[key.Hash()] = struct{}{}
	}
	m.mu.Unlock()
	var hashes []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if _, ok := pooled[e.Name()]; !ok {
			hashes = append(hashes, e.Name())
		}
	}
	return hashes
}

// HasGolden reports whether this node can provision the key without a cold
// boot — a configured pool golden or a promoted template on disk.
func (m *Manager) HasGolden(key types.PoolKey) bool {
	return m.goldenDirFor(key) != ""
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

// goldenDirFor resolves a key's golden: the pool's when configured, else an
// on-disk golden a Promote published for an unpooled key; "" cold-boots.
func (m *Manager) goldenDirFor(key types.PoolKey) string {
	m.mu.Lock()
	var dir string
	if p := m.pools[key]; p != nil {
		dir = p.goldenDir
	}
	m.mu.Unlock()
	if dir != "" {
		return dir
	}
	if onDisk, ok := m.goldenOnDisk(key.Hash()); ok {
		return onDisk
	}
	return ""
}

func (m *Manager) goldenOnDisk(hash string) (string, bool) {
	dir := filepath.Join(m.goldensDir(), hash)
	fi, err := os.Stat(dir)
	return dir, err == nil && fi.IsDir()
}
