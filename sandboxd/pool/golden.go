package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (m *Manager) buildGolden(ctx context.Context, p *pool) {
	logger := log.WithFunc("pool.buildGolden")
	hash := p.hash
	name := vmPrefix + "gb-" + hash
	snap := goldenPrefix + hash + "-" + randHex(6)
	final := filepath.Join(m.goldensDir(), hash)
	imageID := m.imageIDs(ctx)[p.key.Template]
	v := m.view.Load()
	m.mu.Lock()
	adopted := m.adoptGolden(p, imageID)
	p.building = !adopted
	m.mu.Unlock()
	if adopted {
		m.refillOnce(ctx)
		return
	}

	err := m.buildGoldenSteps(ctx, v, p, name, snap, final, imageID)
	if rmErr := m.eng.SnapshotRemove(ctx, snap); rmErr != nil && err == nil {
		logger.Warnf(ctx, "drop golden snapshot %s: %v", snap, rmErr)
	}
	m.destroy(ctx, name)

	m.mu.Lock()
	p.building = false
	stale := err == nil && m.goldenStamp(m.view.Load(), p.key, imageID) != m.goldenStamp(v, p.key, imageID)
	switch {
	case stale:
		p.nextBuild = time.Time{}
	case err == nil:
		p.install(final, imageID)
	default:
		p.nextBuild = time.Now().Add(buildRetryDelay)
		// A cold boot hits the same bridge and disk as a refill, so it feeds the park too
		if reason := engine.CapacitySignature(err); reason != "" {
			m.noteCapacityLocked(time.Now(), reason)
		}
	}
	m.mu.Unlock()
	if err != nil {
		logger.Errorf(ctx, err, "build golden for %s", hash)
		return
	}
	if stale {
		logger.Infof(ctx, "golden for %s was built against a superseded config; rebuilding", hash)
		m.kickRefill()
		return
	}
	logger.Infof(ctx, "golden ready for %s (%s)", hash, p.key.Template)
	m.kickRefill()
}

func (m *Manager) buildGoldenSteps(ctx context.Context, v *configView, p *pool, name, snap, final, imageID string) error {
	key := p.key
	rec, err := m.eng.RunCold(ctx, name, key, v.poolStorage[key])
	if err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, name, rec.VsockSocket, coldProbeTimeout)
	if err != nil {
		return err
	}
	if err = m.out.PrepareGuest(ctx, v.out, key, sock); err != nil {
		return err
	}
	if err = m.runWarmup(ctx, v.poolWarmups[key], sock); err != nil {
		return fmt.Errorf("warmup: %w", err)
	}
	if err = m.eng.SettleGuest(ctx, sock); err != nil {
		log.WithFunc("pool.buildGoldenSteps").Warnf(ctx, "capture golden of %s before its guest settled: %v", key.Template, err)
	}
	if err := m.dumpGolden(ctx, name, snap, final, &p.goldenUse); err != nil {
		return err
	}
	stamp := m.goldenStamp(v, key, imageID)
	if err := os.WriteFile(final+goldenStampSuffix, []byte(stamp), 0o644); err != nil { //nolint:gosec // public stamp
		return fmt.Errorf("write golden stamp: %w", err)
	}
	return nil
}

func (m *Manager) runWarmup(ctx context.Context, warmup []string, sock string) error {
	if len(warmup) == 0 {
		return nil
	}
	return m.eng.Warmup(ctx, sock, warmup)
}

// adoptGolden points p at an on-disk golden whose stamp matches what the pool bakes now.
func (m *Manager) adoptGolden(p *pool, imageID string) bool {
	g := filepath.Join(m.goldensDir(), p.hash)
	stamp, err := os.ReadFile(g + goldenStampSuffix) //nolint:gosec // node-local golden path
	want := m.goldenStamp(m.view.Load(), p.key, imageID)
	if err != nil || string(stamp) != want || !dirExists(g) {
		return false
	}
	p.install(g, imageID)
	return true
}

// dropPoolLocked's rename hides the golden from adoptGolden, and the slow unlink runs off the lock.
func (m *Manager) dropPoolLocked(ctx context.Context, p *pool) {
	delete(m.pools, p.key)
	g := filepath.Join(m.goldensDir(), p.hash)
	_ = os.Remove(g + goldenStampSuffix)
	doomed := g + "-" + randHex(4) + ".tmp"
	if os.Rename(g, doomed) != nil {
		return
	}
	go func() {
		if os.RemoveAll(doomed) == nil {
			log.WithFunc("pool.dropPoolLocked").Infof(ctx, "removed the golden of dropped pool %s", p.key.Template)
		}
	}()
}

func (m *Manager) retireUnclonableLocked(p *pool, gen uint64, now time.Time) bool {
	if p.cloneFails < refillFailStreak || p.goldenGen != gen || p.goldenDir == "" || now.Sub(p.goldenRetiredAt) < buildRetryDelay {
		return false
	}
	p.goldenDir, p.imageID = "", ""
	p.goldenGen++
	p.nextBuild, p.goldenRetiredAt = time.Time{}, now
	p.refillFails, p.nextRefill, p.cloneFails = 0, time.Time{}, 0
	_ = os.Remove(filepath.Join(m.goldensDir(), p.hash) + goldenStampSuffix)
	return true
}

func (m *Manager) rebuildRetiredGolden(ctx context.Context, hash string) {
	log.WithFunc("pool.rebuildRetiredGolden").Warnf(ctx, "golden of %s failed %d clones running; rebuilding it", hash, refillFailStreak)
	m.kickRefill()
}

func (m *Manager) goldenStamp(v *configView, key types.PoolKey, imageID string) string {
	var fp string
	if v.out.Intercepts(key) {
		fp = m.out.CAFingerprint()
	}
	fields := []string{fp, string(m.out.Lane(key)), imageID}
	// Only a sized pool stamps its disk, in bytes so "40G" and "40GiB" match and unsized goldens stay adoptable
	if storage := v.poolStorage[key]; storage != "" {
		n, _ := config.StorageBytes(storage)
		fields = append(fields, "storage="+strconv.FormatInt(n, 10))
	}
	if v.noBalloon {
		fields = append(fields, "no_balloon")
	}
	return strings.Join(append(fields, v.poolWarmups[key]...), "\x00")
}

func (m *Manager) checkImagesOnce(ctx context.Context) {
	m.mu.Lock()
	empty := len(m.pools) == 0
	m.mu.Unlock()
	if empty {
		return
	}
	ids, err := m.eng.ImageIDs(ctx)
	if err != nil {
		log.WithFunc("pool.checkImagesOnce").Warnf(ctx, "list images: %v", err)
		return
	}
	changed := false
	m.mu.Lock()
	for _, p := range m.pools {
		if p.goldenDir == "" || p.rebuild || ids[p.key.Template] == p.imageID {
			continue
		}
		p.rebuild, changed = true, true
	}
	m.mu.Unlock()
	if changed {
		m.kickRefill()
	}
}

func (m *Manager) imageIDs(ctx context.Context) map[string]string {
	ids, err := m.eng.ImageIDs(ctx)
	if err != nil {
		log.WithFunc("pool.imageIDs").Warnf(ctx, "list images: %v", err)
	}
	return ids
}

func (m *Manager) dumpGolden(ctx context.Context, name, snap, final string, use *sync.RWMutex) error {
	select {
	case m.goldenDumps <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.goldenDumps }()
	if err := m.eng.SnapshotSave(ctx, name, snap); err != nil {
		return err
	}
	return m.exportGolden(ctx, snap, final, use)
}

func (m *Manager) exportGolden(ctx context.Context, snap, final string, use *sync.RWMutex) error {
	staging, err := os.MkdirTemp(filepath.Dir(final), filepath.Base(final)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("stage golden: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	tmp := filepath.Join(staging, "export") // cocoon wants the target absent
	if err := m.eng.SnapshotExport(ctx, snap, tmp); err != nil {
		return err
	}
	use.Lock()
	defer use.Unlock()
	if err := os.Rename(final, filepath.Join(staging, "old")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear golden dir: %w", err)
	}
	return os.Rename(tmp, final)
}
