package pool

import (
	"context"
	"errors"
	"fmt"
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

type vmProvisioner func(name string) (types.VMRecord, error)

// kickRefill nudges Run past its refill ticker without blocking the claim path.
func (m *Manager) kickRefill() {
	select {
	case m.refillKick <- struct{}{}:
	default:
	}
}

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.draining {
		return
	}
	now := time.Now()
	// spawning VMs into a full node still takes the global rtnl lock even when doomed to fail
	parked := now.Before(m.atCapacityUntil)
	inFlight := 0
	for key, p := range m.pools {
		inFlight += p.refilling
		// SetPools leaves a removed pool in place while a build or refill is in flight
		if p.removed {
			if !p.building && p.refilling == 0 {
				delete(m.pools, key)
			}
			continue
		}
		if !parked && p.goldenDir == "" && !p.building && now.After(p.nextBuild) {
			p.building = true
			go m.buildGolden(ctx, p)
		}
	}
	if parked {
		return
	}
	limit := cap(m.refillSem) + cap(m.probeSem)
	for spawned := true; spawned && inFlight < limit; {
		spawned = false
		for _, p := range m.pools {
			if inFlight >= limit {
				return
			}
			if p.removed || p.goldenDir == "" || p.refillGated(now) ||
				len(p.warm)+p.refilling >= p.effectiveTarget(now) {
				continue
			}
			select {
			case m.refillSem <- struct{}{}:
				p.refilling++
				inFlight++
				go m.refillOne(ctx, p, p.goldenDir, p.goldenGen)
				spawned = true
			default:
				return
			}
		}
	}
}

// shrinkOnce destroys the warm VMs a decayed target no longer wants; refill only ever fills.
func (m *Manager) shrinkOnce(ctx context.Context) {
	var trim []string
	m.mu.Lock()
	now := time.Now()
	for _, p := range m.pools {
		trim = append(trim, p.shrink(now)...)
	}
	m.mu.Unlock()
	m.destroyAll(ctx, trim)
}

func (m *Manager) refillOne(ctx context.Context, p *pool, golden string, gen uint64) {
	start := time.Now()
	sb, err := m.startVM(ctx, p.key, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, p.key)
	})
	<-m.refillSem
	if err == nil {
		if ctx.Err() == nil {
			m.refillOnce(ctx)
		}
		sb, err = m.readyBounded(ctx, sb, time.Now().Add(warmProbeTimeout))
	}
	if err == nil {
		err = m.warmClone(ctx, sb)
	}
	if err == nil {
		m.prebindEgress(ctx, sb)
	}
	keep := false
	var fails int
	var wait time.Duration
	capReason := engine.CapacitySignature(err)
	m.mu.Lock()
	now := time.Now()
	p.refilling--
	if err == nil && !m.draining && p.goldenDir != "" && p.goldenGen == gen && len(p.warm) < p.effectiveTarget(now) {
		p.warm = append(p.warm, sb)
		p.noteLead(time.Since(start))
		keep = true
	}
	// a canceled context or a capacity failure says nothing about this pool's health
	backedOff := false
	if ctx.Err() == nil && capReason == "" {
		backedOff = p.noteRefillResult(now, err != nil)
	}
	if backedOff {
		fails, wait = p.refillFails, time.Until(p.nextRefill)
	}
	entered := capReason != "" && m.noteCapacityLocked(now, capReason)
	parked := now.Before(m.atCapacityUntil)
	m.mu.Unlock()
	if err != nil {
		logger := log.WithFunc("pool.refillOne")
		switch {
		case ctx.Err() != nil:
		case entered:
			logger.Errorf(ctx, err, "node is at capacity; parking refill for %s", capacityBackoff)
		case parked:
		default:
			hash := p.hash
			logger.Errorf(ctx, err, "refill %s", hash)
			if backedOff {
				logger.Warnf(ctx,
					"refill %s failed %d times running; pausing refill for %s",
					hash, fails, wait.Round(time.Millisecond))
			}
		}
		return
	}
	if ctx.Err() == nil {
		m.refillOnce(ctx)
	}
	if !keep {
		m.destroy(ctx, sb.VMName)
	}
}

// noteCapacityLocked parks refill node-wide, reporting whether this call parked it.
func (m *Manager) noteCapacityLocked(now time.Time, reason string) bool {
	first := !now.Before(m.atCapacityUntil)
	m.atCapacityUntil = now.Add(capacityBackoff)
	m.atCapacityReason = reason
	return first
}

func (m *Manager) buildGolden(ctx context.Context, p *pool) {
	logger := log.WithFunc("pool.buildGolden")
	hash := p.hash
	name := vmPrefix + "gb-" + hash
	snap := goldenPrefix + hash
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

	err := m.buildGoldenSteps(ctx, v, p.key, name, snap, final, imageID)
	if rmErr := m.eng.SnapshotRemove(ctx, snap); rmErr != nil && err == nil {
		logger.Debugf(ctx, "drop golden snapshot %s: %v", snap, rmErr)
	}
	m.destroy(ctx, name)

	m.mu.Lock()
	p.building = false
	stale := err == nil && m.goldenStamp(m.view.Load(), p.key, imageID) != m.goldenStamp(v, p.key, imageID)
	switch {
	case stale:
		p.nextBuild = time.Time{}
	case err == nil:
		p.goldenDir, p.imageID = final, imageID
		p.goldenGen++
	default:
		p.nextBuild = time.Now().Add(buildRetryDelay)
		// a cold boot hits the same bridge and disk as a refill, so it feeds the park too
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
}

func (m *Manager) buildGoldenSteps(ctx context.Context, v *configView, key types.PoolKey, name, snap, final, imageID string) error {
	rec, err := m.eng.RunCold(ctx, name, key, v.poolStorage[key])
	if err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, name, rec.VsockSocket, coldProbeTimeout)
	if err != nil {
		return err
	}
	if err = m.markLane(ctx, key, sock); err != nil {
		return err
	}
	if v.poolEgress[key].Intercepts() {
		if err = m.eng.InstallCACert(ctx, sock, m.egressCA.CertPEM()); err != nil {
			return fmt.Errorf("install egress ca: %w", err)
		}
	}
	if err = m.runWarmup(ctx, v.poolWarmups[key], sock); err != nil {
		return fmt.Errorf("warmup: %w", err)
	}
	if err := m.eng.SnapshotSave(ctx, name, snap); err != nil {
		return err
	}
	if err := m.exportGolden(ctx, snap, final); err != nil {
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

// a restore maps the golden memory lazily, so the warmup's pages fault in here instead of under the first claim
func (m *Manager) warmClone(ctx context.Context, sb *types.Sandbox) error {
	if err := m.runWarmup(ctx, m.view.Load().poolWarmups[sb.Key], sb.VsockSocket); err != nil {
		m.destroy(ctx, sb.VMName)
		return fmt.Errorf("clone warmup: %w", err)
	}
	return nil
}

// adoptGolden points p at an on-disk golden whose stamp matches what the pool bakes now.
func (m *Manager) adoptGolden(p *pool, imageID string) bool {
	g := filepath.Join(m.goldensDir(), p.hash)
	stamp, err := os.ReadFile(g + goldenStampSuffix) //nolint:gosec // node-local golden path
	want := m.goldenStamp(m.view.Load(), p.key, imageID)
	if err != nil || string(stamp) != want || !dirExists(g) {
		return false
	}
	p.goldenDir, p.imageID = g, imageID
	p.goldenGen++
	return true
}

func (m *Manager) goldenStamp(v *configView, key types.PoolKey, imageID string) string {
	var fp string
	if v.poolEgress[key].Intercepts() {
		fp = m.EgressCAFingerprint()
	}
	fields := []string{fp, string(m.laneOf(key)), imageID}
	// only a sized pool stamps its disk, in bytes so "40G" and "40GiB" match and unsized goldens stay adoptable
	if storage := v.poolStorage[key]; storage != "" {
		n, _ := config.StorageBytes(storage)
		fields = append(fields, "storage="+strconv.FormatInt(n, 10))
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
	var trim []string
	m.mu.Lock()
	for _, p := range m.pools {
		if p.goldenDir == "" || ids[p.key.Template] == p.imageID {
			continue
		}
		p.goldenDir, p.imageID = "", ""
		trim = append(trim, p.trimWarm(0)...)
	}
	m.mu.Unlock()
	m.destroyAll(ctx, trim)
}

func (m *Manager) imageIDs(ctx context.Context) map[string]string {
	ids, err := m.eng.ImageIDs(ctx)
	if err != nil {
		log.WithFunc("pool.imageIDs").Warnf(ctx, "list images: %v", err)
	}
	return ids
}

// exportGolden exports snap into final through a unique sibling *.tmp dir.
func (m *Manager) exportGolden(ctx context.Context, snap, final string) error {
	staging, err := os.MkdirTemp(filepath.Dir(final), filepath.Base(final)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("stage golden: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	tmp := filepath.Join(staging, "export") // cocoon wants the target absent
	if err := m.eng.SnapshotExport(ctx, snap, tmp); err != nil {
		return err
	}
	if err := os.RemoveAll(final); err != nil {
		return fmt.Errorf("clear golden dir: %w", err)
	}
	return os.Rename(tmp, final)
}

// sourceSnap picks the snapshot to export a claimed sandbox from, with its cleanup; the caller holds sb.Transition.
func (m *Manager) sourceSnap(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	// archive() clears VMName under the same lock, so this is the check that cannot race it
	if sb.ArchiveCk != "" {
		return "", nil, ErrArchived
	}
	if sb.Failed != "" {
		return "", nil, ErrFailed
	}
	if sb.HibernateSnap != "" {
		return sb.HibernateSnap, func() {}, nil
	}
	snap := forkPrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		return "", nil, err
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}

// exportSource captures a claimed sandbox into exportDir and returns its snapshot, VM name and guest-env presence.
func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir string) (snap, vmName string, guestEnv bool, err error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.trimForCapture(ctx, sb)
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return "", "", false, err
	}
	defer cleanup()
	return snap, sb.VMName, m.heldGuestEnv(sb), m.eng.SnapshotExport(ctx, snap, exportDir)
}

// trimForCapture trims a live sandbox's copy-on-write disk when its pool asks for it; a failed trim only leaves the capture larger.
func (m *Manager) trimForCapture(ctx context.Context, sb *types.Sandbox) {
	if !m.view.Load().poolTrims[sb.PolicyKey()] || sb.HibernateSnap != "" || sb.ArchiveCk != "" {
		return
	}
	if err := m.eng.TrimCow(ctx, sb.VsockSocket); err != nil {
		log.WithFunc("pool.trimForCapture").Warnf(ctx, "trim %s before its capture: %v", sb.ID, err)
	}
}

// provision creates one claim-ready VM, cloning from a golden when available.
func (m *Manager) provision(ctx context.Context, key types.PoolKey, golden string) (*types.Sandbox, error) {
	if golden == "" {
		return m.provisionCold(ctx, key)
	}
	return m.provisionVM(ctx, key, claimProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, key)
	})
}

func (m *Manager) provisionCold(ctx context.Context, key types.PoolKey) (*types.Sandbox, error) {
	sb, err := m.provisionVM(ctx, key, coldProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.RunCold(ctx, name, key, m.view.Load().poolStorage[key])
	})
	if err != nil {
		return nil, err
	}
	if err := m.markLane(ctx, key, sb.VsockSocket); err != nil {
		m.destroy(ctx, sb.VMName)
		return nil, err
	}
	// a pre-golden cold claim must trust the root, or intercepted hosts fail TLS for its life
	if m.poolIntercepts(key) {
		if err := m.eng.InstallCACert(ctx, sb.VsockSocket, m.egressCA.CertPEM()); err != nil {
			m.destroy(ctx, sb.VMName)
			return nil, fmt.Errorf("install egress ca: %w", err)
		}
	}
	return sb, nil
}

func (m *Manager) provisionVM(ctx context.Context, key types.PoolKey, probeTimeout time.Duration, create vmProvisioner) (*types.Sandbox, error) {
	sb, err := m.startVM(ctx, key, create)
	if err != nil {
		return nil, err
	}
	return m.readyVM(ctx, sb, time.Now().Add(probeTimeout))
}

func (m *Manager) startVM(ctx context.Context, key types.PoolKey, create vmProvisioner) (*types.Sandbox, error) {
	name := vmName(key)
	rec, err := create(name)
	if err != nil {
		m.destroy(ctx, name)
		return nil, err
	}
	return &types.Sandbox{VMName: name, Key: key, VsockSocket: rec.VsockSocket, TAP: rec.TapDevice()}, nil
}

func (m *Manager) readyVM(ctx context.Context, sb *types.Sandbox, deadline time.Time) (*types.Sandbox, error) {
	sock, err := m.probeReady(ctx, sb.VMName, sb.VsockSocket, time.Until(deadline))
	if err != nil {
		m.destroy(ctx, sb.VMName)
		return nil, err
	}
	sb.VsockSocket = sock
	return sb, nil
}

func (m *Manager) readyBounded(ctx context.Context, sb *types.Sandbox, deadline time.Time) (*types.Sandbox, error) {
	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	select {
	case m.probeSem <- struct{}{}:
		defer func() { <-m.probeSem }()
	case <-probeCtx.Done():
		m.destroy(ctx, sb.VMName)
		return nil, probeCtx.Err()
	}
	return m.readyVM(probeCtx, sb, deadline)
}

func (m *Manager) cloneBatch(ctx context.Context, count int, key types.PoolKey, create vmProvisioner) ([]*types.Sandbox, error) {
	children := make([]*types.Sandbox, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		m.refillSem <- struct{}{}
		wg.Go(func() {
			child, err := m.startVM(ctx, key, create)
			<-m.refillSem
			if err == nil {
				child, err = m.readyBounded(ctx, child, time.Now().Add(claimProbeTimeout))
			}
			children[i], errs[i] = child, err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		for _, child := range children {
			if child != nil {
				m.destroy(ctx, child.VMName)
			}
		}
		return nil, err
	}
	return children, nil
}

// probeReady waits until a VM's silkd answers, polling `vm list` when sock is empty.
func (m *Manager) probeReady(ctx context.Context, name, sock string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	if sock == "" {
		var err error
		sock, err = m.vsockOf(ctx, name)
		for err != nil && time.Now().Before(deadline) && ctx.Err() == nil {
			time.Sleep(vsockPollInterval)
			sock, err = m.vsockOf(ctx, name)
		}
		if err != nil {
			return "", err
		}
	}
	if err := m.eng.Probe(ctx, sock, time.Until(deadline)); err != nil {
		return "", err
	}
	return sock, nil
}

func (m *Manager) vsockOf(ctx context.Context, name string) (string, error) {
	vm, ok, err := m.eng.Inspect(ctx, name)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("vm %s not found after create", name)
	}
	if vm.VsockSocket == "" {
		return "", fmt.Errorf("vm %s has no vsock socket", name)
	}
	return vm.VsockSocket, nil
}

func (m *Manager) destroyAll(ctx context.Context, names []string) *sync.WaitGroup {
	return m.runBounded(ctx, len(names), func(ctx context.Context, i int) { m.destroy(ctx, names[i]) })
}

// runBounded fans f over n items on the refill semaphore, sharing the node-wide budget.
func (m *Manager) runBounded(ctx context.Context, n int, f func(context.Context, int)) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := range n {
		// acquire inside the goroutine so a batch larger than the budget cannot block the caller
		wg.Go(func() {
			select {
			case m.refillSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-m.refillSem }()
			f(ctx, i)
		})
	}
	return &wg
}

func (m *Manager) dropSnap(ctx context.Context, snap string) {
	if snap == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.SnapshotRemove(ctx, snap); err != nil {
		log.WithFunc("pool.dropSnap").Warnf(ctx, "drop snapshot %s: %v", snap, err)
	}
}
