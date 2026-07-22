package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// kickRefill nudges Run past its refill ticker after a warm claim; the
// 1-buffered channel coalesces bursts and never blocks the claim path.
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
	for key, p := range m.pools {
		// SetPools leaves a removed pool in place while a build/refill is in
		// flight; sweep it once quiescent.
		if p.removed {
			if !p.building && p.refilling == 0 {
				delete(m.pools, key)
			}
			continue
		}
		if p.goldenDir == "" && !p.building && now.After(p.nextBuild) {
			p.building = true
			go m.buildGolden(ctx, p)
		}
	}
	inFlight := 0
	for _, p := range m.pools {
		inFlight += p.refilling
	}
	limit := 2 * cap(m.refillSem)
	for spawned := true; spawned && inFlight < limit; {
		spawned = false
		for _, p := range m.pools {
			if inFlight >= limit {
				return
			}
			if p.removed || p.goldenDir == "" || len(p.warm)+p.refilling >= p.effectiveTarget(now) {
				continue
			}
			select {
			case m.refillSem <- struct{}{}:
				p.refilling++
				inFlight++
				go m.refillOne(ctx, p, p.goldenDir)
				spawned = true
			default:
				return
			}
		}
	}
}

func (m *Manager) refillOne(ctx context.Context, p *pool, golden string) {
	start := time.Now()
	sb, err := m.startVM(ctx, p.key, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, p.key)
	})
	<-m.refillSem
	if err == nil {
		if ctx.Err() == nil {
			m.refillOnce(ctx)
		}
		sb, err = m.readyBounded(ctx, sb, time.Now().Add(claimProbeTimeout))
	}
	keep := false
	m.mu.Lock()
	p.refilling--
	if err == nil && !m.draining && len(p.warm) < p.effectiveTarget(time.Now()) {
		p.warm = append(p.warm, sb)
		p.noteLead(time.Since(start))
		keep = true
	}
	m.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			log.WithFunc("pool.refillOne").Errorf(ctx, err, "refill %s", p.key.Hash())
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

func (m *Manager) buildGolden(ctx context.Context, p *pool) {
	logger := log.WithFunc("pool.buildGolden")
	hash := p.key.Hash()
	name := vmPrefix + "gb-" + hash
	snap := goldenPrefix + hash
	final := filepath.Join(m.goldensDir(), hash)

	err := m.buildGoldenSteps(ctx, p.key, name, snap, final)
	if rmErr := m.eng.SnapshotRemove(ctx, snap); rmErr != nil && err == nil {
		logger.Debugf(ctx, "drop golden snapshot %s: %v", snap, rmErr)
	}
	m.destroy(ctx, name)

	m.mu.Lock()
	p.building = false
	if err == nil {
		p.goldenDir = final
	} else {
		p.nextBuild = time.Now().Add(buildRetryDelay)
	}
	m.mu.Unlock()
	if err != nil {
		logger.Errorf(ctx, err, "build golden for %s", hash)
		return
	}
	logger.Infof(ctx, "golden ready for %s (%s)", hash, p.key.Template)
}

func (m *Manager) buildGoldenSteps(ctx context.Context, key types.PoolKey, name, snap, final string) error {
	rec, err := m.eng.RunCold(ctx, name, key)
	if err != nil {
		return err
	}
	sock, err := m.probeReady(ctx, name, rec.VsockSocket, coldProbeTimeout)
	if err != nil {
		return err
	}
	caBaked := m.poolIntercepts(key)
	if caBaked {
		if err := m.eng.InstallCACert(ctx, sock, m.egressCA.CertPEM()); err != nil {
			return fmt.Errorf("install egress ca: %w", err)
		}
	}
	if err := m.eng.SnapshotSave(ctx, name, snap); err != nil {
		return err
	}
	if err := m.exportGolden(ctx, snap, final); err != nil {
		return err
	}
	if err := m.writeGoldenCASidecar(final, caBaked); err != nil {
		return err
	}
	return os.WriteFile(final+runtimeSidecarSuffix, []byte(runtimeMarker), 0o644) //nolint:gosec // public runtime marker
}

// writeGoldenCASidecar records (or clears) the baked-CA fingerprint, so a
// rotated CA or flipped intercept flag forces a rebuild on restart.
func (m *Manager) writeGoldenCASidecar(final string, caBaked bool) error {
	path := final + caSidecarSuffix
	if !caBaked {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("clear ca sidecar: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, []byte(m.egressCA.Fingerprint()), 0o644); err != nil { //nolint:gosec // public fingerprint
		return fmt.Errorf("write ca sidecar: %w", err)
	}
	return nil
}

// adoptGolden points p at a golden already on disk from the pool's earlier
// life, when its baked-CA state still fits; buildGolden covers the rest.
func (m *Manager) adoptGolden(p *pool) {
	if g := filepath.Join(m.goldensDir(), p.key.Hash()); dirExists(g) && m.goldenRuntimeMatches(g) && m.goldenCAMatches(g, m.poolEgress[p.key].Intercepts()) {
		p.goldenDir = g
	}
}

func (m *Manager) goldenRuntimeMatches(final string) bool {
	runtime, err := os.ReadFile(final + runtimeSidecarSuffix) //nolint:gosec // node-local golden marker
	return err == nil && string(runtime) == runtimeMarker
}

// goldenCAMatches reports whether a golden's baked-CA state fits the pool: a
// CA-baked one must carry the current fingerprint, a plain one none. The plain
// case only stats (no read) — it runs under m.mu on the live SetPools path.
func (m *Manager) goldenCAMatches(final string, caNeeded bool) bool {
	if !caNeeded {
		_, err := os.Stat(final + caSidecarSuffix)
		return errors.Is(err, os.ErrNotExist)
	}
	fp, err := os.ReadFile(final + caSidecarSuffix) //nolint:gosec // node-local golden path
	return err == nil && m.egressCA != nil && string(fp) == m.egressCA.Fingerprint()
}

// exportGolden exports snap into final through a unique sibling *.tmp dir:
// a crash mid-export never leaves a half-written dir that would pass for a
// golden, and concurrent promotes to one name cannot clobber each other's
// staging (last rename wins whole).
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

// sourceSnap picks the snapshot to export a claimed sandbox from: the wake
// image of a hibernated one (kept — the wake still needs it), or a transient
// capture of a running one, dropped by the returned cleanup.
func (m *Manager) sourceSnap(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	if sb.HibernateSnap != "" {
		return sb.HibernateSnap, func() {}, nil
	}
	snap := forkPrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		return "", nil, err
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}

// exportSource captures a claimed sandbox's state into exportDir, returning
// the snapshot name it exported. Only this window holds the transition lock —
// it pins the source snapshot against a concurrent wake consuming it; the
// minutes-long clone fan-out after it must not block the source's own
// wake/hibernate traffic.
func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir string) (string, error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return "", err
	}
	defer cleanup()
	return snap, m.eng.SnapshotExport(ctx, snap, exportDir)
}

// provision creates one claim-ready VM: clone from a golden when available,
// cold-boot the template otherwise.
func (m *Manager) provision(ctx context.Context, key types.PoolKey, golden string) (*types.Sandbox, error) {
	if golden == "" {
		sb, err := m.provisionVM(ctx, key, coldProbeTimeout, func(name string) (types.VMRecord, error) {
			return m.eng.RunCold(ctx, name, key)
		})
		if err != nil {
			return nil, err
		}
		// A pre-golden cold claim must trust the root too, or its intercepted
		// hosts fail TLS for the sandbox's whole life.
		if m.poolIntercepts(key) {
			if err := m.eng.InstallCACert(ctx, sb.VsockSocket, m.egressCA.CertPEM()); err != nil {
				m.destroy(ctx, sb.VMName)
				return nil, fmt.Errorf("install egress ca: %w", err)
			}
		}
		return sb, nil
	}
	return m.provisionVM(ctx, key, claimProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, key)
	})
}

// provisionVM creates and probes one VM, cleaning up any failure.
func (m *Manager) provisionVM(ctx context.Context, key types.PoolKey, probeTimeout time.Duration, create func(name string) (types.VMRecord, error)) (*types.Sandbox, error) {
	sb, err := m.startVM(ctx, key, create)
	if err != nil {
		return nil, err
	}
	return m.readyVM(ctx, sb, time.Now().Add(probeTimeout))
}

func (m *Manager) startVM(ctx context.Context, key types.PoolKey, create func(name string) (types.VMRecord, error)) (*types.Sandbox, error) {
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

// cloneBatch creates and probes a fork batch through separate gates.
func (m *Manager) cloneBatch(ctx context.Context, count int, startOne func() (*types.Sandbox, error)) ([]*types.Sandbox, error) {
	children := make([]*types.Sandbox, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		m.refillSem <- struct{}{}
		wg.Go(func() {
			child, err := startOne()
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

// probeReady waits until a VM's silkd answers, returning its vsock socket. sock
// is what the lifecycle command reported; when empty (a heavy image can boot
// past cocoon's post-start inspect) it polls `vm list` until the socket appears.
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
	vms, err := m.eng.List(ctx, name)
	if err != nil {
		return "", err
	}
	i := slices.IndexFunc(vms, func(vm types.VMRecord) bool { return vm.Config.Name == name })
	if i < 0 {
		return "", fmt.Errorf("vm %s not found after create", name)
	}
	if vms[i].VsockSocket == "" {
		return "", fmt.Errorf("vm %s has no vsock socket", name)
	}
	return vms[i].VsockSocket, nil
}

// runBounded fans f over n items on the refill semaphore, so engine
// batches (reap destroys, idle hibernates, reconcile sweeps) share the
// same node-wide concurrency budget as refills without blocking the
// caller. Callers that need completion Wait; fire-and-forget drops it.
func (m *Manager) runBounded(ctx context.Context, n int, f func(context.Context, int)) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := range n {
		// Acquire inside the goroutine: a batch larger than the budget
		// must not block the caller (Run's select loop).
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

// removeVM deletes a VM (on a cancellation-immune ctx, else `cocoon vm rm`
// no-ops and orphans it) and reports whether it is confirmed gone; a failed
// remove leaves it running, so the caller must keep its lock.
func (m *Manager) removeVM(ctx context.Context, name string) bool {
	if err := m.eng.Remove(context.WithoutCancel(ctx), name); err != nil {
		log.WithFunc("pool.removeVM").Errorf(ctx, err, "remove vm %s", name)
		return false
	}
	return true
}

func (m *Manager) destroy(ctx context.Context, name string) { _ = m.removeVM(ctx, name) }

func (m *Manager) dropSnap(ctx context.Context, snap string) {
	if snap == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.SnapshotRemove(ctx, snap); err != nil {
		log.WithFunc("pool.dropSnap").Warnf(ctx, "drop snapshot %s: %v", snap, err)
	}
}
