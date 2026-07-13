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

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.draining {
		return
	}
	now := time.Now()
	for _, p := range m.pools {
		target := p.effectiveTarget(now)
		if p.goldenDir == "" {
			if !p.building && now.After(p.nextBuild) {
				p.building = true
				go m.buildGolden(ctx, p)
			}
			continue
		}
		golden := p.goldenDir
		for len(p.warm)+p.refilling < target {
			select {
			case m.refillSem <- struct{}{}:
				p.refilling++
				go m.refillOne(ctx, p, golden)
			default:
				return
			}
		}
	}
}

func (m *Manager) refillOne(ctx context.Context, p *pool, golden string) {
	releaseSlot := true
	defer func() {
		if releaseSlot {
			<-m.refillSem
		}
	}()
	start := time.Now()
	sb, err := m.provision(ctx, p.key, golden)
	keep := false
	m.mu.Lock()
	p.refilling--
	target := p.effectiveTarget(time.Now())
	if err == nil && !m.draining {
		if len(p.warm) < target {
			p.warm = append(p.warm, sb)
			p.noteLead(time.Since(start))
			keep = true
		}
		if p.goldenDir != "" && len(p.warm)+p.refilling < target {
			p.refilling++
			releaseSlot = false
			go m.refillOne(ctx, p, p.goldenDir)
		}
	}
	m.mu.Unlock()
	if err != nil {
		log.WithFunc("pool.refillOne").Errorf(ctx, err, "refill %s", p.key.Hash())
		return
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
	return m.writeGoldenCASidecar(final, caBaked)
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

// provisionSnap creates one claim-ready clone from a local-store snapshot —
// fork's fast path, which skips the export-to-dir copy.
func (m *Manager) provisionSnap(ctx context.Context, key types.PoolKey, snap string) (*types.Sandbox, error) {
	return m.provisionVM(ctx, key, claimProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.CloneSnap(ctx, snap, name, key)
	})
}

// provisionVM creates one VM via create, probes it ready, and destroys it on
// any failure — including create-command failures, which can leave a half-
// created VM behind (e.g. the CLI killed by timeout after the VMM spawned).
func (m *Manager) provisionVM(ctx context.Context, key types.PoolKey, probeTimeout time.Duration, create func(name string) (types.VMRecord, error)) (*types.Sandbox, error) {
	name := vmName(key)
	rec, err := create(name)
	var sock string
	if err == nil {
		sock, err = m.probeReady(ctx, name, rec.VsockSocket, probeTimeout)
	}
	if err != nil {
		m.destroy(ctx, name)
		return nil, err
	}
	return &types.Sandbox{VMName: name, Key: key, VsockSocket: sock, TAP: rec.TapDevice()}, nil
}

// cloneBatch builds count claim-ready VMs via provisionOne, bounded by the
// refill semaphore — forks and refills contend for the same node resources,
// so they share one gate. One failure destroys the batch.
func (m *Manager) cloneBatch(ctx context.Context, count int, provisionOne func() (*types.Sandbox, error)) ([]*types.Sandbox, error) {
	children := make([]*types.Sandbox, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		m.refillSem <- struct{}{}
		wg.Go(func() {
			defer func() { <-m.refillSem }()
			children[i], errs[i] = provisionOne()
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
	wg.Add(n)
	for i := range n {
		// Acquire inside the goroutine: a batch larger than the budget
		// must not block the caller (Run's select loop).
		go func() {
			defer wg.Done()
			select {
			case m.refillSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-m.refillSem }()
			f(ctx, i)
		}()
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
