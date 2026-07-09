package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (m *Manager) refillOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	defer func() { <-m.refillSem }()
	start := time.Now()
	sb, err := m.provision(ctx, p.key, golden)
	keep := false
	m.mu.Lock()
	p.refilling--
	target := p.effectiveTarget(time.Now())
	if err == nil && len(p.warm) < target {
		p.warm = append(p.warm, sb)
		p.noteLead(time.Since(start))
		keep = true
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
	sock, err := m.eng.RunCold(ctx, name, key)
	if err != nil {
		return err
	}
	if _, err := m.probeReady(ctx, name, sock, coldProbeTimeout); err != nil {
		return err
	}
	if err := m.eng.SnapshotSave(ctx, name, snap); err != nil {
		return err
	}
	return m.exportGolden(ctx, snap, final)
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

// exportSource captures a claimed sandbox's state into exportDir. Only this
// window holds the transition lock — it pins the source snapshot against a
// concurrent wake consuming it; the minutes-long clone fan-out after it must
// not block the source's own wake/hibernate traffic.
func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir string) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		return err
	}
	defer cleanup()
	return m.eng.SnapshotExport(ctx, snap, exportDir)
}

// provision creates one claim-ready VM: clone from a golden when available,
// cold-boot the template otherwise. The VM is destroyed on any failure —
// including create-command failures, which can leave a half-created VM
// behind (e.g. the CLI killed by timeout after the VMM spawned).
func (m *Manager) provision(ctx context.Context, key types.PoolKey, golden string) (*types.Sandbox, error) {
	name := vmName(key)
	probeTimeout := claimProbeTimeout
	var sock string
	var err error
	if golden != "" {
		sock, err = m.eng.Clone(ctx, golden, name, key)
	} else {
		sock, err = m.eng.RunCold(ctx, name, key)
		probeTimeout = coldProbeTimeout
	}
	if err == nil {
		sock, err = m.probeReady(ctx, name, sock, probeTimeout)
	}
	if err != nil {
		m.destroy(ctx, name)
		return nil, err
	}
	return &types.Sandbox{VMName: name, Key: key, VsockSocket: sock}, nil
}

// cloneBatch builds count claim-ready clones from an exported snapshot dir,
// bounded by the refill semaphore — forks and refills contend for the same
// node resources, so they share one gate. One failure destroys the batch.
func (m *Manager) cloneBatch(ctx context.Context, key types.PoolKey, dir string, count int) ([]*types.Sandbox, error) {
	children := make([]*types.Sandbox, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		m.refillSem <- struct{}{}
		wg.Go(func() {
			defer func() { <-m.refillSem }()
			children[i], errs[i] = m.provision(ctx, key, dir)
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

// probeReady waits until a VM's silkd answers, returning its vsock socket —
// the claim-ready gate after clone, cold-run, or restore. sock is the socket
// the lifecycle command already reported; when empty (cocoon's post-start
// inspect ran before the VMM bound it — a heavy image's android 8G alloc
// widens that window) it falls back to polling `vm list` until the socket
// appears, within the same probe budget.
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
	for _, vm := range vms {
		if vm.Config.Name != name {
			continue
		}
		if vm.VsockSocket == "" {
			return "", fmt.Errorf("vm %s has no vsock socket", name)
		}
		return vm.VsockSocket, nil
	}
	return "", fmt.Errorf("vm %s not found after create", name)
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

// destroy removes a VM on a cancellation-immune ctx: cleanup is usually
// triggered by a failed or abandoned request, and running `cocoon vm rm` on
// the caller's canceled ctx would no-op and orphan a live VM.
func (m *Manager) destroy(ctx context.Context, name string) {
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.Remove(ctx, name); err != nil {
		log.WithFunc("pool.destroy").Errorf(ctx, err, "remove vm %s", name)
	}
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
