package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type vmProvisioner func(name string) (types.VMRecord, error)

func (m *Manager) provision(ctx context.Context, key types.PoolKey, golden string) (*types.Sandbox, error) {
	if golden == "" {
		return m.provisionCold(ctx, key)
	}
	return m.provisionVM(ctx, key, claimProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.Clone(ctx, golden, name, key)
	})
}

func (m *Manager) provisionResolved(ctx context.Context, key types.PoolKey, g goldenResolution) (*types.Sandbox, error) {
	if g.held == nil {
		return m.provision(ctx, key, g.dir)
	}
	return m.provisionVM(ctx, key, claimProbeTimeout, func(name string) (types.VMRecord, error) {
		rec, err := m.eng.Clone(ctx, g.dir, name, key)
		m.noteClaimClone(ctx, g, err)
		return rec, err
	})
}

func (m *Manager) noteClaimClone(ctx context.Context, g goldenResolution, err error) {
	capReason := engine.CapacitySignature(err)
	m.mu.Lock()
	g.held.countClone(ctx, g.gen, err, capReason)
	retired := m.retireUnclonableLocked(g.held, g.gen, time.Now())
	m.mu.Unlock()
	if retired {
		m.rebuildRetiredGolden(ctx, g.held.hash)
	}
}

func (m *Manager) provisionCold(ctx context.Context, key types.PoolKey) (*types.Sandbox, error) {
	sb, err := m.provisionVM(ctx, key, coldProbeTimeout, func(name string) (types.VMRecord, error) {
		return m.eng.RunCold(ctx, name, key, m.view.Load().poolStorage[key])
	})
	if err != nil {
		return nil, err
	}
	// A pre-golden cold claim must trust the root, or intercepted hosts fail TLS for its life
	if err := m.out.PrepareGuest(ctx, m.view.Load().out, key, sb.VsockSocket); err != nil {
		m.destroy(ctx, sb.VMName)
		return nil, err
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

// probeReady waits until a VM's silkd answers, polling `vm inspect` when sock is empty.
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
