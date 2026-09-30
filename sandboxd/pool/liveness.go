package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	vmmRestartBudget = 3
	vmmRestartWindow = 10 * time.Minute

	reasonColdBootFailed = "vmm exited; cold boot failed"
)

// livenessOnce finds claims whose VMM exited outside a transition and restarts or fails each.
func (m *Manager) livenessOnce(ctx context.Context) {
	if !m.livenessSweep.CompareAndSwap(false, true) {
		return
	}
	type suspect struct {
		sb     *types.Sandbox
		vmName string
	}
	var running []suspect
	now := time.Now()
	m.mu.Lock()
	for _, sb := range m.claimed {
		if _, archiving := m.archiving[sb.ID]; !archiving && runningShaped(sb) && !leaseLapsed(sb, now) {
			running = append(running, suspect{sb, sb.VMName})
		}
	}
	m.mu.Unlock()
	if len(running) == 0 {
		m.livenessSweep.Store(false)
		return
	}
	go func() {
		defer m.livenessSweep.Store(false)
		vms, err := m.eng.List(ctx)
		if err != nil {
			log.WithFunc("pool.livenessOnce").Warnf(ctx, "list vms: %v", err)
			return
		}
		up := make(map[string]bool, len(vms))
		for _, vm := range vms {
			up[vm.Config.Name] = vm.State == vmStateRunning
		}
		down := slices.DeleteFunc(running, func(s suspect) bool { return up[s.vmName] })
		m.runBounded(ctx, len(down), func(ctx context.Context, i int) { m.recoverVMM(ctx, down[i].sb) }).Wait()
	}()
}

// recoverVMM re-checks one suspect under its Transition lock, then restarts it or marks it failed.
func (m *Manager) recoverVMM(ctx context.Context, sb *types.Sandbox) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	now := time.Now()
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb && runningShaped(sb) && !leaseLapsed(sb, now)
	m.mu.Unlock()
	if !live {
		return
	}
	logger := log.WithFunc("pool.recoverVMM")
	// the sweep's list predates this lock, so a wake that finished in between reads as down there
	vm, found, err := m.findVM(ctx, sb.VMName)
	switch {
	case err != nil:
		logger.Warnf(ctx, "list %s: %v", sb.VMName, err)
		return
	case found && vm.State == vmStateRunning:
		return
	}
	if !sb.ExitRecorded {
		sb.ExitRecorded = true
		m.recordUsage(ctx, usageEvent{Event: "vmm_exit", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
	}
	reason := coldBootBlocker(sb, found)
	if reason == "" && m.vmmRestart == types.VMMRestartNone {
		reason = "vmm exited"
	}
	sb.RestartLog = slices.DeleteFunc(sb.RestartLog, func(at time.Time) bool { return now.Sub(at) > vmmRestartWindow })
	if reason == "" && len(sb.RestartLog) >= vmmRestartBudget {
		reason = fmt.Sprintf("vmm exited after %d restarts within %s", len(sb.RestartLog), vmmRestartWindow)
	}
	if reason != "" {
		m.failLocked(ctx, sb, reason)
		return
	}
	sb.RestartLog = append(sb.RestartLog, now)
	err = m.restartLocked(ctx, sb)
	switch {
	case err == nil, errors.Is(err, ErrUnknownSandbox):
	case len(sb.RestartLog) < vmmRestartBudget:
		logger.Warnf(ctx, "restart %s: %v; retrying", sb.ID, err)
	default:
		logger.Errorf(ctx, err, "restart %s", sb.ID)
		m.failLocked(ctx, sb, reasonColdBootFailed)
	}
}

// restartFailed cold-boots a failed claim on an explicit wake and resets its restart budget.
func (m *Manager) restartFailed(ctx context.Context, sb *types.Sandbox) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	live, failed := m.claimed[sb.ID] == sb, sb.Failed != ""
	m.mu.Unlock()
	switch {
	case !live:
		return ErrUnknownSandbox
	case !failed:
		return nil
	}
	_, found, err := m.findVM(ctx, sb.VMName)
	if err != nil {
		return fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	if reason := coldBootBlocker(sb, found); reason != "" {
		return fmt.Errorf("%w: %s", ErrFailed, reason)
	}
	sb.RestartLog = nil
	if err = m.restartLocked(ctx, sb); err != nil {
		return fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	return nil
}

// restartLocked cold-boots sb's VM from its own disk and republishes it; the caller holds sb.Transition.
func (m *Manager) restartLocked(ctx context.Context, sb *types.Sandbox) error {
	ctx = context.WithoutCancel(ctx)
	if err := m.eng.Start(ctx, sb.VMName); err != nil {
		m.stopVM(ctx, sb.VMName)
		return fmt.Errorf("cold boot: %w", err)
	}
	sock, err := m.probeReady(ctx, sb.VMName, "", coldProbeTimeout)
	if err != nil {
		m.stopVM(ctx, sb.VMName)
		return fmt.Errorf("probe after cold boot: %w", err)
	}
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		sb.VsockSocket, sb.Failed = sock, ""
		sb.Restarts++
		sb.RestartedAt = time.Now()
		js = m.store.set(sb)
	}
	m.mu.Unlock()
	if !live {
		// the release that dropped the claim owns the VM teardown
		return ErrUnknownSandbox
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
	sb.ExitRecorded = false
	sb.Touch()
	m.counters.vmmRestarts.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "vmm_restart", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
	log.WithFunc("pool.restartLocked").Warnf(ctx, "restarted %s (%s) from its disk; guest memory is lost", sb.ID, sb.VMName)
	return nil
}

// failLocked parks sb as failed with reason; the caller holds sb.Transition.
func (m *Manager) failLocked(ctx context.Context, sb *types.Sandbox, reason string) {
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb
	var js claimSnapshot
	if live {
		sb.Failed = reason
		js = m.store.set(sb)
	}
	m.mu.Unlock()
	if !live {
		return
	}
	if err := m.store.commit(js); err != nil {
		m.recommit(ctx, js)
	}
	m.counters.vmmFailures.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "vmm_failed", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Reference: reason})
	log.WithFunc("pool.failLocked").Warnf(ctx, "sandbox %s failed: %s", sb.ID, reason)
}

// runningShaped reports whether sb's VM should be up; callers hold m.mu.
func runningShaped(sb *types.Sandbox) bool {
	return sb.HibernateSnap == "" && sb.PendingSnap == "" && sb.ArchiveCk == "" && sb.Failed == ""
}

// leaseLapsed reports whether reap destroys sb instead of archiving it; callers hold m.mu.
func leaseLapsed(sb *types.Sandbox, now time.Time) bool {
	return sb.OnExpire != types.ExpireArchive && !sb.Deadline.IsZero() && now.After(sb.Deadline)
}

// coldBootBlocker names why sb's VM cannot be cold-booted, "" when it can.
func coldBootBlocker(sb *types.Sandbox, found bool) string {
	switch {
	case !found:
		return "vm record gone"
	case sb.Key.Net == types.NetEgress:
		// the netdev nft chain dies with the tap, so the guest would boot with an unlocked NIC
		return "vmm exited; an egress-lane sandbox cannot cold-boot"
	case hasAppliedVolumes(sb):
		return "vmm exited; volume mounts do not survive a cold boot"
	default:
		return ""
	}
}
