package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	vmmRestartBudget = 3
	vmmRestartWindow = 10 * time.Minute
	// with the cocoon daemon stream live, the list only catches an event the stream dropped
	livenessBackstop   = time.Minute
	vmEventsBackoff    = time.Second
	vmEventsMaxBackoff = 30 * time.Second

	reasonColdBootFailed = "vmm exited; cold boot failed"
)

type liveSuspect struct {
	sb     *types.Sandbox
	vmName string
}

// livenessOnce finds claims whose VMM exited outside a transition and restarts or fails each.
func (m *Manager) livenessOnce(ctx context.Context) {
	if m.vmmEvents.Load() && time.Since(time.Unix(0, m.lastVMPoll.Load())) < livenessBackstop {
		return
	}
	if !m.livenessSweep.CompareAndSwap(false, true) {
		return
	}
	watched := m.watchedClaims()
	if len(watched) == 0 {
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
		m.lastVMPoll.Store(time.Now().UnixNano())
		up := make(map[string]bool, len(vms))
		for _, vm := range vms {
			up[vm.Config.Name] = vm.State == vmStateRunning
		}
		m.recoverDown(ctx, watched, up).Wait()
	}()
}

// watchVMMs follows cocoon daemon's event stream, reconnecting with backoff while the poll covers the gap.
func (m *Manager) watchVMMs(ctx context.Context) {
	logger := log.WithFunc("pool.watchVMMs")
	backoff := vmEventsBackoff
	for ctx.Err() == nil {
		opened := time.Now()
		err := m.eng.VMEvents(ctx, m.cocoondSocket,
			func(vms []engine.VMStatus) { m.onVMSync(ctx, vms) },
			func(change engine.VMChange) { m.onVMChange(ctx, change) })
		m.vmmEvents.Store(false)
		if ctx.Err() != nil {
			return
		}
		logger.Warnf(ctx, "cocoon daemon events: %v; polling until the stream is back", err)
		if time.Since(opened) > vmEventsMaxBackoff {
			backoff = vmEventsBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, vmEventsMaxBackoff)
	}
}

// onVMSync treats the stream's opening snapshot like one poll.
func (m *Manager) onVMSync(ctx context.Context, vms []engine.VMStatus) {
	up := make(map[string]bool, len(vms))
	for _, vm := range vms {
		up[vm.Name] = vm.Live
	}
	m.vmmEvents.Store(true)
	m.lastVMPoll.Store(time.Now().UnixNano())
	m.recoverDown(ctx, m.watchedClaims(), up)
}

func (m *Manager) onVMChange(ctx context.Context, change engine.VMChange) {
	if change.VM.Live && change.Kind != engine.VMDeleted {
		return
	}
	watched := slices.DeleteFunc(m.watchedClaims(), func(s liveSuspect) bool { return s.vmName != change.VM.Name })
	m.recoverDown(ctx, watched, nil)
}

func (m *Manager) recoverDown(ctx context.Context, watched []liveSuspect, up map[string]bool) *sync.WaitGroup {
	down := slices.DeleteFunc(watched, func(s liveSuspect) bool { return up[s.vmName] })
	return m.runBounded(ctx, len(down), func(ctx context.Context, i int) { m.recoverVMM(ctx, down[i].sb) })
}

// watchedClaims lists the claims whose VM should be up, with the VM name read under m.mu.
func (m *Manager) watchedClaims() []liveSuspect {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []liveSuspect
	for _, sb := range m.claimed {
		if _, archiving := m.archiving[sb.ID]; !archiving && runningShaped(sb) && !leaseLapsed(sb, now) {
			out = append(out, liveSuspect{sb, sb.VMName})
		}
	}
	return out
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
	vm, found, err := m.eng.Inspect(ctx, sb.VMName)
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
	_, found, err := m.eng.Inspect(ctx, sb.VMName)
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
	// cocoon keeps a VM's run dir across a cold boot, so the journaled socket path still holds
	sock, err := m.probeReady(ctx, sb.VMName, sb.VsockSocket, coldProbeTimeout)
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
	logger := log.WithFunc("pool.restartLocked")
	// a displaced door's close unlinks the rebound path, so the old door goes first
	m.disarmEgress(sb.ID, false)
	if err := m.armEgressProxy(ctx, sb); err != nil {
		logger.Errorf(ctx, err, "arm egress proxy %s", sb.ID)
	}
	if m.disarmIfReleased(sb) {
		return ErrUnknownSandbox
	}
	if err := m.deliverEnv(ctx, []*types.Sandbox{sb}, false); err != nil {
		logger.Errorf(ctx, err, "redeliver env to %s", sb.ID)
	}
	sb.ExitRecorded = false
	sb.Touch()
	m.counters.vmmRestarts.Add(1)
	m.recordUsage(ctx, usageEvent{Event: "vmm_restart", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
	logger.Warnf(ctx, "restarted %s (%s) from its disk; guest memory is lost", sb.ID, sb.VMName)
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
