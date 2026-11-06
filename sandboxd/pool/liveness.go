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
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

const (
	vmmRestartBudget = 3
	vmmRestartWindow = 10 * time.Minute
	// With the cocoon daemon stream live, the list only catches an event the stream dropped
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
	if !m.sweeps.liveness.CompareAndSwap(false, true) {
		return
	}
	watched := m.watchedClaims("")
	go func() {
		defer m.sweeps.liveness.Store(false)
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
		recovering := m.recoverDown(ctx, watched, up)
		m.evictDeadWarm(ctx, up).Wait()
		recovering.Wait()
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
	m.recoverDown(ctx, m.watchedClaims(""), up)
	m.evictDeadWarm(ctx, up)
}

func (m *Manager) onVMChange(ctx context.Context, change engine.VMChange) {
	if change.VM.Live && change.Kind != engine.VMDeleted {
		return
	}
	m.evictDeadWarm(ctx, map[string]bool{change.VM.Name: false})
	m.recoverDown(ctx, m.watchedClaims(change.VM.Name), nil)
}

func (m *Manager) recoverDown(ctx context.Context, watched []liveSuspect, up map[string]bool) *sync.WaitGroup {
	down := slices.DeleteFunc(watched, func(s liveSuspect) bool { return up[s.vmName] })
	return m.runBounded(ctx, len(down), func(ctx context.Context, i int) { m.recoverVMM(ctx, down[i].sb) })
}

// watchedClaims lists the claims whose VM should be up, only vmName's when it is set, with the VM name read under m.mu.
func (m *Manager) watchedClaims(vmName string) []liveSuspect {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	watched := func(sb *types.Sandbox) bool { return m.vmSettledLocked(sb) && !m.leaseLapsed(sb, now) }
	if vmName != "" {
		if sb := m.claimedVMs[vmName]; sb != nil && watched(sb) {
			return []liveSuspect{{sb, vmName}}
		}
		return nil
	}
	var out []liveSuspect
	for _, sb := range m.claimed {
		if watched(sb) {
			out = append(out, liveSuspect{sb, sb.VMName})
		}
	}
	return out
}

func (m *Manager) evictDeadWarm(ctx context.Context, up map[string]bool) *sync.WaitGroup {
	var suspects []liveSuspect
	m.mu.Lock()
	for _, p := range m.pools {
		for _, sb := range p.warm {
			if live, present := up[sb.VMName]; present && !live {
				suspects = append(suspects, liveSuspect{sb, sb.VMName})
			}
		}
	}
	m.mu.Unlock()
	return m.runBounded(ctx, len(suspects), func(ctx context.Context, i int) { m.evictWarm(ctx, suspects[i]) })
}

func (m *Manager) evictWarm(ctx context.Context, suspect liveSuspect) {
	logger := log.WithFunc("pool.evictWarm")
	vm, found, err := m.eng.Inspect(ctx, suspect.vmName)
	switch {
	case err != nil:
		logger.Warnf(ctx, "inspect %s: %v", suspect.vmName, err)
		return
	case found && vm.State == vmStateRunning:
		return
	}
	m.mu.Lock()
	evicted := false
	if p := m.pools[suspect.sb.Key]; p != nil {
		if i := slices.Index(p.warm, suspect.sb); i >= 0 {
			p.warm = slices.Delete(p.warm, i, i+1)
			evicted = true
		}
	}
	m.mu.Unlock()
	if !evicted {
		m.recoverDown(ctx, m.watchedClaims(suspect.vmName), nil)
		return
	}
	logger.Warnf(ctx, "warm vm %s exited; replacing it", suspect.vmName)
	m.destroy(ctx, suspect.vmName)
	m.kickRefill()
}

// recoverVMM re-checks one suspect under its Transition lock, then restarts it or marks it failed.
func (m *Manager) recoverVMM(ctx context.Context, sb *types.Sandbox) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	now := time.Now()
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb && m.vmSettledLocked(sb) && !m.leaseLapsed(sb, now) && !(sb.Replicated && m.isFenced())
	volumes := m.hasVolumeHolds(sb)
	m.mu.Unlock()
	if !live {
		return
	}
	logger := log.WithFunc("pool.recoverVMM")
	// The sweep's list predates this lock, so a wake that finished in between reads as down there
	vm, found, err := m.eng.Inspect(ctx, sb.VMName)
	switch {
	case err != nil:
		logger.Warnf(ctx, "inspect %s: %v", sb.VMName, err)
		return
	case found && vm.State == vmStateRunning:
		return
	}
	m.recordExit(ctx, sb)
	reason := coldBootBlocker(sb, found, volumes)
	if reason == "" && m.view.Load().vmmRestart == types.VMMRestartNone {
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
	live, failed, volumes := m.claimed[sb.ID] == sb, sb.Failed != "", m.hasVolumeHolds(sb)
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
	if reason := coldBootBlocker(sb, found, volumes); reason != "" {
		return fmt.Errorf("%w: %s", ErrFailed, reason)
	}
	if err = m.beginWake(sb); err != nil {
		return fmt.Errorf("wake %s: %w", sb.ID, err)
	}
	defer m.endWake(sb)
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
	m.mu.Lock()
	if m.claimed[sb.ID] == sb {
		sb.ExitRecorded = false
		m.recordUsage(ctx, usage.Event{Event: "vmm_restart", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
	}
	m.mu.Unlock()
	// cocoon keeps a VM's run dir across a cold boot, so the journaled socket path still holds
	sock, err := m.probeReady(ctx, sb.VMName, sb.VsockSocket, coldProbeTimeout)
	if err != nil {
		m.stopRestarted(ctx, sb)
		return fmt.Errorf("probe after cold boot: %w", err)
	}
	m.mu.Lock()
	sb.VsockSocket = sock
	env := sb.Env
	m.mu.Unlock()
	err = m.out.ArmProxy(ctx, sb)
	if err == nil && env.HasGuest() {
		err = m.eng.WriteGuestEnv(ctx, sock, env.GuestFile())
	}
	if err != nil {
		m.out.Disarm(sb.ID, false)
		m.stopRestarted(ctx, sb)
		return fmt.Errorf("restore egress and env after cold boot: %w", err)
	}
	if live, _ := m.commitIfLive(ctx, sb, func() {
		sb.Failed = ""
		sb.Restarts++
		sb.RestartedAt = time.Now()
	}); !live {
		// The release that dropped the claim owns the VM teardown
		m.out.Disarm(sb.ID, true)
		return ErrUnknownSandbox
	}
	if m.disarmIfReleased(sb) {
		return ErrUnknownSandbox
	}
	sb.Touch()
	log.WithFunc("pool.restartLocked").Warnf(ctx, "restarted %s (%s) from its disk; guest memory is lost", sb.ID, sb.VMName)
	return nil
}

func (m *Manager) stopRestarted(ctx context.Context, sb *types.Sandbox) {
	m.stopVM(ctx, sb.VMName)
	m.recordExit(ctx, sb)
}

func (m *Manager) recordExit(ctx context.Context, sb *types.Sandbox) {
	if !sb.ExitRecorded {
		sb.ExitRecorded = true
		m.recordUsage(ctx, usage.Event{Event: "vmm_exit", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant})
	}
}

// failLocked parks sb as failed with reason; the caller holds sb.Transition.
func (m *Manager) failLocked(ctx context.Context, sb *types.Sandbox, reason string) {
	if live, _ := m.commitIfLive(ctx, sb, func() { sb.Failed = reason }); !live {
		return
	}
	m.recordExit(ctx, sb)
	m.recordUsage(ctx, usage.Event{Event: "vmm_failed", ID: sb.ID, VMName: sb.VMName, Tenant: sb.Tenant, Reference: reason})
	log.WithFunc("pool.failLocked").Warnf(ctx, "sandbox %s failed: %s", sb.ID, reason)
}

// leaseLapsed reports whether reap destroys sb instead of archiving it; callers hold m.mu.
func (m *Manager) leaseLapsed(sb *types.Sandbox, now time.Time) bool {
	return (sb.OnExpire != types.ExpireArchive || m.tenantGone(sb.Tenant)) && !sb.Deadline.IsZero() && now.After(sb.Deadline)
}

func (m *Manager) vmSettledLocked(sb *types.Sandbox) bool {
	_, archiving := m.archiving[sb.ID]
	_, moving := m.moving[sb.ID]
	return runningShaped(sb) && !archiving && !moving
}

// runningShaped reports whether sb's VM should be up; callers hold m.mu or sb.Transition.
func runningShaped(sb *types.Sandbox) bool {
	return sb.HibernateSnap == "" && sb.PendingSnap == "" && sb.ArchiveCk == "" && sb.Failed == "" && !sb.MoveIn
}

// coldBootBlocker names why sb's VM cannot be cold-booted, "" when it can.
func coldBootBlocker(sb *types.Sandbox, found, volumes bool) string {
	switch {
	case !found:
		return "vm record gone"
	case sb.Key.Net == types.NetEgress:
		return "vmm exited; an egress-lane sandbox cannot cold-boot"
	case volumes:
		return "vmm exited; volume mounts do not survive a cold boot"
	default:
		return ""
	}
}
