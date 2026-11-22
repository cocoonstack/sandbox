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
	rebootSettleGap    = 30 * time.Second
	rebootProbeRetries = 3
)

type rebootGate struct {
	next    time.Time
	queued  bool
	retries int
}

func (m *Manager) Rebooted(ctx context.Context, sb *types.Sandbox, bootID string) {
	var (
		js       claimSnapshot
		accepted bool
		journal  bool
	)
	m.mu.Lock()
	switch {
	case m.claimed[sb.ID] != sb || bootID == sb.BootID:
	case sb.ColdBooting:
		sb.BootID = bootID
	default:
		accepted, journal = true, !sb.RebootPending
		sb.BootID, sb.RebootPending = bootID, true
		gate := m.reboots[sb]
		gate.retries = 0
		m.reboots[sb] = gate
		if journal {
			js = m.store.set(sb)
		}
		if runningShaped(sb) {
			m.queueRebootLocked(ctx, sb)
		}
	}
	m.mu.Unlock()
	logger := log.WithFunc("pool.Rebooted")
	if !accepted {
		logger.Debugf(ctx, "no reboot settle for the boot notice %s of %s", bootID, sb.ID)
	}
	if !journal {
		return
	}
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
		logger.Errorf(ctx, err, "persist the reboot notice of %s", sb.ID)
	}
}

func (m *Manager) rebootArmed(ctx context.Context, sb *types.Sandbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb.RebootPending && !sb.ColdBooting && m.claimed[sb.ID] == sb {
		m.queueRebootLocked(ctx, sb)
	}
}

func (m *Manager) queueRebootLocked(ctx context.Context, sb *types.Sandbox) {
	gate := m.reboots[sb]
	if gate.queued {
		return
	}
	gate.queued = true
	m.reboots[sb] = gate
	ctx = context.WithoutCancel(ctx)
	time.AfterFunc(time.Until(gate.next), func() { m.settleReboot(ctx, sb) })
}

func (m *Manager) settleReboot(ctx context.Context, sb *types.Sandbox) {
	runCtx, run, err := m.beginVolumeRun(ctx, sb)
	if err != nil {
		return
	}
	defer m.finishVolumeRun(sb, run)
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	live := m.ownsRunLocked(sb, run) && runningShaped(sb) && !sb.ColdBooting && sb.RebootPending
	gate, sock := m.reboots[sb], sb.VsockSocket
	if live {
		gate.next, gate.queued = time.Now().Add(rebootSettleGap), false
		m.reboots[sb] = gate
	} else {
		delete(m.reboots, sb)
	}
	m.mu.Unlock()
	if !live {
		return
	}
	if err := m.eng.Probe(runCtx, sock, claimProbeTimeout); err != nil {
		m.retryReboot(ctx, sb, err)
		return
	}
	mounted, restoreErr := m.restoreRebootedGuest(runCtx, sb, run)
	if live, _ = m.commitIfLive(ctx, sb, func() {
		sb.Restarts++
		sb.RestartedAt = time.Now()
		done := m.reboots[sb]
		sb.RebootPending, done.retries = done.queued, 0
		m.reboots[sb] = done
	}); !live {
		return
	}
	logger := log.WithFunc("pool.settleReboot")
	if restoreErr != nil {
		logger.Errorf(ctx, restoreErr, "guest of %s rebooted inside its VMM; restore its env and volumes", sb.ID)
		return
	}
	logger.Warnf(ctx, "guest of %s rebooted inside its VMM; restored env and %d volumes", sb.ID, mounted)
}

func (m *Manager) retryReboot(ctx context.Context, sb *types.Sandbox, probeErr error) {
	m.mu.Lock()
	gate := m.reboots[sb]
	waiting := m.claimed[sb.ID] == sb && !gate.queued
	retry := waiting && gate.retries < rebootProbeRetries
	if retry {
		gate.retries++
		m.reboots[sb] = gate
		m.queueRebootLocked(ctx, sb)
	}
	m.mu.Unlock()
	logger := log.WithFunc("pool.retryReboot")
	switch {
	case !waiting:
		return
	case retry:
		logger.Warnf(ctx, "guest of %s did not answer its reboot settle, retry %d of %d in %s: %v", sb.ID, gate.retries, rebootProbeRetries, rebootSettleGap, probeErr)
	default:
		logger.Warnf(ctx, "guest of %s did not answer its reboot settle; its next arm or notice settles it: %v", sb.ID, probeErr)
	}
}

func (m *Manager) restoreRebootedGuest(ctx context.Context, sb *types.Sandbox, run *volumeRun) (int, error) {
	m.mu.Lock()
	env, sock := sb.Env, sb.VsockSocket
	mounts := slices.DeleteFunc(slices.Clone(sb.Volumes), func(v types.Volume) bool {
		return v.AttachOnly || volumeIndex(sb.VolumesWant, v.Name) < 0
	})
	m.mu.Unlock()
	var errs []error
	if env.HasGuest() {
		if err := m.eng.WriteGuestEnv(ctx, sock, env.GuestFile()); err != nil {
			errs = append(errs, fmt.Errorf("write env: %w", err))
		}
	}
	for _, v := range mounts {
		if err := m.checkVolumeRun(ctx, sb, run); err != nil {
			return len(mounts), errors.Join(append(errs, err)...)
		}
		err := m.eng.MountLiveVolume(ctx, sock, v.Name, v.Mount, v.RW())
		if err != nil {
			m.recordVolumeError(sb, run, v.Name, fmt.Errorf("%w: %w", errRemountFailed, err))
			dropErr := m.commitVolumeApplied(ctx, sb, run, v, false)
			errs = append(errs, fmt.Errorf("mount volume %q: %w", v.Name, err), dropErr)
			continue
		}
		m.clearVolumeError(sb, run, v.Name)
	}
	return len(mounts), errors.Join(errs...)
}
