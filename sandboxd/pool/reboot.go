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

const rebootSettleGap = 30 * time.Second

type rebootGate struct {
	next   time.Time
	queued bool
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
	m.reboots[sb] = rebootGate{next: gate.next, queued: true}
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
	if live {
		m.reboots[sb] = rebootGate{next: time.Now().Add(rebootSettleGap)}
	} else {
		delete(m.reboots, sb)
	}
	m.mu.Unlock()
	if !live {
		return
	}
	mounted, restoreErr := m.restoreRebootedGuest(runCtx, sb, run)
	if live, _ = m.commitIfLive(ctx, sb, func() {
		sb.Restarts++
		sb.RestartedAt = time.Now()
		sb.RebootPending = m.reboots[sb].queued
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

func (m *Manager) restoreRebootedGuest(ctx context.Context, sb *types.Sandbox, run *volumeRun) (int, error) {
	m.mu.Lock()
	env, sock := sb.Env, sb.VsockSocket
	mounts := slices.DeleteFunc(slices.Clone(sb.Volumes), func(v types.Volume) bool {
		return v.AttachOnly || volumeIndex(sb.VolumesWant, v.Name) < 0
	})
	m.mu.Unlock()
	if err := m.eng.Probe(ctx, sock, claimProbeTimeout); err != nil {
		return 0, err
	}
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
