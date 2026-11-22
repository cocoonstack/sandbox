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
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.claimed[sb.ID] != sb || bootID == sb.BootID:
	case sb.ColdBooting:
		sb.BootID = bootID
	case runningShaped(sb):
		sb.BootID, sb.RebootPending = bootID, true
		m.store.recommit(ctx, m.store.set(sb))
		m.queueRebootLocked(ctx, sb)
		return
	}
	log.WithFunc("pool.Rebooted").Debugf(ctx, "no reboot settle for the boot notice %s of %s", bootID, sb.ID)
}

func (m *Manager) rebootArmed(ctx context.Context, sb *types.Sandbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb.RebootPending && m.claimed[sb.ID] == sb {
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
	live := m.ownsRunLocked(sb, run) && runningShaped(sb) && !sb.ColdBooting
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
	mounts := slices.DeleteFunc(slices.Clone(sb.Volumes), func(v types.Volume) bool { return v.AttachOnly })
	m.mu.Unlock()
	if err := m.eng.Probe(ctx, sock, claimProbeTimeout); err != nil {
		return 0, err
	}
	if env.HasGuest() {
		if err := m.eng.WriteGuestEnv(ctx, sock, env.GuestFile()); err != nil {
			return 0, fmt.Errorf("write env: %w", err)
		}
	}
	var errs []error
	for _, v := range mounts {
		if err := m.checkVolumeRun(ctx, sb, run); err != nil {
			return len(mounts), errors.Join(append(errs, err)...)
		}
		if err := m.eng.MountLiveVolume(ctx, sock, v.Name, v.Mount, v.RW()); err != nil {
			errs = append(errs, fmt.Errorf("mount volume %q: %w", v.Name, err))
		}
	}
	return len(mounts), errors.Join(errs...)
}
