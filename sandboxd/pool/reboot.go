package pool

import (
	"context"
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
	case sb.ColdBooting || sb.Waking:
		sb.BootID = bootID
	case runningShaped(sb):
		sb.BootID = bootID
		m.queueRebootLocked(ctx, sb)
		return
	}
	log.WithFunc("pool.Rebooted").Debugf(ctx, "no reboot settle for the boot notice %s of %s", bootID, sb.ID)
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
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	m.mu.Lock()
	live := m.claimed[sb.ID] == sb && runningShaped(sb) && !sb.ColdBooting
	if live {
		m.reboots[sb] = rebootGate{next: time.Now().Add(rebootSettleGap)}
	} else {
		delete(m.reboots, sb)
	}
	env, sock := sb.Env, sb.VsockSocket
	mounts := slices.DeleteFunc(slices.Clone(sb.Volumes), func(v types.Volume) bool { return v.AttachOnly })
	m.mu.Unlock()
	if !live {
		return
	}
	restoreErr := m.restoreRebootedGuest(ctx, sock, env, mounts)
	if live, _ = m.commitIfLive(ctx, sb, func() {
		sb.Restarts++
		sb.RestartedAt = time.Now()
	}); !live {
		return
	}
	logger := log.WithFunc("pool.settleReboot")
	if restoreErr != nil {
		logger.Errorf(ctx, restoreErr, "guest of %s rebooted inside its VMM; restore its env and volumes", sb.ID)
		return
	}
	logger.Warnf(ctx, "guest of %s rebooted inside its VMM; restored env and %d volumes", sb.ID, len(mounts))
}

func (m *Manager) restoreRebootedGuest(ctx context.Context, sock string, env types.Env, mounts []types.Volume) error {
	if err := m.eng.Probe(ctx, sock, claimProbeTimeout); err != nil {
		return err
	}
	if env.HasGuest() {
		if err := m.eng.WriteGuestEnv(ctx, sock, env.GuestFile()); err != nil {
			return fmt.Errorf("write env: %w", err)
		}
	}
	for _, v := range mounts {
		if err := m.eng.MountLiveVolume(ctx, sock, v.Name, v.Mount, v.RW()); err != nil {
			return fmt.Errorf("mount volume %q: %w", v.Name, err)
		}
	}
	return nil
}
