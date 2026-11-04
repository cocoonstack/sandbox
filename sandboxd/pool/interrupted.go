package pool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	captureResumeBudget = 30 * time.Minute
	captureResumeRetry  = 5 * time.Second
)

func (m *Manager) settleInterrupted(ctx context.Context, sb *types.Sandbox) {
	defer sb.Transition.Unlock()
	logger := log.WithFunc("pool.settleInterrupted")
	m.mu.Lock()
	sb.ReaderSafe = false
	snap := cmp.Or(sb.PendingSnap, sb.SaveSnap)
	m.mu.Unlock()
	logger.Warnf(ctx, "%s has an interrupted capture %s; holding it until the capture exits", sb.ID, snap)
	if _, err := m.settleCapture(ctx, sb); err != nil {
		logger.Warnf(ctx, "%s keeps its capture intent for the next start: %v", sb.ID, err)
		return
	}
	logger.Infof(ctx, "%s settled its interrupted capture", sb.ID)
}

func (m *Manager) settleCapture(ctx context.Context, sb *types.Sandbox) (adopted bool, err error) {
	m.mu.Lock()
	pending, saved, vmName, sock := sb.PendingSnap, sb.SaveSnap, sb.VMName, sb.VsockSocket
	m.mu.Unlock()
	if err = m.eng.WaitCapture(ctx, cmp.Or(pending, saved)); err != nil {
		return false, err
	}
	var rec types.VMRecord
	err = retryCapture(ctx, func() (bool, error) {
		var inspectErr error
		rec, _, inspectErr = m.eng.Inspect(ctx, vmName)
		return false, inspectErr
	})
	if err != nil {
		return false, err
	}
	snaps, err := m.eng.SnapshotList(ctx)
	if err != nil {
		return false, err
	}
	running := rec.State == vmStateRunning
	switch {
	// A listed hibernate snapshot is the guest as it last ran, so it is the wake image in any VMM state
	case pending != "" && slices.Contains(snaps, pending):
		if running {
			if err = m.eng.Stop(ctx, vmName); err != nil {
				return false, err
			}
		}
	case running:
		if err = m.resumeGuest(ctx, sb, sock); err != nil {
			if ctx.Err() != nil || errors.Is(err, engine.ErrStopping) {
				return false, err
			}
			m.failLocked(ctx, sb, fmt.Sprintf("capture stopped short: guest did not resume: %v", err))
		}
	}
	adopted, err = m.settlePendingSnap(ctx, sb)
	if adopted {
		m.out.Disarm(sb.ID, true)
	}
	if saved != "" {
		if _, commitErr := m.commitIfLive(ctx, sb, func() { sb.SaveSnap = "" }); commitErr != nil {
			err = errors.Join(err, commitErr)
		}
		if slices.Contains(snaps, saved) {
			m.dropSnap(ctx, saved)
		}
	}
	return adopted, err
}

func (m *Manager) resumeGuest(ctx context.Context, sb *types.Sandbox, sock string) error {
	err := retryCapture(ctx, func() (bool, error) {
		startErr := m.eng.Start(ctx, sb.VMName)
		if startErr == nil {
			return false, nil
		}
		m.mu.Lock()
		live := m.claimed[sb.ID] == sb
		m.mu.Unlock()
		log.WithFunc("pool.resumeGuest").Warnf(ctx, "%s has not resumed: %v", sb.ID, startErr)
		return !live || errors.Is(startErr, engine.ErrStopping), startErr
	})
	if err != nil {
		return err
	}
	return m.eng.Probe(ctx, sock, coldProbeTimeout)
}

func retryCapture(ctx context.Context, try func() (final bool, err error)) error {
	deadline := time.Now().Add(captureResumeBudget)
	for {
		final, err := try()
		if err == nil || final || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(captureResumeRetry):
		}
	}
}
