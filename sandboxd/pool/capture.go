package pool

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// sourceSnap picks the snapshot to export a claimed sandbox from, with its cleanup; the caller holds sb.Transition.
func (m *Manager) sourceSnap(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	// The archive clears VMName under the same lock, so this check cannot race it
	if sb.ArchiveCk != "" {
		return "", nil, ErrArchived
	}
	if sb.Failed != "" {
		return "", nil, ErrFailed
	}
	if sb.HibernateSnap != "" {
		return sb.HibernateSnap, func() {}, nil
	}
	snap := forkPrefix + strings.TrimPrefix(sb.VMName, vmPrefix) + "-" + randHex(3)
	intent, live := m.setIfLive(sb, func() { sb.SaveSnap = snap })
	if !live {
		return "", nil, ErrUnknownSandbox
	}
	if err := m.store.commit(intent); err != nil {
		m.setIfLive(sb, func() { sb.SaveSnap = "" })
		return "", nil, fmt.Errorf("save %s: persist intent: %w", sb.ID, err)
	}
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		_, settleErr := m.settleFailedCapture(ctx, sb)
		return "", nil, errors.Join(err, settleErr)
	}
	if _, err := m.commitIfLive(ctx, sb, func() { sb.SaveSnap = "" }); err != nil {
		log.WithFunc("pool.sourceSnap").Warnf(ctx, "clear save intent of %s: %v", sb.ID, err)
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}

func (m *Manager) captureAllowed(sb *types.Sandbox) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb {
		return ErrUnknownSandbox
	}
	if _, err := m.fenceWatch(sb); err != nil {
		return err
	}
	if m.hasVolumeHolds(sb) {
		return ErrVolumeCapture
	}
	return nil
}

func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir, from string) (snap, vmName string, guestEnv bool, tun outbound.TunState, err error) {
	if lockErr := m.lockTransition(sb); lockErr != nil {
		return "", "", false, outbound.TunState{}, lockErr
	}
	if from != "" && sb.HibernateSnap != from {
		sb.Transition.Unlock()
		return "", "", false, outbound.TunState{}, errWokeMeanwhile
	}
	if captureErr := m.captureAllowed(sb); captureErr != nil {
		sb.Transition.Unlock()
		return "", "", false, outbound.TunState{}, captureErr
	}
	if settleErr := m.settleCapture(ctx, sb); settleErr != nil {
		sb.Transition.Unlock()
		return "", "", false, outbound.TunState{}, settleErr
	}
	m.trimForCapture(ctx, sb)
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		sb.Transition.Unlock()
		return "", "", false, outbound.TunState{}, err
	}
	defer cleanup()
	vmName, guestEnv, tun = sb.VMName, m.heldGuestEnv(sb), m.tunState(sb)
	// A fresh snapshot is this capture's alone, so dials need not wait out its export; a wake consumes a hibernate snapshot
	if snap != sb.HibernateSnap {
		sb.Transition.Unlock()
	} else {
		defer sb.Transition.Unlock()
	}
	return snap, vmName, guestEnv, tun, m.eng.SnapshotExport(ctx, snap, exportDir)
}

// A failed trim only leaves the capture larger.
func (m *Manager) trimForCapture(ctx context.Context, sb *types.Sandbox) {
	if !m.view.Load().poolTrims[sb.PolicyKey()] || sb.HibernateSnap != "" || sb.ArchiveCk != "" {
		return
	}
	if err := m.eng.TrimCow(ctx, sb.VsockSocket); err != nil {
		log.WithFunc("pool.trimForCapture").Warnf(ctx, "trim %s before its capture: %v", sb.ID, err)
	}
}
