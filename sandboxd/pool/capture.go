package pool

import (
	"context"
	"strings"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// sourceSnap picks the snapshot to export a claimed sandbox from, with its cleanup; the caller holds sb.Transition.
func (m *Manager) sourceSnap(ctx context.Context, sb *types.Sandbox) (string, func(), error) {
	// archive() clears VMName under the same lock, so this is the check that cannot race it
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
	if err := m.eng.SnapshotSave(ctx, sb.VMName, snap); err != nil {
		return "", nil, err
	}
	return snap, func() { m.dropSnap(ctx, snap) }, nil
}

func (m *Manager) captureAllowed(sb *types.Sandbox) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb {
		return ErrUnknownSandbox
	}
	if m.hasVolumeHolds(sb) {
		return ErrVolumeCapture
	}
	return nil
}

// exportSource captures a claimed sandbox into exportDir and returns its snapshot, VM name and guest-env presence.
func (m *Manager) exportSource(ctx context.Context, sb *types.Sandbox, exportDir string) (snap, vmName string, guestEnv bool, err error) {
	sb.Transition.Lock()
	if captureErr := m.captureAllowed(sb); captureErr != nil {
		sb.Transition.Unlock()
		return "", "", false, captureErr
	}
	m.trimForCapture(ctx, sb)
	snap, cleanup, err := m.sourceSnap(ctx, sb)
	if err != nil {
		sb.Transition.Unlock()
		return "", "", false, err
	}
	defer cleanup()
	vmName, guestEnv = sb.VMName, m.heldGuestEnv(sb)
	// a fresh snapshot is this capture's alone, so dials need not wait out its export; a wake consumes a hibernate snapshot
	if snap != sb.HibernateSnap {
		sb.Transition.Unlock()
	} else {
		defer sb.Transition.Unlock()
	}
	return snap, vmName, guestEnv, m.eng.SnapshotExport(ctx, snap, exportDir)
}

// trimForCapture trims a live sandbox's copy-on-write disk when its pool asks for it; a failed trim only leaves the capture larger.
func (m *Manager) trimForCapture(ctx context.Context, sb *types.Sandbox) {
	if !m.view.Load().poolTrims[sb.PolicyKey()] || sb.HibernateSnap != "" || sb.ArchiveCk != "" {
		return
	}
	if err := m.eng.TrimCow(ctx, sb.VsockSocket); err != nil {
		log.WithFunc("pool.trimForCapture").Warnf(ctx, "trim %s before its capture: %v", sb.ID, err)
	}
}
