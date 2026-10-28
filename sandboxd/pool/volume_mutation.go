package pool

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	volumeChangeFailed      = "volume change failed; retry the desired target"
	volumeChangeInterrupted = "volume change interrupted; retry the desired target"
)

type volumeEdit func([]types.Volume) ([]types.Volume, error)

type volumeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	holds  []types.Volume
}

// GetVolumes reads a claim's desired and applied volumes; a tenant reaches only its own claims.
func (m *Manager) GetVolumes(id, tenant string) (types.SandboxVolumesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	if sb == nil || !tenantOwns(tenant, sb.Tenant) {
		return types.SandboxVolumesResponse{}, ErrUnknownSandbox
	}
	return m.volumeView(sb), nil
}

// SetVolumes converges a running claim onto the full desired volume set.
func (m *Manager) SetVolumes(ctx context.Context, id, tenant string, want []types.Volume) (types.SandboxVolumesResponse, error) {
	return m.changeVolumes(ctx, id, tenant, func([]types.Volume) ([]types.Volume, error) { return slices.Clone(want), nil })
}

// PatchVolumes adds and removes a running claim's volumes by name.
func (m *Manager) PatchVolumes(ctx context.Context, id, tenant string, patch types.VolumePatch) (types.SandboxVolumesResponse, error) {
	return m.changeVolumes(ctx, id, tenant, func(current []types.Volume) ([]types.Volume, error) {
		if err := validateDetachNames(patch.Remove); err != nil {
			return nil, err
		}
		additions, err := normalizeLiveVolumes(patch.Add)
		if err != nil {
			return nil, err
		}
		for _, v := range additions {
			if slices.Contains(patch.Remove, v.Name) {
				return nil, fmt.Errorf("%w: volume %q is both added and removed", ErrBadVolume, v.Name)
			}
			if i := volumeIndex(current, v.Name); i >= 0 {
				if current[i] != v {
					return nil, fmt.Errorf("%w: volume %q already has a different mount or mode", ErrBadVolume, v.Name)
				}
			} else {
				current = append(current, v)
			}
		}
		return slices.DeleteFunc(current, func(v types.Volume) bool { return slices.Contains(patch.Remove, v.Name) }), nil
	})
}

func (m *Manager) changeVolumes(ctx context.Context, id, tenant string, change volumeEdit) (view types.SandboxVolumesResponse, err error) {
	sb, ok := m.byID(id)
	if !ok || !tenantOwns(tenant, sb.Tenant) {
		return view, ErrUnknownSandbox
	}
	var run *volumeRun
	if err = m.eng.CheckLiveVolumeSupport(ctx); err == nil {
		ctx, run, err = m.beginVolumeRun(ctx, sb)
	}
	if err != nil {
		view, _ = m.GetVolumes(id, tenant)
		return view, err
	}
	failedName := ""
	defer func() {
		m.recordVolumeError(sb, run, failedName, err)
		view = m.finishVolumeRun(sb, run)
	}()
	m.mu.Lock()
	current, held := slices.Clone(sb.VolumesWant), heldVolumes(sb)
	m.mu.Unlock()
	target, err := change(current)
	if err != nil {
		return view, err
	}
	target, err = normalizeLiveVolumes(target)
	if err != nil {
		return view, err
	}
	if err = validateLiveTarget(held, target); err != nil {
		return view, err
	}
	additions := volumeDifference(target, held)
	if len(additions) != 0 {
		if err = m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return view, err
		}
	}
	resolved, err := m.resolveVolumes(ctx, sb.Tenant, additions)
	if err != nil {
		return view, err
	}
	m.mu.Lock()
	err = m.checkVolumeRunLocked(ctx, sb, run)
	if err == nil && slices.ContainsFunc(additions, types.Volume.RW) {
		err = m.liveWriterErrorLocked(sb)
	}
	m.mu.Unlock()
	if err != nil {
		return view, err
	}
	sb.Touch()
	m.mu.Lock()
	uncertain := volumeDifference(volumeDifference(sb.VolumesWant, sb.Volumes), target)
	m.mu.Unlock()
	for _, v := range uncertain {
		failedName = v.Name
		if err = m.detachVolumeRun(ctx, sb, run, v); err != nil {
			return view, err
		}
	}
	failedName = ""
	if err = m.prepareVolumeTarget(ctx, sb, run, target, additions, resolved); err != nil {
		return view, err
	}
	m.mu.Lock()
	removals := volumeDifference(sb.Volumes, target)
	m.mu.Unlock()
	for _, v := range removals {
		failedName = v.Name
		if err = m.detachVolumeRun(ctx, sb, run, v); err != nil {
			return view, err
		}
		if err = m.commitVolumeApplied(ctx, sb, run, v, false); err != nil {
			return view, err
		}
		m.clearVolumeError(sb, run, v.Name)
	}
	m.mu.Lock()
	pending, catalog := volumeDifference(sb.VolumesWant, sb.Volumes), m.catalog.entries
	m.mu.Unlock()
	for _, v := range pending {
		failedName = v.Name
		if err = m.checkVolumeRun(ctx, sb, run); err != nil {
			return view, err
		}
		repair, resolveErr := m.resolveCatalogEntry(ctx, v, catalog[v.Name])
		if resolveErr != nil {
			return view, resolveErr
		}
		applyCtx, cancel := context.WithTimeout(ctx, engine.VolumeMutationTimeout)
		err = m.applyVolume(applyCtx, sb, repair, true)
		cancel()
		if err != nil {
			return view, err
		}
		if err = m.checkVolumeRun(ctx, sb, run); err != nil {
			return view, err
		}
		if err = m.commitVolumeApplied(ctx, sb, run, v, true); err != nil {
			return view, err
		}
		m.clearVolumeError(sb, run, v.Name)
	}
	return view, nil
}

func (m *Manager) beginVolumeRun(ctx context.Context, sb *types.Sandbox) (context.Context, *volumeRun, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ctx, nil, err
		}
		m.mu.Lock()
		if m.claimed[sb.ID] != sb {
			m.mu.Unlock()
			return ctx, nil, ErrUnknownSandbox
		}
		if active := m.volumeRuns[sb]; active != nil {
			done := active.done
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx, nil, ctx.Err()
			case <-done:
			}
			continue
		}
		runCtx, cancel := context.WithCancel(ctx)
		run := &volumeRun{cancel: cancel, done: make(chan struct{}), holds: heldVolumes(sb)}
		m.volumeRuns[sb] = run
		m.mu.Unlock()
		return runCtx, run, nil
	}
}

func (m *Manager) finishVolumeRun(sb *types.Sandbox, run *volumeRun) types.SandboxVolumesResponse {
	m.mu.Lock()
	if m.claimed[sb.ID] == sb {
		m.releaseVolumes(volumeDifference(run.holds, heldVolumes(sb)))
	}
	delete(m.volumeRuns, sb)
	view := m.volumeView(sb)
	close(run.done)
	m.mu.Unlock()
	run.cancel()
	return view
}

func (m *Manager) restoreVolumeHolds(sb *types.Sandbox, holds []types.Volume) {
	if m.volumeRuns[sb] == nil {
		m.releaseVolumes(volumeDifference(holds, heldVolumes(sb)))
	}
}

func (m *Manager) checkVolumeRun(ctx context.Context, sb *types.Sandbox, run *volumeRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkVolumeRunLocked(ctx, sb, run)
}

func (m *Manager) checkVolumeRunLocked(ctx context.Context, sb *types.Sandbox, run *volumeRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.ownsRunLocked(sb, run) {
		return ErrUnknownSandbox
	}
	switch {
	case sb.Failed != "":
		return ErrFailed
	case sb.ArchiveCk != "":
		return ErrArchived
	case sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.VsockSocket == "":
		return ErrPaused
	}
	return nil
}

func (m *Manager) cancelVolumeRun(sb *types.Sandbox) []types.Volume {
	delete(m.volumeErrors, sb)
	if run := m.volumeRuns[sb]; run != nil {
		run.cancel()
		return slices.Clone(run.holds)
	}
	return heldVolumes(sb)
}

func (m *Manager) prepareVolumeTarget(ctx context.Context, sb *types.Sandbox, run *volumeRun, target, additions []types.Volume, resolved []resolvedVolume) error {
	js, changed, err := m.stageVolumeTarget(ctx, sb, run, target, additions, resolved)
	if err != nil || !changed {
		return err
	}
	return m.commitVolumeRun(ctx, sb, run, js)
}

func (m *Manager) stageVolumeTarget(ctx context.Context, sb *types.Sandbox, run *volumeRun, target, additions []types.Volume, resolved []resolvedVolume) (claimSnapshot, bool, error) {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if err := m.checkVolumeRunLocked(ctx, sb, run); err != nil {
			return err
		}
		if sb.OnExpire == types.ExpireArchive && len(target) > 0 {
			return ErrVolumeCapture
		}
		if slices.ContainsFunc(additions, types.Volume.RW) {
			if err := m.liveWriterErrorLocked(sb); err != nil {
				return err
			}
		}
		if err := m.checkResolvedLocked(resolved); err != nil {
			return err
		}
		m.adoptVolumes(additions)
		run.holds = append(run.holds, additions...)
		return nil
	}(); err != nil {
		return claimSnapshot{}, false, err
	}
	cleanErr := m.confirmVolumesClean(resolved)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkVolumeRunLocked(ctx, sb, run); err != nil {
		return claimSnapshot{}, false, err
	}
	if cleanErr != nil {
		m.releaseVolumes(additions)
		run.holds = volumeDifference(run.holds, additions)
		return claimSnapshot{}, false, cleanErr
	}
	replaced := func(v types.Volume) bool {
		return volumeIndex(sb.VolumesWant, v.Name) < 0 && volumeIndex(target, v.Name) >= 0
	}
	if len(additions) == 0 && slices.Equal(target, sb.VolumesWant) && !slices.ContainsFunc(sb.Volumes, replaced) && m.store.synced() {
		return claimSnapshot{}, false, nil
	}
	sb.Volumes = slices.DeleteFunc(slices.Clone(sb.Volumes), replaced)
	sb.VolumesWant = slices.Clone(target)
	return m.store.set(sb), true, nil
}

func (m *Manager) liveWriterErrorLocked(sb *types.Sandbox) error {
	if m.view.Load().poolReplicate[sb.PolicyKey()] > 0 {
		return ErrReplicatingPool
	}
	if m.archives == nil && (sb.HasReplica() || sb.ArchivePending != "") {
		return ErrReplicaStranded
	}
	if sb.HasReplica() || sb.ArchivePending != "" || m.store.hasCheckpointReferences(sb.ID) {
		return ErrReplicaPresent
	}
	return nil
}

func (m *Manager) detachVolumeRun(ctx context.Context, sb *types.Sandbox, run *volumeRun, v types.Volume) error {
	m.mu.Lock()
	err := m.checkVolumeRunLocked(ctx, sb, run)
	entry := m.catalog.entries[v.Name]
	m.mu.Unlock()
	if err != nil {
		return err
	}
	cleanRW, err := m.eng.DetachVolume(ctx, sb.VMName, sb.VsockSocket, v.Name, v.Mount)
	if err != nil {
		return fmt.Errorf("detach volume %q: %w", v.Name, err)
	}
	if err = m.checkVolumeRun(ctx, sb, run); err != nil {
		return err
	}
	if needsUnmount(v) && cleanRW && entry != nil {
		if err = clearVolumeDirty(entry.disk.Path); err != nil {
			return fmt.Errorf("clear dirty marker for volume %q: %w", v.Name, err)
		}
	}
	return nil
}

func (m *Manager) commitVolumeApplied(ctx context.Context, sb *types.Sandbox, run *volumeRun, v types.Volume, attached bool) error {
	m.mu.Lock()
	if err := m.checkVolumeRunLocked(ctx, sb, run); err != nil {
		m.mu.Unlock()
		return err
	}
	sb.Volumes = slices.DeleteFunc(slices.Clone(sb.Volumes), func(a types.Volume) bool { return a.Name == v.Name })
	if attached {
		sb.Volumes = append(sb.Volumes, v)
	}
	js := m.store.set(sb)
	m.mu.Unlock()
	return m.commitVolumeRun(ctx, sb, run, js)
}

func (m *Manager) commitVolumeRun(ctx context.Context, sb *types.Sandbox, run *volumeRun, js claimSnapshot) error {
	if err := m.store.commit(js); err != nil {
		m.store.recommit(ctx, js)
		return fmt.Errorf("persist volumes: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ownsRunLocked(sb, run) {
		return ErrUnknownSandbox
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dropped := volumeDifference(run.holds, heldVolumes(sb))
	m.releaseVolumes(dropped)
	run.holds = volumeDifference(run.holds, dropped)
	return nil
}

func (m *Manager) ownsRunLocked(sb *types.Sandbox, run *volumeRun) bool {
	return m.claimed[sb.ID] == sb && m.volumeRuns[sb] == run
}

func (m *Manager) hasVolumeHolds(sb *types.Sandbox) bool {
	run := m.volumeRuns[sb]
	return hasVolumes(sb) || run != nil && len(run.holds) > 0
}

func (m *Manager) volumeView(sb *types.Sandbox) types.SandboxVolumesResponse {
	view := types.SandboxVolumesResponse{
		Volumes:     slices.Clone(sb.Volumes),
		VolumesWant: slices.Clone(sb.VolumesWant),
		States:      []types.VolumeState{},
		Converging:  m.volumeRuns[sb] != nil,
	}
	for _, v := range heldVolumes(sb) {
		state := types.VolumeAttached
		if volumeIndex(sb.VolumesWant, v.Name) < 0 {
			state = types.VolumeDetaching
		} else if volumeIndex(sb.Volumes, v.Name) < 0 {
			state = types.VolumePending
		}
		message := m.volumeErrors[sb][v.Name]
		if message == "" && state != types.VolumeAttached && !view.Converging {
			message = volumeChangeInterrupted
		}
		view.States = append(view.States, types.VolumeState{Name: v.Name, State: state, Error: message})
	}
	return view
}

func (m *Manager) recordVolumeError(sb *types.Sandbox, run *volumeRun, name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.ownsRunLocked(sb, run) {
		return
	}
	if err == nil {
		delete(m.volumeErrors, sb)
		return
	}
	errs := m.volumeErrors[sb]
	if name != "" {
		message := volumeChangeFailed
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			message = volumeChangeInterrupted
		}
		if conflict, ok := errors.AsType[*engine.VolumeMountConflict](err); ok {
			message = conflict.Error()
		}
		if errs == nil {
			errs = map[string]string{}
		}
		errs[name] = message
	}
	held := heldVolumes(sb)
	maps.DeleteFunc(errs, func(name, _ string) bool { return volumeIndex(held, name) < 0 })
	if len(errs) == 0 {
		delete(m.volumeErrors, sb)
		return
	}
	m.volumeErrors[sb] = errs
}

func (m *Manager) clearVolumeError(sb *types.Sandbox, run *volumeRun, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ownsRunLocked(sb, run) {
		delete(m.volumeErrors[sb], name)
	}
}

func normalizeLiveVolumes(volumes []types.Volume) ([]types.Volume, error) {
	out, err := types.ValidateVolumes(volumes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadVolume, err)
	}
	return out, nil
}

func validateLiveTarget(held, target []types.Volume) error {
	for _, v := range held {
		i := volumeIndex(target, v.Name)
		if i >= 0 && target[i] != v {
			return fmt.Errorf("%w: remove volume %q before changing its mount or mode", ErrBadVolume, v.Name)
		}
	}
	return nil
}

func volumeIndex(volumes []types.Volume, name string) int {
	return slices.IndexFunc(volumes, func(v types.Volume) bool { return v.Name == name })
}

func volumeDifference(left, right []types.Volume) []types.Volume {
	return slices.DeleteFunc(slices.Clone(left), func(v types.Volume) bool { return volumeIndex(right, v.Name) >= 0 })
}

func validateDetachNames(names []string) error {
	if len(names) > types.MaxClaimVolumes {
		return fmt.Errorf("%w: too many volume names", ErrBadVolume)
	}
	for i, name := range names {
		if !types.ValidVolumeName(name) || slices.Contains(names[:i], name) {
			return fmt.Errorf("%w: invalid or duplicate volume name %q", ErrBadVolume, name)
		}
	}
	return nil
}

func heldVolumes(sb *types.Sandbox) []types.Volume {
	return slices.Concat(sb.Volumes, volumeDifference(sb.VolumesWant, sb.Volumes))
}
