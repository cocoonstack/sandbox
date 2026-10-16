package pool

import (
	"context"
	"fmt"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/engine"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type volumeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	holds  []types.Volume
}

func (m *Manager) GetVolumes(id string, cred Cred) (types.SandboxVolumesResponse, error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return types.SandboxVolumesResponse{}, ErrUnknownSandbox
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[id] != sb {
		return types.SandboxVolumesResponse{}, ErrUnknownSandbox
	}
	return volumeView(sb), nil
}

func (m *Manager) SetVolumes(ctx context.Context, id string, cred Cred, want []types.Volume) (types.SandboxVolumesResponse, error) {
	return m.changeVolumes(ctx, id, cred, func([]types.Volume) ([]types.Volume, error) { return slices.Clone(want), nil })
}

func (m *Manager) PatchVolumes(ctx context.Context, id string, cred Cred, patch types.VolumePatch) (types.SandboxVolumesResponse, error) {
	return m.changeVolumes(ctx, id, cred, func(current []types.Volume) ([]types.Volume, error) {
		if err := validateDetachNames(patch.Remove); err != nil {
			return nil, err
		}
		additions, err := normalizeLiveVolumes(patch.Add, current)
		if err != nil {
			return nil, err
		}
		for _, v := range additions {
			if volumeIndex(current, v.Name) < 0 && immutableVolume(v) {
				return nil, fmt.Errorf("%w: live additions require read-only mounts", ErrBadVolume)
			}
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

func (m *Manager) changeVolumes(ctx context.Context, id string, cred Cred, change func([]types.Volume) ([]types.Volume, error)) (view types.SandboxVolumesResponse, err error) {
	sb, ok := m.resolve(id, cred)
	if !ok {
		return view, ErrUnknownSandbox
	}
	ctx, run, err := m.beginVolumeRun(ctx, sb)
	if err != nil {
		view, _ = m.GetVolumes(id, cred)
		return view, err
	}
	failedName := ""
	defer func() {
		view = m.finishVolumeRun(sb, run)
		if err != nil {
			view.Error = err.Error()
			for i := range view.States {
				if view.States[i].Name == failedName {
					view.States[i].Error = err.Error()
				}
			}
		}
	}()
	m.mu.Lock()
	current, held := slices.Clone(sb.VolumesWant), heldVolumes(sb)
	m.mu.Unlock()
	target, err := change(current)
	if err != nil {
		return view, err
	}
	target, err = normalizeLiveVolumes(target, held)
	if err != nil {
		return view, err
	}
	if err = validateLiveTarget(held, target); err != nil {
		return view, err
	}
	additions := volumeDifference(target, held)
	mutable := slices.DeleteFunc(slices.Clone(target), immutableVolume)
	if len(mutable) != 0 {
		if err = m.tenantRemoved(ctx, sb.Tenant); err != nil {
			return view, err
		}
	}
	resolved, err := m.resolveVolumes(ctx, sb.Tenant, mutable)
	if err != nil {
		return view, err
	}
	if err = m.checkVolumeRun(ctx, sb, run); err != nil {
		return view, err
	}
	m.mu.Lock()
	uncertain := volumeDifference(volumeDifference(sb.VolumesWant, sb.Volumes), target)
	m.mu.Unlock()
	for _, v := range uncertain {
		failedName = v.Name
		if err = m.detachVolumeRun(ctx, sb, run, v); err != nil {
			return view, err
		}
	}
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
		m.mu.Lock()
		next := slices.DeleteFunc(slices.Clone(sb.Volumes), func(a types.Volume) bool { return a.Name == v.Name })
		m.mu.Unlock()
		if err = m.commitVolumeApplied(ctx, sb, run, next); err != nil {
			return view, err
		}
	}
	for _, v := range resolved {
		failedName = v.applied.Name
		if err = m.checkVolumeRun(ctx, sb, run); err != nil {
			return view, err
		}
		applyCtx, cancel := context.WithTimeout(ctx, engine.VolumeMutationTimeout)
		err = m.applyVolume(applyCtx, sb, v)
		cancel()
		if err != nil {
			return view, err
		}
		if err = m.checkVolumeRun(ctx, sb, run); err != nil {
			return view, err
		}
		m.mu.Lock()
		next := slices.Clone(sb.Volumes)
		if volumeIndex(next, v.applied.Name) < 0 {
			next = append(next, v.applied)
		}
		m.mu.Unlock()
		if err = m.commitVolumeApplied(ctx, sb, run, next); err != nil {
			return view, err
		}
	}
	sb.Touch()
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
		if m.volumeRuns == nil {
			m.volumeRuns = make(map[*types.Sandbox]*volumeRun)
		}
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
	view := volumeView(sb)
	delete(m.volumeRuns, sb)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb || m.volumeRuns[sb] != run {
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
	if run := m.volumeRuns[sb]; run != nil {
		run.cancel()
		return slices.Clone(run.holds)
	}
	return heldVolumes(sb)
}

func (m *Manager) prepareVolumeTarget(ctx context.Context, sb *types.Sandbox, run *volumeRun, target, additions []types.Volume, resolved []resolvedVolume) error {
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	if err := m.checkVolumeRun(ctx, sb, run); err != nil {
		return err
	}
	m.mu.Lock()
	switch {
	case sb.Failed != "":
		m.mu.Unlock()
		return ErrFailed
	case sb.ArchiveCk != "":
		m.mu.Unlock()
		return ErrArchived
	case sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.VsockSocket == "":
		m.mu.Unlock()
		return ErrPaused
	case sb.OnExpire == types.ExpireArchive && len(target) > 0:
		m.mu.Unlock()
		return ErrVolumeCapture
	}
	if err := m.reserveVolumes(additions); err != nil {
		m.mu.Unlock()
		return err
	}
	run.holds = append(run.holds, additions...)
	m.mu.Unlock()
	cleanErr := m.confirmVolumesClean(resolved)
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || ctx.Err() != nil {
		m.mu.Unlock()
		return context.Canceled
	}
	if cleanErr != nil {
		m.releaseVolumes(additions)
		run.holds = volumeDifference(run.holds, additions)
		m.mu.Unlock()
		return cleanErr
	}
	previous := sb.VolumesWant
	sb.VolumesWant = slices.Clone(target)
	js := m.store.set(sb)
	m.mu.Unlock()
	err := m.store.commit(js)
	if err != nil && m.store.sync {
		// A directory fsync may fail after rename. The new target and holds must
		// survive until a later successful commit resolves that uncertainty.
		return fmt.Errorf("persist volume target: %w", err)
	}
	if err != nil {
		m.mu.Lock()
		if m.claimed[sb.ID] == sb && m.volumeRuns[sb] == run {
			sb.VolumesWant = previous
			rb := m.store.set(sb)
			m.releaseVolumes(additions)
			run.holds = volumeDifference(run.holds, additions)
			m.mu.Unlock()
			m.recommit(ctx, rb)
		} else {
			m.mu.Unlock()
		}
		return fmt.Errorf("persist volume target: %w", err)
	}
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || m.volumeRuns[sb] != run || ctx.Err() != nil {
		m.mu.Unlock()
		return context.Canceled
	}
	dropped := volumeDifference(run.holds, heldVolumes(sb))
	m.releaseVolumes(dropped)
	run.holds = volumeDifference(run.holds, dropped)
	m.mu.Unlock()
	return nil
}

func (m *Manager) detachVolumeRun(ctx context.Context, sb *types.Sandbox, run *volumeRun, v types.Volume) error {
	if err := m.checkVolumeRun(ctx, sb, run); err != nil {
		return err
	}
	if err := m.eng.DetachVolume(ctx, sb.VMName, sb.VsockSocket, v.Name, v.Mount); err != nil {
		return fmt.Errorf("detach volume %q: %w", v.Name, err)
	}
	return m.checkVolumeRun(ctx, sb, run)
}

func (m *Manager) commitVolumeApplied(ctx context.Context, sb *types.Sandbox, run *volumeRun, next []types.Volume) error {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb || m.volumeRuns[sb] != run || ctx.Err() != nil {
		m.mu.Unlock()
		return context.Canceled
	}
	previous := sb.Volumes
	sb.Volumes = next
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		if m.claimed[sb.ID] == sb && m.volumeRuns[sb] == run {
			sb.Volumes = previous
			rb := m.store.set(sb)
			m.mu.Unlock()
			m.recommit(ctx, rb)
		} else {
			m.mu.Unlock()
		}
		return fmt.Errorf("persist volume result: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimed[sb.ID] != sb || m.volumeRuns[sb] != run || ctx.Err() != nil {
		return context.Canceled
	}
	dropped := volumeDifference(run.holds, heldVolumes(sb))
	m.releaseVolumes(dropped)
	run.holds = volumeDifference(run.holds, dropped)
	return nil
}

func normalizeLiveVolumes(volumes, held []types.Volume) ([]types.Volume, error) {
	if len(volumes) > types.MaxClaimVolumes {
		return nil, fmt.Errorf("%w: too many volumes", ErrBadVolume)
	}
	out := make([]types.Volume, 0, len(volumes))
	for _, v := range volumes {
		attachOnly := v.AttachOnly
		if i := volumeIndex(held, v.Name); i >= 0 && held[i].Mount == "" && v.Mount == "" {
			attachOnly = true
		}
		normalized, err := types.ValidateVolumes([]types.Volume{v}, attachOnly)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadVolume, err)
		}
		if volumeIndex(out, v.Name) >= 0 {
			return nil, fmt.Errorf("%w: duplicate volume %q", ErrBadVolume, v.Name)
		}
		if attachOnly {
			normalized[0].AttachOnly = false
		}
		out = append(out, normalized[0])
	}
	mounted := slices.DeleteFunc(slices.Clone(out), func(v types.Volume) bool { return v.Mount == "" })
	if _, err := types.ValidateVolumes(mounted, false); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadVolume, err)
	}
	return out, nil
}

func validateLiveTarget(held, target []types.Volume) error {
	for _, v := range held {
		i := volumeIndex(target, v.Name)
		if i >= 0 && target[i] != v {
			return fmt.Errorf("%w: remove volume %q before changing its mount or mode", ErrBadVolume, v.Name)
		}
		if immutableVolume(v) && i < 0 {
			return fmt.Errorf("%w: writable and attach-only volumes cannot change live", ErrBadVolume)
		}
	}
	if slices.ContainsFunc(volumeDifference(target, held), immutableVolume) {
		return fmt.Errorf("%w: live additions require read-only mounts", ErrBadVolume)
	}
	return nil
}

func immutableVolume(v types.Volume) bool { return v.RW() || v.AttachOnly || v.Mount == "" }
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

func (m *Manager) hasVolumeHolds(sb *types.Sandbox) bool {
	return hasVolumes(sb) || m.volumeRuns[sb] != nil && len(m.volumeRuns[sb].holds) > 0
}

func heldVolumes(sb *types.Sandbox) []types.Volume {
	return append(slices.Clone(sb.Volumes), volumeDifference(sb.VolumesWant, sb.Volumes)...)
}

func volumeView(sb *types.Sandbox) types.SandboxVolumesResponse {
	view := types.SandboxVolumesResponse{Volumes: slices.Clone(sb.Volumes), VolumesWant: slices.Clone(sb.VolumesWant), States: []types.VolumeState{}}
	for _, v := range heldVolumes(sb) {
		state := "attached"
		if volumeIndex(sb.VolumesWant, v.Name) < 0 {
			state = "detaching"
		} else if volumeIndex(sb.Volumes, v.Name) < 0 {
			state = "pending"
		}
		view.States = append(view.States, types.VolumeState{Name: v.Name, State: state})
	}
	return view
}
