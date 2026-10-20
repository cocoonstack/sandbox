package pool

import (
	"context"
	"errors"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type volumeGrowEngine interface {
	InspectDisk(context.Context, engine.DiskRef) (engine.DiskInspection, error)
	GrowDisk(context.Context, engine.DiskGrowRequest) (engine.DiskGrowResult, error)
	ObserveVolume(context.Context, string, string, string) (engine.GuestVolumeObservation, error)
}

type managedGrowth struct {
	Request        types.VolumeGrow              `json:"request"`
	Runtime        engine.DiskGrowRequest        `json:"runtime"`
	SandboxID      string                        `json:"sandbox_id"`
	VMName         string                        `json:"vm_name"`
	Guest          engine.GuestVolumeObservation `json:"guest"`
	State          string                        `json:"state"`
	Outcome        string                        `json:"outcome"`
	GuestSizeBytes int64                         `json:"guest_size_bytes"`
}

func (g *managedGrowth) fenced() bool {
	return g != nil && g.State != "completed" && g.Outcome != "old_vmm_gone"
}
func growBusy() error {
	return volumeError(409, "volume_grow_busy", "volume growth owns the sandbox lifecycle")
}
func growRecovery() error {
	return volumeError(409, "volume_grow_recovery_required", "volume growth requires recovery; retain the operation ID and retry after recovery")
}
func (r *volumeRegistry) growthOwnsHold(id string) bool {
	for _, v := range r.data.Volumes {
		if v.Growth != nil && (v.Growth.fenced() || r.fault != nil) && v.Growth.SandboxID == id {
			return true
		}
	}
	return false
}
func (m *Manager) growthOwnsHold(id string) bool {
	if m.managed == nil {
		return false
	}
	m.managed.mu.Lock()
	defer m.managed.mu.Unlock()
	return m.managed.growthOwnsHold(id)
}
func (m *Manager) growthOwnsVM(name string) bool {
	if m.managed == nil {
		return false
	}
	m.managed.mu.Lock()
	defer m.managed.mu.Unlock()
	for _, v := range m.managed.data.Volumes {
		if v.Growth != nil && (v.Growth.fenced() || m.managed.fault != nil) && v.Growth.VMName == name {
			return true
		}
	}
	return false
}

func (r *volumeRegistry) growVolume(name, expectedID, tenant string, req types.VolumeGrow) (managedVolume, error) {
	if r.fault != nil {
		return managedVolume{}, managedStorageError()
	}
	v, ok := r.data.Volumes[name]
	if !ok || tenant != "" && tenant != v.Tenant {
		return managedVolume{}, managedNotFound()
	}
	if expectedID == "" {
		return v, volumeError(428, "volume_precondition_required", "If-Match must identify the volume")
	}
	if string(v.ID) != expectedID {
		return v, volumeError(412, "volume_precondition_failed", "volume identity does not match")
	}
	if v.Growth != nil && req.OperationID == v.Growth.Request.OperationID {
		if req != v.Growth.Request {
			return v, volumeError(409, "volume_grow_conflict", "operation ID has different parameters")
		}
		return v, nil
	}
	if req.ExpectedGeneration != v.CapacityGeneration {
		return v, volumeError(412, "volume_precondition_failed", "capacity generation does not match")
	}
	if v.Growth.fenced() {
		return v, growBusy()
	}
	if v.State != volumeReady {
		return v, growRecovery()
	}
	if req.SizeGiB<<30 <= v.Size {
		return v, volumeError(400, "invalid_volume_grow", "target must exceed committed capacity")
	}
	if v.CapacityGeneration == ^uint64(0) {
		return v, volumeError(409, "volume_generation_exhausted", "capacity generation is exhausted")
	}
	return v, nil
}

func (m *Manager) GrowVolume(ctx context.Context, name, expectedID string, req types.VolumeGrow, tenant string) (types.ManagedVolumeInfo, error) {
	if !types.ValidVolumeName(name) || req.SizeGiB < 1 || req.SizeGiB > 1024 || !engine.ValidGrowID(req.OperationID) {
		return types.ManagedVolumeInfo{}, volumeError(400, "invalid_volume_grow", "invalid volume name, size_gib (1 through 1024), or canonical operation UUID")
	}
	r := m.managed
	if r == nil {
		return types.ManagedVolumeInfo{}, managedNotFound()
	}
	r.mu.Lock()
	v, err := r.growVolume(name, expectedID, tenant, req)
	if err != nil {
		r.mu.Unlock()
		return types.ManagedVolumeInfo{}, err
	}
	if v.Growth != nil && v.Growth.Request == req && !v.Growth.fenced() {
		info := r.info(v)
		r.mu.Unlock()
		if v.Growth.State != "completed" {
			return info, growRecovery()
		}
		return info, nil
	}
	var owner string
	if v.Growth != nil && v.Growth.Request == req {
		owner = v.Growth.SandboxID
	} else {
		holders := r.holders(v.ID)
		if len(holders) == 1 {
			owner = holders[0].SandboxID
		}
	}
	r.mu.Unlock()
	ge, supported := m.eng.(volumeGrowEngine)
	if !supported {
		return types.ManagedVolumeInfo{}, volumeError(503, "volume_grow_unsupported", "runtime does not support managed volume growth")
	}
	m.mu.Lock()
	sb := m.claimed[owner]
	m.mu.Unlock()
	if sb == nil {
		return types.ManagedVolumeInfo{}, growRecovery()
	}
	sb.Transition.Lock()
	defer sb.Transition.Unlock()
	r.mu.Lock()
	v, err = r.growVolume(name, expectedID, tenant, req)
	r.mu.Unlock()
	if err != nil {
		return types.ManagedVolumeInfo{}, err
	}
	if v.Growth == nil || v.Growth.Request != req {
		if err := m.admitGrowth(ctx, sb, v, req, ge); err != nil {
			return types.ManagedVolumeInfo{}, err
		}
	}
	if err := m.driveGrowth(ctx, name, sb, ge); err != nil {
		return types.ManagedVolumeInfo{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v = r.data.Volumes[name]
	info := r.info(v)
	if v.Growth.State == "recovery_required" {
		return info, growRecovery()
	}
	return info, nil
}

func (m *Manager) growthHolderLocked(sb *types.Sandbox, v managedVolume) (volumeHold, error) {
	r := m.managed
	h, ok := r.data.Holds[sb.ID]
	_, outbound := m.view.Load().out.Resolve(sb, m.claimPooledLocked)
	_, removing := m.pendingRemovals[h.VMName]
	if !ok || m.claimed[sb.ID] != sb || h.State != holdClaimed || h.Sleep != nil || !sleepMatchesClaim(h, sb) || sb.Failed != "" || sb.HibernateSnap != "" || sb.PendingSnap != "" || m.volumeRuns[sb] != nil || outbound || removing || h.Refs[0].ID != v.ID || !h.Refs[0].Volume.RW() || h.Refs[0].Volume.Mount == "" || len(r.holders(v.ID)) != 1 || r.growthOwnsHold(sb.ID) {
		return h, growBusy()
	}
	if r.verify(v) != nil {
		return h, managedStorageError()
	}
	return h, nil
}

func (m *Manager) admitGrowth(ctx context.Context, sb *types.Sandbox, v managedVolume, req types.VolumeGrow, ge volumeGrowEngine) error {
	r := m.managed
	m.mu.Lock()
	r.mu.Lock()
	h, err := m.growthHolderLocked(sb, v)
	r.mu.Unlock()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	rec, present, err := m.eng.Inspect(ctx, h.VMName)
	if err != nil || !present || rec.ID == "" || rec.Config.Name != h.VMName || rec.State != vmStateRunning || len(rec.AttachedDevices.Disks) != 1 {
		return growRecovery()
	}
	disk := rec.AttachedDevices.Disks[0]
	if disk.ID == "" || disk.Name != v.Name || disk.Path != r.path(v.ID) || disk.ReadOnly {
		return growRecovery()
	}
	inspection, err := ge.InspectDisk(ctx, engine.DiskRef{VMID: rec.ID, DiskID: disk.ID})
	if err != nil || inspection.Error != nil || inspection.GuestBlockStatus != "unobserved" || inspection.Live.State != "observed" || inspection.Live.Binding == nil {
		return volumeError(503, "volume_grow_unsupported", "runtime cannot provide a qualified disk binding")
	}
	binding := *inspection.Live.Binding
	p, err := r.process(rec.PID)
	if err != nil || p != h.Process || binding.Validate() != nil || binding.Ref != inspection.Requested || binding.Ref.VMID != rec.ID || binding.Ref.DiskID != disk.ID || binding.VMM != (engine.DiskProcess{BootID: p.BootID, PID: p.PID, Start: p.Start}) || binding.Serial != v.Name || binding.Path != r.path(v.ID) || binding.Backing.Device != v.Backing.Device || binding.Backing.Inode != v.Backing.Inode || binding.ObservedBytes != v.Size || binding.Revision == ^uint64(0) {
		return growRecovery()
	}
	if inspection.LatestReceipt != nil && inspection.LatestReceipt.Outcome == "outcome_unknown" {
		return growRecovery()
	}
	guest, err := ge.ObserveVolume(ctx, rec.VsockSocket, v.Name, h.Refs[0].Volume.Mount)
	if err != nil || !guest.Valid() || guest.Serial != v.Name || guest.SizeBytes != v.Size {
		return growRecovery()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	current, err := r.growVolume(v.Name, string(v.ID), v.Tenant, req)
	if err != nil {
		return err
	}
	if current.Growth != nil && current.Growth.Request == req {
		return nil
	}
	if _, err := m.growthHolderLocked(sb, current); err != nil {
		return err
	}
	if current.Size != v.Size || current.Backing != v.Backing {
		return growRecovery()
	}
	if r.data.Version != 3 {
		r.data.Version = 3
		if err := r.commit(); err != nil {
			return err
		}
	}
	current.State = volumeGrowing
	current.Growth = &managedGrowth{Request: req, Runtime: engine.DiskGrowRequest{Expected: binding, ID: req.OperationID, TargetBytes: req.SizeGiB << 30}, SandboxID: sb.ID, VMName: h.VMName, Guest: guest, State: "pending", Outcome: "unobserved"}
	r.data.Volumes[v.Name] = current
	return r.commit()
}

func (m *Manager) driveGrowth(ctx context.Context, name string, sb *types.Sandbox, ge volumeGrowEngine) error {
	r := m.managed
	r.mu.Lock()
	v := r.data.Volumes[name]
	g := *v.Growth
	r.mu.Unlock()
	if !g.fenced() {
		return nil
	}
	inspection, inspectErr := ge.InspectDisk(ctx, g.Runtime.Expected.Ref)
	if inspection.Requested != g.Runtime.Expected.Ref || inspection.GuestBlockStatus != "unobserved" {
		return nil
	}
	receipt, live := inspection.LatestReceipt, inspection.Live
	if receipt == nil || !receipt.Request.Equal(g.Runtime) {
		// Only the original source binding permits a first dispatch. A known receipt is query-only.
		if sb == nil || g.Outcome != "unobserved" || g.State != "pending" || inspectErr != nil || inspection.Error != nil || live.State != "observed" || live.Binding == nil || !live.Binding.Equal(g.Runtime.Expected) {
			return m.saveGrowth(v, g, "recovery_required", g.Outcome, 0)
		}
		if receipt != nil && receipt.Revision > g.Runtime.Expected.Revision {
			return m.saveGrowth(v, g, "recovery_required", g.Outcome, 0)
		}
		result, _ := ge.GrowDisk(ctx, g.Runtime)
		receipt, live = result.Receipt, result.Live
		if receipt == nil {
			return nil
		}
	}
	if !receipt.Request.Equal(g.Runtime) || receipt.Revision != g.Runtime.Expected.Revision+1 {
		return m.saveGrowth(v, g, "recovery_required", g.Outcome, 0)
	}
	switch receipt.Outcome {
	case "outcome_unknown":
		return m.saveGrowth(v, g, "recovery_required", "outcome_unknown", 0)
	case "old_vmm_gone":
		p := g.Runtime.Expected.VMM
		if !r.processGone(processIdentity{BootID: p.BootID, PID: p.PID, Start: p.Start}) {
			return m.saveGrowth(v, g, "recovery_required", g.Outcome, 0)
		}
		return m.saveGrowth(v, g, "recovery_required", "old_vmm_gone", 0)
	case "acknowledged":
		if live.State != "observed" || live.Binding == nil || live.Binding.Validate() != nil || !live.Binding.SameIdentity(g.Runtime.Expected) || live.Binding.ObservedBytes != g.Runtime.TargetBytes || live.Binding.Revision != receipt.Revision {
			return m.saveGrowth(v, g, "recovery_required", "acknowledged", 0)
		}
	default:
		return m.saveGrowth(v, g, "recovery_required", g.Outcome, 0)
	}
	if err := m.saveGrowth(v, g, "pending", "acknowledged", 0); err != nil {
		return err
	}
	if sb == nil {
		return nil
	}
	guest, err := ge.ObserveVolume(ctx, sb.VsockSocket, v.Name, g.Guest.Mount)
	if err != nil {
		return nil
	}
	if !guest.Valid() || !guest.SameIdentity(g.Guest) {
		return m.saveGrowth(v, g, "recovery_required", "acknowledged", 0)
	}
	if guest.SizeBytes == g.Guest.SizeBytes {
		return nil
	}
	if guest.SizeBytes != g.Runtime.TargetBytes {
		return m.saveGrowth(v, g, "recovery_required", "acknowledged", guest.SizeBytes)
	}
	closing, closeErr := ge.InspectDisk(ctx, g.Runtime.Expected.Ref)
	if closeErr != nil || closing.Error != nil || closing.Requested != g.Runtime.Expected.Ref || closing.LatestReceipt == nil || !closing.LatestReceipt.Request.Equal(g.Runtime) || closing.LatestReceipt.Revision != receipt.Revision || closing.LatestReceipt.Outcome != "acknowledged" || closing.Live.State != "observed" || closing.Live.Binding == nil || !closing.Live.Binding.Equal(*live.Binding) {
		return m.saveGrowth(v, g, "recovery_required", "acknowledged", guest.SizeBytes)
	}
	p, processErr := r.process(g.Runtime.Expected.VMM.PID)
	if processErr != nil || p != (processIdentity{BootID: g.Runtime.Expected.VMM.BootID, PID: g.Runtime.Expected.VMM.PID, Start: g.Runtime.Expected.VMM.Start}) {
		return m.saveGrowth(v, g, "recovery_required", "acknowledged", guest.SizeBytes)
	}
	return m.saveGrowth(v, g, "completed", "acknowledged", guest.SizeBytes)
}

func (m *Manager) saveGrowth(v managedVolume, g managedGrowth, state, outcome string, guestSize int64) error {
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.data.Volumes[v.Name]
	if r.fault != nil || current.ID != v.ID || current.Growth == nil || current.Growth.Request != g.Request {
		return managedStorageError()
	}
	g.State, g.Outcome, g.GuestSizeBytes = state, outcome, guestSize
	current.Growth = &g
	if state == "completed" {
		if !current.Growth.fenced() && current.CapacityGeneration != g.Request.ExpectedGeneration {
			return managedStorageError()
		}
		actual, err := inspectBacking(r.path(v.ID))
		if err != nil || actual.Device != v.Backing.Device || actual.Inode != v.Backing.Inode || actual.Size != g.Runtime.TargetBytes {
			return managedStorageError()
		}
		current.Size, current.Backing, current.State = actual.Size, actual, volumeReady
		current.CapacityGeneration++
		for id, h := range r.data.Holds {
			h.Refs = slices.Clone(h.Refs)
			for i := range h.Refs {
				if h.Refs[i].ID == v.ID {
					h.Refs[i].Backing = actual
				}
			}
			r.data.Holds[id] = h
		}
	} else if outcome == "old_vmm_gone" {
		current.State = volumeRecovery
	}
	r.data.Volumes[v.Name] = current
	return r.commit()
}

func (r *volumeRegistry) validateGrowth(v managedVolume) error {
	g := v.Growth
	if g == nil {
		if v.State == volumeGrowing {
			return errors.New("growing volume lacks intent")
		}
		return nil
	}
	b := g.Runtime.Expected
	if r.data.Version != 3 || !engine.ValidGrowID(g.Request.OperationID) || g.Runtime.ID != g.Request.OperationID || g.Request.SizeGiB < 1 || g.Request.SizeGiB > 1024 || g.Runtime.TargetBytes != g.Request.SizeGiB<<30 || b.Validate() != nil || b.Revision == ^uint64(0) || b.Path != r.path(v.ID) || b.Serial != v.Name || b.Backing.Inode != v.Backing.Inode || b.Backing.Device != v.Backing.Device || b.ObservedBytes < 1<<30 || b.ObservedBytes%(1<<30) != 0 || b.ObservedBytes >= g.Runtime.TargetBytes || g.Request.ExpectedGeneration == ^uint64(0) || !g.Guest.Valid() || g.Guest.Serial != v.Name || g.Guest.SizeBytes != b.ObservedBytes || g.SandboxID == "" || g.VMName == "" {
		return errors.New("invalid volume growth intent")
	}
	switch g.Outcome {
	case "unobserved", "outcome_unknown", "acknowledged", "old_vmm_gone":
	default:
		return errors.New("invalid growth outcome")
	}
	switch g.State {
	case "completed":
		if (v.State != volumeReady && v.State != volumeDeleting && v.State != volumeRecovery) || g.Outcome != "acknowledged" || g.GuestSizeBytes != g.Runtime.TargetBytes || v.Size != g.Runtime.TargetBytes || g.Request.ExpectedGeneration == ^uint64(0) || v.CapacityGeneration != g.Request.ExpectedGeneration+1 {
			return errors.New("inconsistent completed growth")
		}
	case "pending", "recovery_required":
		if g.State == "pending" && g.Outcome != "unobserved" && g.Outcome != "acknowledged" {
			return errors.New("pending growth has terminal runtime outcome")
		}
		if v.Size != b.ObservedBytes || v.CapacityGeneration != g.Request.ExpectedGeneration || g.Outcome == "old_vmm_gone" && ((v.State != volumeRecovery && v.State != volumeDeleting) || g.State != "recovery_required") || g.Outcome != "old_vmm_gone" && v.State != volumeGrowing {
			return errors.New("inconsistent unresolved growth")
		}
		if g.fenced() {
			h, ok := r.data.Holds[g.SandboxID]
			if !ok || h.VMName != g.VMName || h.State != holdClaimed || h.Sleep != nil || len(h.Refs) != 1 || h.Refs[0].ID != v.ID || !h.Refs[0].Volume.RW() || h.Refs[0].Volume.Mount == "" || h.Process != (processIdentity{BootID: b.VMM.BootID, PID: b.VMM.PID, Start: b.VMM.Start}) {
				return errors.New("growth lost owner")
			}
		}
	default:
		return errors.New("invalid growth state")
	}
	return nil
}

func (m *Manager) recoverGrowth(ctx context.Context, claims map[string]*types.Sandbox) error {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.fault != nil {
		r.mu.Unlock()
		return managedStorageError()
	}
	var pending []managedVolume
	for _, v := range r.data.Volumes {
		if v.Growth.fenced() {
			pending = append(pending, v)
		}
	}
	r.mu.Unlock()
	ge, ok := m.eng.(volumeGrowEngine)
	if !ok && len(pending) > 0 {
		return managedStorageError()
	}
	for _, v := range pending {
		sb := claims[v.Growth.SandboxID]
		h, held := m.managedHold(v.Growth.SandboxID)
		if sb != nil && (!held || !sleepMatchesClaim(h, sb) || sb.VMName != v.Growth.VMName || sb.Tenant != v.Tenant) {
			sb = nil
		}
		if err := m.driveGrowth(ctx, v.Name, sb, ge); err != nil {
			return err
		}
	}
	return nil
}
