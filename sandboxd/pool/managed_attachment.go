package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var errProcessMetadata = errors.New("process metadata unavailable")

func readProcessIdentity(pid int) (processIdentity, error) {
	return readProcessIdentityAt("/proc", pid)
}

func readProcessIdentityAt(procDir string, pid int) (processIdentity, error) {
	if pid <= 0 {
		return processIdentity{}, errors.New("VMM PID unavailable")
	}
	proc := os.DirFS(procDir)
	boot, err := fs.ReadFile(proc, "sys/kernel/random/boot_id")
	if err != nil {
		// Missing boot metadata cannot prove that the PID is gone.
		return processIdentity{}, fmt.Errorf("%w: read VMM boot identity: %w", errProcessMetadata, err)
	}
	bootID := strings.TrimSpace(string(boot))
	if bootID == "" {
		return processIdentity{}, errors.New("empty VMM boot identity")
	}
	stat, err := fs.ReadFile(proc, strconv.Itoa(pid)+"/stat")
	if err != nil {
		return processIdentity{}, err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return processIdentity{}, errors.New("invalid process identity")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		return processIdentity{}, errors.New("invalid process identity")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return processIdentity{}, err
	}
	return processIdentity{BootID: bootID, PID: pid, Start: ticks}, nil
}

func (r *volumeRegistry) processGone(p processIdentity) bool {
	if p.PID <= 0 || p.Start == 0 || p.BootID == "" {
		return false
	}
	current, err := r.process(p.PID)
	if processMissing(err) {
		return true
	}
	return err == nil && current != p
}

func (m *Manager) admitManaged(tenant string, volumes []resolvedVolume) ([]types.Volume, error) {
	applied := appliedVolumes(volumes)
	for i := range volumes {
		var err error
		if volumes[i].managedID == "" {
			volumes[i].backing, err = catalogBacking(volumes[i].disk.Path)
		} else {
			volumes[i].backing, err = inspectBacking(volumes[i].disk.Path)
			if err == nil {
				err = m.managed.verifyDirectories()
			}
		}
		if err != nil {
			return nil, managedStorageError()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.quotaErr(1, tenant); err != nil {
		return nil, err
	}
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fault != nil {
		return nil, managedStorageError()
	}
	h := volumeHold{SandboxID: "sb_" + randHex(8), Tenant: tenant, State: holdAttaching}
	for _, volume := range volumes {
		ref, err := r.referenceForAdmission(tenant, volume)
		if err != nil {
			return nil, err
		}
		h.Refs = append(h.Refs, ref)
	}
	if err := m.reserveVolumes(applied); err != nil {
		return nil, err
	}
	r.data.Holds[h.SandboxID] = h
	for i := range volumes {
		volumes[i].holdID = h.SandboxID
	}
	r.mu.Unlock()
	m.mu.Unlock()
	cleanErr := m.confirmVolumesClean(volumes)
	r.mu.Lock()
	if cleanErr != nil {
		delete(r.data.Holds, h.SandboxID)
	}
	err := r.commitAndRelock()
	r.mu.Unlock()
	m.mu.Lock()
	r.mu.Lock()
	if cleanErr != nil && err == nil {
		m.releaseVolumes(applied)
		return nil, cleanErr
	}
	if err != nil {
		h.State = holdRetiring
		r.data.Holds[h.SandboxID] = h
		return nil, err
	}
	if current, ok := r.data.Holds[h.SandboxID]; !ok || current.State != holdAttaching || current.VMName != "" {
		return nil, managedStorageError()
	}
	return applied, nil
}

func (r *volumeRegistry) referenceForAdmission(tenant string, volume resolvedVolume) (volumeRef, error) {
	if volume.managedID == "" {
		return volumeRef{Volume: volume.applied, Path: volume.disk.Path, Backing: volume.backing}, nil
	}
	v, ok := r.data.Volumes[volume.applied.Name]
	if !ok || v.ID != volume.managedID || (tenant != "" && v.Tenant != tenant) {
		return volumeRef{}, managedNotFound()
	}
	if v.State != volumeReady || v.Backing != volume.backing {
		return volumeRef{}, managedStorageError()
	}
	for _, other := range r.data.Holds {
		for _, ref := range other.Refs {
			if ref.ID == v.ID && (ref.Volume.RW() || volume.applied.RW()) {
				return volumeRef{}, &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: v.Name, Holders: r.holders(v.ID)}
			}
		}
	}
	return volumeRef{Volume: volume.applied, ID: v.ID, Path: r.path(v.ID), Backing: v.Backing}, nil
}

func (m *Manager) releaseUnbound(volumes []resolvedVolume, reserved []types.Volume) {
	if len(reserved) == 0 {
		return
	}
	if len(volumes) == 0 || volumes[0].holdID == "" {
		m.unreserveVolumes(reserved)
		return
	}
	r := m.managed
	m.mu.Lock()
	defer m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	id := volumes[0].holdID
	h, ok := r.data.Holds[id]
	if !ok || h.VMName != "" || r.fault != nil {
		return
	}
	delete(r.data.Holds, id)
	m.mu.Unlock()
	err := r.commitAndRelock()
	r.mu.Unlock()
	m.mu.Lock()
	r.mu.Lock()
	if err == nil {
		m.releaseVolumes(reserved)
	} else {
		h.State = holdRetiring
		r.data.Holds[id] = h
	}
}

func (m *Manager) bindManaged(ctx context.Context, sb *types.Sandbox, volumes []resolvedVolume) error {
	if len(volumes) == 0 || volumes[0].holdID == "" {
		return nil
	}
	id := volumes[0].holdID
	sb.ID = id
	rec, present, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil || !present {
		return managedStorageError()
	}
	p, err := m.managed.process(rec.PID)
	if err != nil {
		return managedStorageError()
	}
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fault != nil {
		return managedStorageError()
	}
	h, ok := r.data.Holds[id]
	if !ok || h.State != holdAttaching || h.VMName != "" {
		return managedStorageError()
	}
	h.VMName = sb.VMName
	h.Process = p
	r.data.Holds[id] = h
	return r.commitAndRelock()
}

func (m *Manager) activateManaged(id string) error {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	if !ok {
		return nil
	}
	if r.fault != nil || h.State == holdRetiring {
		return managedStorageError()
	}
	if h.State == holdClaimed {
		return nil
	}
	h.State = holdClaimed
	r.data.Holds[id] = h
	return r.commitAndRelock()
}

func (m *Manager) finishManaged(id string, clears []string, charges ...[]types.Volume) bool {
	r := m.managed
	r.fileMu.Lock()
	defer r.fileMu.Unlock()
	r.mu.Lock()
	h, ok := r.data.Holds[id]
	blocked := r.closed || r.fault != nil || r.writeFault != nil || r.growthOwnsHold(id) || r.discharging[id]
	r.mu.Unlock()
	if !ok {
		return !blocked
	}
	if blocked {
		return false
	}
	if m.store.contains(id) {
		commit := m.store.commit
		if h.Sleep != nil {
			commit = m.store.commitDurable
		}
		if err := commit(m.store.del(id)); err != nil {
			return false
		}
	}
	for _, path := range clears {
		for _, ref := range h.Refs {
			if ref.Path == path {
				if identity, err := catalogBacking(path); err == nil && identity == ref.Backing {
					_ = clearVolumeDirty(path)
				}
				break
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.data.Holds[id]; !ok {
		return true
	}
	if r.discharging == nil {
		r.discharging = map[string]bool{}
	}
	r.discharging[id] = true
	delete(r.data.Holds, id)
	if r.commitAndRelock() != nil {
		r.data.Holds[id] = h
		delete(r.discharging, id)
		return false
	}
	r.mu.Unlock()
	m.mu.Lock()
	volumes := make([]types.Volume, 0, len(h.Refs))
	for _, ref := range h.Refs {
		volumes = append(volumes, ref.Volume)
	}
	for _, charge := range charges {
		for _, volume := range charge {
			if volumeIndex(volumes, volume.Name) < 0 {
				volumes = append(volumes, volume)
			}
		}
	}
	m.releaseVolumes(volumes)
	m.mu.Unlock()
	r.mu.Lock()
	delete(r.discharging, id)
	return true
}

func (m *Manager) restoreManagedClaim(previous volumeHold, err error) {
	if previous.SandboxID == "" {
		return
	}
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, published := errors.AsType[*claimPublishError](err); published {
		r.fault = err
		return
	}
	h, ok := r.data.Holds[previous.SandboxID]
	if ok && h.State == holdRetiring {
		h.State = previous.State
		r.data.Holds[h.SandboxID] = h
		_ = r.commitAndRelock()
	}
}

func (m *Manager) recoverManaged(ctx context.Context, claims map[string]*types.Sandbox, live map[string]types.VMRecord) (map[string]bool, error) {
	owned := map[string]bool{}
	r := m.managed
	if r == nil {
		return owned, nil
	}
	r.mu.Lock()
	if r.fault != nil {
		r.mu.Unlock()
		return owned, managedStorageError()
	}
	holds := make([]volumeHold, 0, len(r.data.Holds))
	for _, h := range r.data.Holds {
		holds = append(holds, h)
	}
	r.mu.Unlock()
	for _, h := range holds {
		m.mu.Lock()
		for _, ref := range h.Refs {
			m.adoptVolumes([]types.Volume{ref.Volume})
		}
		m.mu.Unlock()
		sb, exists := claims[h.SandboxID]
		if m.growthOwnsHold(h.SandboxID) {
			owned[h.VMName] = true
			r.mu.Lock()
			g := r.data.Volumes[h.Refs[0].Volume.Name].Growth
			r.mu.Unlock()
			if !exists || !sleepMatchesClaim(h, sb) || g == nil || !m.growthRecordMatches(*g, sb, live[h.VMName]) {
				return owned, managedStorageError()
			}
			continue
		}
		if h.Sleep != nil && (h.State != holdRetiring || exists && sleepMatchesClaim(h, sb)) {
			if !exists || !sleepMatchesClaim(h, sb) {
				return owned, managedStorageError()
			}
			if h.State == holdRetiring {
				r.mu.Lock()
				h.State = holdClaimed
				r.data.Holds[h.SandboxID] = h
				err := r.commitAndRelock()
				r.mu.Unlock()
				if err != nil {
					return owned, err
				}
			}
			projectSleepState(sb, h.Sleep)
			owned[h.VMName] = true
			continue
		}
		valid, err := r.recoverableManagedClaim(h, sb, live)
		if err != nil {
			return owned, err
		}
		if valid {
			owned[h.VMName] = true
			if h.State == holdRetiring {
				r.mu.Lock()
				h.State = holdAttaching
				r.data.Holds[h.SandboxID] = h
				r.mu.Unlock()
			}
			if err := m.activateManaged(h.SandboxID); err != nil {
				return owned, err
			}
			continue
		}
		delete(claims, h.SandboxID)
		if err := m.retireManaged(h.SandboxID); err != nil {
			return owned, err
		}
		if h.VMName == "" {
			if !m.finishManaged(h.SandboxID, nil) {
				return owned, managedStorageError()
			}
			continue
		}
		owned[h.VMName] = true
		m.removeOrRetry(ctx, h.VMName, h.SandboxID, volumeTeardown{managedID: h.SandboxID})
	}
	for _, sb := range claims {
		for _, v := range heldVolumes(sb) {
			r.mu.Lock()
			_, managed := r.data.Volumes[v.Name]
			_, held := r.data.Holds[sb.ID]
			r.mu.Unlock()
			if managed && !held {
				return owned, managedStorageError()
			}
		}
	}
	return owned, nil
}

func (r *volumeRegistry) recoverableManagedClaim(h volumeHold, sb *types.Sandbox, live map[string]types.VMRecord) (bool, error) {
	rec, present := live[h.VMName]
	if sb == nil || sb.VMName != h.VMName || sb.Tenant != h.Tenant || sb.Failed == egressQuarantineFailure || !present {
		return false, nil
	}
	if rec.PID == 0 && rec.State == vmStateStopped {
		if r.processGone(h.Process) {
			return false, nil
		}
		return false, managedStorageError()
	}
	identity, err := r.process(rec.PID)
	if err != nil && !processMissing(err) {
		return false, managedStorageError()
	}
	if err != nil || identity != h.Process {
		return false, nil
	}
	for _, volume := range heldVolumes(sb) {
		if !slices.ContainsFunc(h.Refs, func(ref volumeRef) bool { return ref.Volume == volume }) {
			return false, managedStorageError()
		}
	}
	return true, nil
}

func (m *Manager) stageManagedLiveAdd(id string, volumes []resolvedVolume) error {
	if len(volumes) == 0 {
		return nil
	}
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	if !ok {
		return nil
	}
	if r.fault != nil || h.State != holdClaimed || r.growthOwnsHold(id) {
		return managedStorageError()
	}
	for _, v := range volumes {
		for _, ref := range h.Refs {
			if ref.Volume.Name == v.applied.Name {
				return volumeError(409, "volume_attachment_uncertain", "previous attachment requires sandbox release")
			}
		}
		h.Refs = append(h.Refs, volumeRef{Volume: v.applied, Path: v.disk.Path, Backing: v.backing})
	}
	r.data.Holds[id] = h
	return nil
}

func (m *Manager) managedLiveRemove(id string, volumes []types.Volume) error {
	if len(volumes) == 0 {
		return nil
	}
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	if !ok {
		return nil
	}
	if r.fault != nil || h.State != holdClaimed || r.growthOwnsHold(id) {
		return managedStorageError()
	}
	remove := map[string]bool{}
	for _, v := range volumes {
		remove[v.Name] = true
	}
	refs := make([]volumeRef, 0, len(h.Refs))
	for _, ref := range h.Refs {
		if !remove[ref.Volume.Name] {
			refs = append(refs, ref)
		}
	}
	h.Refs = refs
	r.data.Holds[id] = h
	return r.commitAndRelock()
}

func catalogBacking(path string) (backingIdentity, error) {
	st, err := os.Stat(path)
	if err != nil {
		return backingIdentity{}, err
	}
	if !st.Mode().IsRegular() {
		return backingIdentity{}, errors.New("catalog backing is not a regular file")
	}
	raw := st.Sys().(*syscall.Stat_t)
	return backingIdentity{Device: backingDeviceID(raw.Dev), Inode: raw.Ino, Size: st.Size()}, nil
}

func (m *Manager) commitManaged() error {
	if m.managed == nil {
		return nil
	}
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitAndRelock()
}

func processMissing(err error) bool {
	return errors.Is(err, os.ErrNotExist) && !errors.Is(err, errProcessMetadata)
}
