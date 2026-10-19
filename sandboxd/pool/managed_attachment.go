package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func readProcessIdentity(pid int) (processIdentity, error) {
	return readProcessIdentityAt("/proc", pid)
}

func readProcessIdentityAt(procDir string, pid int) (processIdentity, error) {
	if pid <= 0 {
		return processIdentity{}, errors.New("VMM PID unavailable")
	}
	boot, err := os.ReadFile(procDir + "/sys/kernel/random/boot_id")
	if err != nil {
		// Missing boot metadata cannot prove that the PID is gone.
		return processIdentity{}, fmt.Errorf("read VMM boot identity: %v", err)
	}
	bootID := strings.TrimSpace(string(boot))
	if bootID == "" {
		return processIdentity{}, errors.New("empty VMM boot identity")
	}
	stat, err := os.ReadFile(procDir + "/" + strconv.Itoa(pid) + "/stat")
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
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return err == nil && current != p
}

func (m *Manager) admitManaged(tenant string, volumes []resolvedVolume) ([]types.Volume, error) {
	applied := appliedVolumes(volumes)
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
		if volume.managedID != "" {
			v, ok := r.data.Volumes[volume.applied.Name]
			if !ok || v.ID != volume.managedID || (tenant != "" && v.Tenant != tenant) {
				return nil, managedNotFound()
			}
			if v.State != volumeReady || r.verify(v) != nil {
				return nil, managedStorageError()
			}
			for _, other := range r.data.Holds {
				for _, ref := range other.Refs {
					if ref.ID == v.ID && (ref.Volume.RW() || volume.applied.RW()) {
						return nil, &VolumeError{Status: 409, Code: "volume_busy", Message: "volume is held by a sandbox", Volume: v.Name, Holders: r.holders(v.ID)}
					}
				}
			}
			h.Refs = append(h.Refs, volumeRef{Volume: volume.applied, ID: v.ID, Path: r.path(v.ID), Backing: v.Backing})
		} else {
			identity, err := catalogBacking(volume.disk.Path)
			if err != nil {
				return nil, ErrVolumeUnavailable
			}
			h.Refs = append(h.Refs, volumeRef{Volume: volume.applied, Path: volume.disk.Path, Backing: identity})
		}
	}
	if err := m.reserveVolumes(applied); err != nil {
		return nil, err
	}
	if err := m.confirmVolumesClean(volumes); err != nil {
		m.releaseVolumes(applied)
		return nil, err
	}
	r.data.Holds[h.SandboxID] = h
	for i := range volumes {
		volumes[i].holdID = h.SandboxID
	}
	if err := r.commit(); err != nil {
		return nil, err
	}
	return applied, nil
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
	if r.commit() == nil {
		m.releaseVolumes(reserved)
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
	return r.commit()
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
	h.State = holdClaimed
	r.data.Holds[id] = h
	return r.commit()
}

func (m *Manager) finishManaged(id string, clears []string) bool {
	r := m.managed
	m.mu.Lock()
	defer m.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.data.Holds[id]
	if !ok {
		return true
	}
	if r.fault != nil {
		return false
	}
	if err := m.store.commit(m.store.del(id)); err != nil {
		return false
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
	delete(r.data.Holds, id)
	if r.commit() != nil {
		r.data.Holds[id] = h
		return false
	}
	refs := make([]types.Volume, len(h.Refs))
	for i, ref := range h.Refs {
		refs[i] = ref.Volume
	}
	m.releaseVolumes(refs)
	return true
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
		rec, present := live[h.VMName]
		valid := exists && sb.VMName == h.VMName && sb.Tenant == h.Tenant && sb.Failed != egressQuarantineFailure && h.State != holdRetiring && present
		if valid {
			identity, err := r.process(rec.PID)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return owned, managedStorageError()
			}
			valid = err == nil && identity == h.Process
		}
		if valid {
			for _, volume := range heldVolumes(sb) {
				matched := false
				for _, ref := range h.Refs {
					if ref.Volume == volume {
						matched = true
						break
					}
				}
				if !matched {
					return owned, managedStorageError()
				}
			}
			owned[h.VMName] = true
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

func (m *Manager) managedLiveAdd(id string, volumes []resolvedVolume) error {
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
	if r.fault != nil || h.State != holdClaimed {
		return managedStorageError()
	}
	for _, v := range volumes {
		for _, ref := range h.Refs {
			if ref.Volume.Name == v.applied.Name {
				return volumeError(409, "volume_attachment_uncertain", "previous attachment requires sandbox release")
			}
		}
		identity, err := catalogBacking(v.disk.Path)
		if err != nil {
			return ErrVolumeUnavailable
		}
		h.Refs = append(h.Refs, volumeRef{Volume: v.applied, Path: v.disk.Path, Backing: identity})
	}
	r.data.Holds[id] = h
	return r.commit()
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
	if r.fault != nil || h.State != holdClaimed {
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
	return r.commit()
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
	return backingIdentity{Device: uint64(raw.Dev), Inode: raw.Ino, Size: st.Size()}, nil
}
