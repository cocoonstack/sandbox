package pool

import (
	"errors"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type sleepPhase string

const (
	sleepPreparing sleepPhase = "preparing"
	sleepAborting  sleepPhase = "aborting"
	sleepCapturing sleepPhase = "capturing"
	sleepSleeping  sleepPhase = "sleeping"
	sleepWaking    sleepPhase = "waking"
	sleepResuming  sleepPhase = "resuming"
)

type managedSleep struct {
	Operation     string                        `json:"operation"`
	Phase         sleepPhase                    `json:"phase"`
	Snapshot      string                        `json:"snapshot"`
	SourceID      string                        `json:"source_id,omitempty"`
	Hook          config.ManagedVolumeHibernate `json:"hook"`
	VMID          string                        `json:"vm_id"`
	Generation    uint64                        `json:"generation"`
	BeforeRestore processIdentity               `json:"before_restore"`
	Restored      processIdentity               `json:"restored,omitzero"`
}

func validateManagedSleep(h volumeHold) error {
	s := h.Sleep
	if h.State == holdAttaching || h.VMName == "" || len(h.Refs) != 1 || h.Refs[0].ID == "" || !h.Refs[0].Volume.RW() || h.Refs[0].Volume.Mount == "" || s.Operation == "" || !strings.HasPrefix(s.Snapshot, hibernatePrefix) || s.VMID == "" || s.Generation == 0 || s.Generation > ^uint64(0)-2 || s.Hook.Validate() != nil || !validSleepProcess(s.BeforeRestore) {
		return errors.New("invalid managed sleep record")
	}
	switch s.Phase {
	case sleepPreparing, sleepAborting, sleepCapturing:
		if s.Restored != (processIdentity{}) || h.Process != s.BeforeRestore {
			return errors.New("invalid pre-restore identity")
		}
	case sleepSleeping:
		if s.Restored != (processIdentity{}) || h.Process != s.BeforeRestore {
			return errors.New("sleeping owner has restored process")
		}
		fallthrough
	case sleepWaking, sleepResuming:
		if s.SourceID == "" {
			return errors.New("managed sleep lacks snapshot identity")
		}
	default:
		return errors.New("invalid managed sleep phase")
	}
	if s.Restored != (processIdentity{}) && (s.Restored != h.Process || !validSleepProcess(s.Restored)) {
		return errors.New("invalid restored process identity")
	}
	if s.Phase == sleepResuming && s.Restored == (processIdentity{}) {
		return errors.New("resume lacks process identity")
	}
	return nil
}

func (r *volumeRegistry) commitManagedSleep(h volumeHold, next *managedSleep) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.data.Holds[h.SandboxID]
	if r.fault != nil || r.growthOwnsHold(h.SandboxID) || !ok || current.State != holdClaimed || (current.Sleep != nil && (h.Sleep == nil || current.Sleep.Operation != h.Sleep.Operation)) {
		return managedStorageError()
	}
	if r.data.Version == 1 {
		r.data.Version = 2
		if err := r.commit(); err != nil {
			return err
		}
	}
	h.Sleep = next
	if next != nil {
		if err := validateManagedSleep(h); err != nil {
			return err
		}
	}
	r.data.Holds[h.SandboxID] = h
	if err := r.commit(); err != nil {
		if next == nil {
			h.Sleep = current.Sleep
			r.data.Holds[h.SandboxID] = h
		}
		return err
	}
	return nil
}

func sleepMatchesClaim(h volumeHold, sb *types.Sandbox) bool {
	return sb != nil && sb.ID == h.SandboxID && sb.VMName == h.VMName && sb.Tenant == h.Tenant && len(h.Refs) == 1 && slices.Equal(sb.Volumes, []types.Volume{h.Refs[0].Volume}) && slices.Equal(sb.VolumesWant, sb.Volumes) && sb.ArchiveCk == "" && sb.ArchivePending == "" && sb.ReplicaCk == "" && sb.ReplicaPending == "" && !sb.Replicated() && sb.Key.Net == types.NetNone
}

func (m *Manager) verifySleepBacking(h volumeHold) error {
	r := m.managed
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fault != nil {
		return managedStorageError()
	}
	ref := h.Refs[0]
	v, ok := r.data.Volumes[ref.Volume.Name]
	if !ok || v.ID != ref.ID || v.State != volumeReady || ref.Path != r.path(v.ID) || ref.Backing != v.Backing || (h.Tenant != "" && v.Tenant != h.Tenant) || r.verify(v) != nil {
		return managedStorageError()
	}
	return nil
}

func (m *Manager) managedLifecycle(sb *types.Sandbox) bool {
	h, ok := m.managedHold(sb.ID)
	return ok && (h.Sleep != nil || (m.managedSleepHook != nil && len(h.Refs) == 1 && h.Refs[0].ID != "" && h.Refs[0].Volume.RW() && h.Refs[0].Volume.Mount != ""))
}

func validSleepProcess(p processIdentity) bool { return p.PID > 0 && p.Start > 0 && p.BootID != "" }

func (m *Manager) managedPaused(sb *types.Sandbox) bool {
	h, held := m.managedHold(sb.ID)
	return held && (h.Sleep != nil || h.State == holdRetiring)
}

func (r *volumeRegistry) sleepOwnsVolume(id volumeID) bool {
	for _, h := range r.data.Holds {
		if h.Sleep != nil && h.Refs[0].ID == id {
			return true
		}
	}
	return false
}

func projectSleepState(sb *types.Sandbox, s *managedSleep) {
	sb.ReaderSafe = false
	switch s.Phase {
	case sleepPreparing, sleepAborting, sleepCapturing:
		sb.PendingSnap, sb.HibernateSnap = s.Snapshot, ""
	default:
		sb.HibernateSnap, sb.PendingSnap = s.Snapshot, ""
	}
}
