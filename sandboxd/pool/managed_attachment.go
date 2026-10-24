package pool

import "errors"

// admitManaged re-checks each managed volume after its name is reserved; a delete or grow that won the name first has already changed the record.
func (m *Manager) admitManaged(volumes []resolvedVolume) error {
	r := m.managed
	for _, volume := range volumes {
		if volume.managedID == "" {
			continue
		}
		r.mu.Lock()
		v, ok := r.data.Volumes[volume.applied.Name]
		r.mu.Unlock()
		switch {
		case !ok || v.ID != volume.managedID:
			return ErrVolumeUnavailable
		case v.GrowTo != 0:
			return growInProgress(v.Name)
		case v.State != volumeReady:
			return managedStorageError()
		}
	}
	return nil
}

// managedBusy names the holders of a refused managed volume; any other refusal passes through.
func (m *Manager) managedBusy(err error, volumes []resolvedVolume) error {
	if !errors.Is(err, ErrVolumeBusy) {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, volume := range volumes {
		held := m.volumeAdmission[volume.applied.Name]
		if volume.managedID != "" && (held.writers > 0 || volume.applied.RW() && held.readers > 0) {
			return &VolumeError{Status: 409, Code: volumeBusyCode, Message: volumeBusyMessage, Volume: volume.applied.Name, Holders: m.volumeHoldersLocked(volume.applied.Name)}
		}
	}
	return err
}
