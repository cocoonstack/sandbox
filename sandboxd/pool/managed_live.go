package pool

import (
	"fmt"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (m *Manager) managedVolumesHeld(id string) ([]types.Volume, bool) {
	h, ok := m.managedHold(id)
	if !ok {
		return nil, false
	}
	volumes := make([]types.Volume, len(h.Refs))
	for i, ref := range h.Refs {
		volumes[i] = ref.Volume
	}
	return volumes, true
}

func (m *Manager) validateManagedTarget(id string, target []types.Volume) error {
	h, ok := m.managedHold(id)
	if !ok {
		return nil
	}
	for _, ref := range h.Refs {
		if ref.ID != "" && volumeIndex(target, ref.Volume.Name) < 0 {
			return fmt.Errorf("%w: managed volumes cannot be removed live", ErrBadVolume)
		}
	}
	return nil
}
