package pool

import (
	"fmt"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// validateManagedTarget refuses a live change that would detach a managed volume; managed volumes attach at claim time only.
func (m *Manager) validateManagedTarget(held, target []types.Volume) error {
	for _, volume := range held {
		if _, managed := m.managedPath(volume.Name); managed && volumeIndex(target, volume.Name) < 0 {
			return fmt.Errorf("%w: managed volumes cannot be removed live", ErrBadVolume)
		}
	}
	return nil
}
