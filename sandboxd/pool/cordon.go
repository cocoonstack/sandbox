package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const cordonFile = "cordoned"

// Cordon refuses new claims until Uncordon, across restarts, and destroys the warm VMs; live moves in stay admitted.
func (m *Manager) Cordon(ctx context.Context) error {
	m.cordonMu.Lock()
	defer m.cordonMu.Unlock()
	if err := utils.WriteFileSync(m.cordonPath(), nil, 0o600); err != nil {
		return fmt.Errorf("persist cordon: %w", err)
	}
	m.mu.Lock()
	m.cordoned = true
	trim := m.trimAllWarmLocked()
	m.mu.Unlock()
	m.recordAudit(ctx, "", auditFrame{Op: "cordon"})
	m.destroyAll(context.WithoutCancel(ctx), trim).Wait()
	return nil
}

// Uncordon lifts a cordon and kicks an immediate refill.
func (m *Manager) Uncordon(ctx context.Context) error {
	m.cordonMu.Lock()
	defer m.cordonMu.Unlock()
	if err := os.Remove(m.cordonPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove cordon: %w", err)
	}
	if err := utils.SyncDir(m.dataDir); err != nil {
		return fmt.Errorf("sync cordon removal: %w", err)
	}
	m.mu.Lock()
	m.cordoned = false
	m.mu.Unlock()
	m.recordAudit(ctx, "", auditFrame{Op: "uncordon"})
	m.refillOnce(context.WithoutCancel(ctx))
	return nil
}

func (m *Manager) loadCordon() error {
	_, err := os.Stat(m.cordonPath())
	switch {
	case err == nil:
		m.cordoned = true
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("read cordon: %w", err)
	}
	return nil
}

func (m *Manager) cordonPath() string {
	return filepath.Join(m.dataDir, cordonFile)
}
