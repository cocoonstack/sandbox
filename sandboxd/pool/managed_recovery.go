package pool

import (
	"context"
	"errors"
	"maps"
	"os"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func (m *Manager) recoverManagedOrphans(ctx context.Context, claims map[string]*types.Sandbox, live map[string]types.VMRecord, owned map[string]bool) error {
	r := m.managed
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if len(r.data.Volumes) == 0 && len(r.data.Orphans) == 0 {
		r.mu.Unlock()
		return nil
	}
	orphans := maps.Clone(r.data.Orphans)
	if orphans == nil {
		orphans = map[string]processIdentity{}
	}
	r.mu.Unlock()
	known := maps.Clone(owned)
	for _, sb := range claims {
		known[sb.VMName] = true
	}
	for name, rec := range live {
		if !strings.HasPrefix(name, vmPrefix) || known[name] {
			continue
		}
		if rec.State == vmStateCreating {
			outcome, err := m.eng.ReconcileStaleCreate(context.WithoutCancel(ctx), name)
			if err != nil {
				return managedStorageError()
			}
			switch outcome {
			case engine.StaleCreateCollected, engine.StaleCreateNotFound:
				delete(live, name)
				continue
			default:
				return managedStorageError()
			}
		}
		if _, tracked := orphans[name]; tracked {
			continue
		}
		identity, err := r.process(rec.PID)
		if (rec.PID > 0 && errors.Is(err, os.ErrNotExist)) || (rec.PID == 0 && rec.State == "stopped") {
			_ = m.eng.Remove(context.WithoutCancel(ctx), name)
			if !m.confirmGone(ctx, name) {
				return managedStorageError()
			}
			delete(live, name)
			continue
		}
		if err != nil {
			return managedStorageError()
		}
		orphans[name] = identity
	}
	if len(orphans) == 0 {
		return nil
	}
	r.mu.Lock()
	r.data.Orphans = maps.Clone(orphans)
	err := r.commit()
	r.mu.Unlock()
	if err != nil {
		return err
	}
	for name, p := range orphans {
		_ = m.eng.Remove(context.WithoutCancel(ctx), name)
		if !m.confirmGone(ctx, name) || !r.processGone(p) {
			return managedStorageError()
		}
		r.mu.Lock()
		delete(r.data.Orphans, name)
		err := r.commit()
		r.mu.Unlock()
		if err != nil {
			return err
		}
		delete(live, name)
	}
	return nil
}
