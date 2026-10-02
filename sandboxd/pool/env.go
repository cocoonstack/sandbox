package pool

import (
	"context"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const envWriteLimit = 8

// SetEnv replaces a claim's env, an empty map clearing it; a change to its guest entries needs a running guest and is written there at once.
func (m *Manager) SetEnv(ctx context.Context, id string, env types.Env, tenant string) error {
	return m.writeEnv(ctx, id, tenant, "set", func(types.Env) types.Env { return env })
}

// PatchEnv sets or removes the named entries of a claim's env and keeps the rest; an unchanged result repairs a running guest's env file.
func (m *Manager) PatchEnv(ctx context.Context, id string, patch types.EnvPatch, tenant string) error {
	return m.writeEnv(ctx, id, tenant, "patch", patch.Apply)
}

// Env reads a claim's env with host-only values dropped; a tenant reaches only its own claims.
func (m *Manager) Env(id, tenant string) (types.Env, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := m.claimed[id]
	if sb == nil || !tenantOwns(tenant, sb.Tenant) {
		return nil, ErrUnknownSandbox
	}
	return sb.Env.Redacted(), nil
}

// An unchanged result must reach the guest again to repair a failed write after the store step.
func (m *Manager) writeEnv(ctx context.Context, id, tenant, verb string, next func(types.Env) types.Env) error {
	sb, ok := m.byID(id)
	if !ok || !tenantOwns(tenant, sb.Tenant) {
		return ErrUnknownSandbox
	}
	// a guest write must not race a capture
	locked := sb.Transition.TryLock()
	if locked {
		defer sb.Transition.Unlock()
	}
	m.mu.Lock()
	if m.claimed[id] != sb {
		m.mu.Unlock()
		return ErrUnknownSandbox
	}
	prev, sock, failed := sb.Env, sb.VsockSocket, sb.Failed != ""
	env := next(prev)
	if len(env) == 0 {
		env = nil
	}
	if err := env.Validate(); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("%w: %w", ErrBadEnv, err)
	}
	if err := m.checkUpstreamEnv(env); err != nil {
		m.mu.Unlock()
		return err
	}
	changed := !env.SameGuest(prev)
	paused := !locked || sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.ArchiveCk != "" || failed
	if changed && paused {
		m.mu.Unlock()
		if failed {
			return ErrFailed
		}
		return ErrPaused
	}
	deliver := !paused && (changed || env.Equal(prev))
	if env.Equal(prev) {
		m.mu.Unlock()
		return m.deliverGuestEnv(ctx, verb, id, sock, env, deliver)
	}
	sb.Env = env
	js := m.store.set(sb)
	m.mu.Unlock()
	if err := m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		if m.claimed[id] == sb && sb.Env.Equal(env) {
			sb.Env = prev
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		m.recommit(ctx, rb)
		return fmt.Errorf("%s env %s: persist claims: %w", verb, id, err)
	}
	return m.deliverGuestEnv(ctx, verb, id, sock, env, deliver)
}

func (m *Manager) deliverGuestEnv(ctx context.Context, verb, id, sock string, env types.Env, deliver bool) error {
	if !deliver {
		return nil
	}
	if err := m.eng.WriteGuestEnv(ctx, sock, env.GuestFile()); err != nil {
		return fmt.Errorf("%s env %s: write to the guest: %w", verb, id, err)
	}
	return nil
}

// deliverEnv writes each claim's guest env file before it is handed out; an inherited guest holds its source's file, so it is rewritten even when empty.
func (m *Manager) deliverEnv(ctx context.Context, sbs []*types.Sandbox, inherited bool) error {
	if !inherited && !slices.ContainsFunc(sbs, func(sb *types.Sandbox) bool { return sb.Env.HasGuest() }) {
		return nil
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(envWriteLimit)
	for _, sb := range sbs {
		if !inherited && !sb.Env.HasGuest() {
			continue
		}
		group.Go(func() error {
			if err := m.eng.WriteGuestEnv(groupCtx, sb.VsockSocket, sb.Env.GuestFile()); err != nil {
				return fmt.Errorf("deliver env to %s: %w", sb.VMName, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// heldGuestEnv reports whether sb's guest holds an env file; the caller holds sb.Transition, so no guest write races its capture.
func (m *Manager) heldGuestEnv(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sb.Env.HasGuest()
}

var _ egress.Secrets = claimSecrets{}

// claimSecrets resolves a rule's secret to the claim's host-only env first, the node's value second.
type claimSecrets struct {
	m  *Manager
	sb *types.Sandbox
}

func (c claimSecrets) Header(name string) (header, value string, ok bool) {
	secrets := c.m.view.Load().secrets
	if header, value, ok = secrets.Header(name); !ok {
		return "", "", false
	}
	env := secrets.EnvName(name)
	c.m.mu.Lock()
	own, set := c.sb.Env.Hidden(env)
	c.m.mu.Unlock()
	if set {
		value = own
	}
	return header, value, true
}
