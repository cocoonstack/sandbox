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
	if len(env) == 0 {
		env = nil
	}
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
	prev, sock := sb.Env, sb.VsockSocket
	changed := !env.SameGuest(prev)
	paused := !locked || sb.HibernateSnap != "" || sb.PendingSnap != "" || sb.ArchiveCk != ""
	if changed && paused {
		m.mu.Unlock()
		return ErrPaused
	}
	// an exact resend is rewritten too, so a repeated call repairs a lost file or a failed clear
	deliver := !paused && (changed || env.Equal(prev))
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
		return fmt.Errorf("set env %s: persist claims: %w", id, err)
	}
	if !deliver {
		return nil
	}
	if err := m.eng.WriteGuestEnv(ctx, sock, env.GuestFile()); err != nil {
		return fmt.Errorf("set env %s: write to the guest: %w", id, err)
	}
	return nil
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
	if header, value, ok = c.m.egressSecrets.Header(name); !ok {
		return "", "", false
	}
	env := c.m.egressSecrets.EnvName(name)
	c.m.mu.Lock()
	own, set := c.sb.Env.Hidden(env)
	c.m.mu.Unlock()
	if set {
		value = own
	}
	return header, value, true
}
