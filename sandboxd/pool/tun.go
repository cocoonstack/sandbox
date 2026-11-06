package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/outbound"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const tunPlanTries = 3

var errTunRaced = errors.New("tun state changed since planned")

// tunSeeder is a node-local address store that learns which addresses the loaded claims hold.
type tunSeeder interface {
	Seed(held map[string]netip.Addr)
}

type tunState struct {
	off     bool
	profile string
	addr    netip.Addr
}

// SetTun updates a claim's tun state without waking a hibernated or archived claim.
func (m *Manager) SetTun(ctx context.Context, id string, enabled *bool, profile *string) error {
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	locked := sb.Transition.TryLock()
	if locked {
		defer sb.Transition.Unlock()
	}
	for range tunPlanTries {
		if err := m.setTun(ctx, sb, locked, enabled, profile); !errors.Is(err, errTunRaced) {
			return err
		}
	}
	return ErrPaused
}

// TunStats snapshots the tun router's counters; false when the node has no tun block.
func (m *Manager) TunStats() (tunnet.Stats, bool) {
	if m.tunRouter == nil {
		return tunnet.Stats{}, false
	}
	return m.tunRouter.Stats(), true
}

func (m *Manager) setTun(ctx context.Context, sb *types.Sandbox, locked bool, enabled *bool, profile *string) error {
	id := sb.ID
	prev, next, assign, err := m.planTun(sb, locked, enabled, profile)
	if err != nil {
		return err
	}
	if assign {
		if next.addr, err = m.tunAddrs.Assign(ctx, id); err != nil {
			return fmt.Errorf("set tun %s: %w", id, err)
		}
	}
	if next == prev {
		if m.tunRowed(sb) {
			return m.rewriteArchiveRow(ctx, sb, locked)
		}
		return nil
	}
	sock, rowed, err := m.commitTun(ctx, sb, locked, prev, next, assign)
	if err != nil {
		return err
	}
	switch {
	case next.off:
		m.out.DisarmTun(ctx, id, sock)
	case sock != "":
		m.out.RearmTun(ctx, sb)
	}
	if rowed {
		if err := m.rewriteArchiveRow(ctx, sb, locked); err != nil {
			return fmt.Errorf("set tun %s: %w", id, err)
		}
	}
	return nil
}

// assignTun gives each new claim of a tun pool its overlay address; a failure leaves that claim without tun, and a mesh never falls back to a local address.
func (m *Manager) assignTun(ctx context.Context, sbs []*types.Sandbox) {
	if m.tunAddrs == nil {
		return
	}
	v := m.view.Load().out
	for _, sb := range sbs {
		if !v.Tun(sb.PolicyKey()) {
			continue
		}
		addr, err := m.tunAddrs.Assign(ctx, sb.ID)
		if err != nil {
			log.WithFunc("pool.assignTun").Warnf(ctx, "claim %s runs without tun: %v", sb.ID, err)
			continue
		}
		sb.TunAddr = addr
	}
}

func (m *Manager) seedTun() {
	seeder, ok := m.tunAddrs.(tunSeeder)
	if !ok {
		return
	}
	held := map[string]netip.Addr{}
	m.mu.Lock()
	for id, sb := range m.claimed {
		if sb.TunAddr.IsValid() {
			held[id] = sb.TunAddr
		}
	}
	m.mu.Unlock()
	seeder.Seed(held)
}

func (m *Manager) releaseTun(ctx context.Context, id string, addr netip.Addr) {
	if m.tunAddrs == nil || !addr.IsValid() {
		return
	}
	if err := m.tunAddrs.Release(context.WithoutCancel(ctx), id, addr); err != nil {
		log.WithFunc("pool.releaseTun").Warnf(ctx, "release overlay address %s of %s, retrying hourly: %v", addr, id, err)
		m.mu.Lock()
		if m.tunReleases == nil {
			m.tunReleases = map[string]netip.Addr{}
		}
		m.tunReleases[id] = addr
		m.mu.Unlock()
	}
}

func (m *Manager) retryTunReleases(ctx context.Context) {
	m.mu.Lock()
	pending := maps.Clone(m.tunReleases)
	m.mu.Unlock()
	for id, addr := range pending {
		if err := m.tunAddrs.Release(ctx, id, addr); err != nil {
			log.WithFunc("pool.retryTunReleases").Warnf(ctx, "release overlay address %s of %s: %v", addr, id, err)
			return
		}
		m.mu.Lock()
		delete(m.tunReleases, id)
		m.mu.Unlock()
	}
}

func (m *Manager) releaseRowTun(ctx context.Context, rec archives.Record) {
	if m.tunAddrs == nil {
		return
	}
	sb := &types.Sandbox{}
	if err := json.Unmarshal(rec.Claim, sb); err != nil {
		log.WithFunc("pool.releaseRowTun").Warnf(ctx, "decode archive row %s: %v", rec.ID, err)
		return
	}
	m.releaseTun(ctx, rec.ID, sb.TunAddr)
}

// foldTunBytes ends the live flows, so their records are written, and copies every armed claim's live tun byte count into its record, so the next commit persists it.
func (m *Manager) foldTunBytes() {
	if m.tunRouter == nil {
		return
	}
	m.tunRouter.EndFlows()
	m.mu.Lock()
	sbs := slices.Collect(maps.Values(m.claimed))
	m.mu.Unlock()
	for _, sb := range sbs {
		if used, ok := m.out.TunUsed(sb.ID); ok {
			m.setTunBytes(sb, used)
		}
	}
}

func (m *Manager) tunState(sb *types.Sandbox) outbound.TunState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return outbound.TunState{On: !sb.TunOff, Addr: sb.TunAddr, Profile: sb.TunProfile, Bytes: sb.TunBytes}
}

func (m *Manager) tunRowed(sb *types.Sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rowFollows(sb) || sb.ArchivePending != ""
}

func (m *Manager) planTun(sb *types.Sandbox, locked bool, enabled *bool, profile *string) (prev, next tunState, assign bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.claimed[sb.ID] != sb:
		return prev, next, false, ErrUnknownSandbox
	case m.pausedForMoveLocked(sb):
		return prev, next, false, ErrEvacuating
	}
	prev = tunState{off: sb.TunOff, profile: sb.TunProfile, addr: sb.TunAddr}
	next = prev
	if enabled != nil {
		next.off = !*enabled
	}
	if profile != nil {
		next.profile = *profile
	}
	// only a disable runs beside a transition, which reads the rest of the tun state
	if !locked && (!next.off || next.profile != prev.profile) {
		return prev, next, false, ErrPaused
	}
	assign = !next.off && !prev.addr.IsValid() && m.tunAddrs != nil && m.view.Load().out.Tun(sb.PolicyKey())
	return prev, next, assign, nil
}

// commitTun persists next on sb and returns the running guest's socket, empty when it is not running or a capture holds it.
func (m *Manager) commitTun(
	ctx context.Context, sb *types.Sandbox, locked bool, prev, next tunState, assign bool,
) (sock string, rowed bool, err error) {
	m.mu.Lock()
	if m.claimed[sb.ID] != sb {
		m.mu.Unlock()
		if assign {
			m.releaseTun(ctx, sb.ID, next.addr)
		}
		return "", false, ErrUnknownSandbox
	}
	if (tunState{off: sb.TunOff, profile: sb.TunProfile, addr: sb.TunAddr}) != prev {
		m.mu.Unlock()
		if assign {
			m.releaseTun(ctx, sb.ID, next.addr)
		}
		return "", false, errTunRaced
	}
	sb.TunOff, sb.TunProfile, sb.TunAddr = next.off, next.profile, next.addr
	if locked && runningShaped(sb) {
		sock = sb.VsockSocket
	}
	rowed = m.rowFollows(sb) || sb.ArchivePending != ""
	js := m.store.set(sb)
	m.mu.Unlock()
	if err = m.store.commit(js); err == nil {
		return sock, rowed, nil
	}
	m.mu.Lock()
	var rb claimSnapshot
	undone := m.claimed[sb.ID] == sb && sb.TunOff == next.off && sb.TunProfile == next.profile && sb.TunAddr == next.addr
	if undone {
		sb.TunOff, sb.TunProfile, sb.TunAddr = prev.off, prev.profile, prev.addr
		rb = m.store.set(sb)
	}
	m.mu.Unlock()
	m.store.recommit(ctx, rb)
	if undone && assign {
		m.releaseTun(ctx, sb.ID, next.addr)
	}
	return "", false, fmt.Errorf("set tun %s: persist claims: %w", sb.ID, err)
}

func (m *Manager) setTunBytes(sb *types.Sandbox, used int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb.TunBytes = used
	if m.claimed[sb.ID] == sb {
		m.store.set(sb)
	}
}
