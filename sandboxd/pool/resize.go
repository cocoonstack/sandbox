package pool

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/usage"
)

// Resize moves a hibernated claim to the node's pool of its template and lane at size, then cold-boots it from its own disk; guest memory is lost.
func (m *Manager) Resize(ctx context.Context, id string, size types.Size) error {
	spec, ok := size.Spec()
	if !ok {
		return fmt.Errorf("%w: unknown size %q", ErrBadKey, size)
	}
	sb, ok := m.byID(id)
	if !ok {
		return ErrUnknownSandbox
	}
	if _, err := m.fenceWatch(sb); err != nil {
		return err
	}
	if err := m.eng.CheckResizeSupport(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrNotResizable, err)
	}
	if err := m.lockTransition(sb); err != nil {
		return err
	}
	defer sb.Transition.Unlock()
	if err := m.settleCapture(ctx, sb); err != nil {
		return fmt.Errorf("resize %s: %w", id, err)
	}
	key := sb.Key
	key.Size = size
	m.mu.Lock()
	_, pooled := m.activePool(key)
	live, volumes := m.claimed[id] == sb, m.hasVolumeHolds(sb)
	hibernated := sb.HibernateSnap != "" && sb.ArchiveCk == "" && sb.Failed == ""
	var gap []string
	if sb.Env.HasInject() {
		probe := &types.Sandbox{Key: key, PolicySource: sb.PolicySource, Layer: sb.Layer, Tenant: sb.Tenant, EgressClass: sb.EgressClass, NoEgress: sb.NoEgress}
		takePoolLayer(probe)
		out := m.view.Load().out
		had := out.InjectGap(sb, sb.Env)
		gap = slices.DeleteFunc(out.InjectGap(probe, sb.Env), func(g string) bool { return slices.Contains(had, g) })
	}
	m.mu.Unlock()
	switch {
	case !live:
		return ErrUnknownSandbox
	case !hibernated:
		return fmt.Errorf("%w: it is not hibernated", ErrNotResizable)
	case !pooled:
		return fmt.Errorf("%w: this node has no %s pool of its template and lane", ErrNotResizable, size)
	case len(gap) > 0:
		return fmt.Errorf("%w: inject %s", ErrNotResizable, strings.Join(gap, ", "))
	}
	rec, found, err := m.eng.Inspect(ctx, sb.VMName)
	if err != nil {
		return fmt.Errorf("resize %s: %w", id, err)
	}
	if reason := coldBootBlocker(sb, found, volumes); reason != "" {
		return fmt.Errorf("%w: %s", ErrNotResizable, strings.TrimPrefix(reason, "vmm exited; "))
	}
	if err = m.beginWake(sb, false); err != nil {
		return fmt.Errorf("resize %s: %w", id, err)
	}
	// A VM resized and left stopped is worse than a wasted call.
	ctx = context.WithoutCancel(ctx)
	if err = m.resolveIntent(ctx, sb); err == nil {
		err = m.eng.Resize(ctx, sb.VMName, spec, growTo(m.view.Load().poolStorage[key], rec.Config.Storage))
	}
	if err != nil {
		m.endWake(sb)
		return fmt.Errorf("resize %s: %w", id, err)
	}
	next, snap, err := m.reshape(ctx, sb, key)
	if err != nil {
		return fmt.Errorf("resize %s: %w", id, err)
	}
	defer next.Transition.Unlock()
	log.WithFunc("pool.Resize").Infof(ctx, "resized %s (%s) from %s to %s", id, next.VMName, sb.Key.Size, size)
	err = m.restartLocked(ctx, next)
	go m.dropSnap(ctx, snap)
	if err != nil {
		return fmt.Errorf("resize %s: %w", id, err)
	}
	return nil
}

// reshape replaces sb with a locked record of the same claim under key, awake and without its hibernate snapshot, which it also takes from sb; a published sandbox's key never changes.
func (m *Manager) reshape(ctx context.Context, sb *types.Sandbox, key types.PoolKey) (*types.Sandbox, string, error) {
	next := &types.Sandbox{}
	next.Transition.Lock()
	m.mu.Lock()
	sb.Waking = false
	m.store.recount(sb)
	switch {
	case m.claimed[sb.ID] != sb:
		m.mu.Unlock()
		return nil, "", ErrUnknownSandbox
	case m.evacuatingLocked(sb.ID):
		m.mu.Unlock()
		return nil, "", ErrEvacuating
	case sb.Replicated && m.isFenced():
		m.mu.Unlock()
		return nil, "", ErrLeaseLost
	}
	snap, readerSafe, deadline := sb.HibernateSnap, sb.ReaderSafe, sb.Deadline
	sb.HibernateSnap, sb.ReaderSafe = "", false
	m.renewWokenLeaseLocked(sb)
	dto := dtoOf(sb)
	dto.Key = key
	raw, err := json.Marshal(&dto)
	if err == nil {
		err = json.Unmarshal(raw, next)
	}
	if err != nil {
		sb.HibernateSnap, sb.ReaderSafe, sb.Deadline = snap, readerSafe, deadline
		m.mu.Unlock()
		return nil, "", fmt.Errorf("copy the claim: %w", err)
	}
	if next.VolumesWant == nil {
		next.VolumesWant = next.Volumes
	}
	next.StaleSnap, next.ReplicaDue = sb.StaleSnap, next.HasReplica()
	next.SetEnv(next.Env)
	takePoolLayer(next)
	next.Touch()
	m.dropClaimLocked(sb)
	m.addClaimLocked(next)
	js := m.store.set(next)
	ev := usage.Event{
		Time: time.Now(), Event: "resize", ID: next.ID, VMName: next.VMName,
		KeyHash: key.Hash(), Template: key.Template, Net: string(key.Net), Size: string(key.Size),
		Tenant: next.Tenant, Deadline: next.Deadline,
	}
	m.mu.Unlock()
	if err = m.store.commit(js); err != nil {
		m.mu.Lock()
		var rb claimSnapshot
		if m.claimed[next.ID] == next {
			sb.HibernateSnap, sb.ReaderSafe, sb.Deadline = snap, readerSafe, deadline
			m.dropClaimLocked(next)
			m.addClaimLocked(sb)
			rb = m.store.set(sb)
		}
		m.mu.Unlock()
		next.Transition.Unlock()
		m.store.recommit(ctx, rb)
		return nil, "", fmt.Errorf("persist claims: %w", err)
	}
	m.recordUsage(ctx, ev)
	return next, snap, nil
}

// growTo is the pool's storage when it is above the disk's size; a disk only grows, and cocoon refuses a smaller one.
func growTo(storage string, disk int64) string {
	if n, _ := config.StorageBytes(storage); n <= disk {
		return ""
	}
	return storage
}

func takePoolLayer(sb *types.Sandbox) {
	if sb.PolicyKey() == sb.Key {
		sb.Layer = types.LayerPooled
	}
}
