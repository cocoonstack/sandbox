// Package outbound gives each sandbox its way out: the doors its guest dials, the egress proxy behind them, and the egress-lane NIC lock.
package outbound

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Engine is the VM engine surface the host needs: a VM's tap, and a guest's lane verdict and egress root.
type Engine interface {
	Inspect(ctx context.Context, name string) (types.VMRecord, bool, error)
	MarkLane(ctx context.Context, vsockSocket string, lane engine.Lane) error
	InstallCACert(ctx context.Context, vsockSocket string, certPEM []byte) error
}

// PooledFunc reports whether sb's key had a pool when it was claimed; it may take the caller's lock.
type PooledFunc func(sb *types.Sandbox) bool

// TapFunc applies or drops the nft lock on one tap.
type TapFunc func(tap string) error

// Options wires a Host to the node: its engine, the live egress view, and where events go.
type Options struct {
	Engine   Engine
	View     func() *View
	Pooled   PooledFunc
	Record   func(ctx context.Context, id, tenant string, ev egress.Event)
	Transfer func(ctx context.Context, id, tenant string, ev egress.Event, sent, received int64)
	CA       *egress.CA
	// LockNIC nft-locks egress-lane NICs, which only a node with bridges has.
	LockNIC bool
	// Dial, Verdict, Lock, Unlock and Sweep default to the internal-address guard and netfilter; tests swap them.
	Dial    egress.DialFunc
	Verdict func(ip netip.Addr, port uint16) (bool, error)
	Lock    TapFunc
	Unlock  TapFunc
	Sweep   func(keep map[string]bool) error
}

// Host holds every sandbox's doors and NIC lock; it guards its own state, never the caller's.
type Host struct {
	o Options

	mu       sync.Mutex
	doors    map[string]*doors
	prebound map[string]*doors
	taps     map[string]string
}

// New builds a Host.
func New(o Options) *Host {
	h := &Host{o: o, doors: map[string]*doors{}, prebound: map[string]*doors{}, taps: map[string]string{}}
	if h.o.Lock == nil {
		h.o.Lock = netfilter.Lock
	}
	if h.o.Unlock == nil {
		h.o.Unlock = netfilter.Unlock
	}
	if h.o.Sweep == nil {
		h.o.Sweep = netfilter.SweepExcept
	}
	if h.o.Dial == nil {
		h.o.Dial = newDialer(func() []egress.InternalAllow { return h.o.View().internalAllow }).DialContext
	}
	if h.o.Verdict == nil {
		h.o.Verdict = func(ip netip.Addr, port uint16) (bool, error) { return destVerdict(h.o.View().internalAllow, ip, port) }
	}
	return h
}

// Arm locks an egress-lane NIC, then serves the proxy: fail-closed, never a free NIC.
func (h *Host) Arm(ctx context.Context, sb *types.Sandbox) error {
	if err := h.lockNIC(ctx, sb); err != nil {
		return err
	}
	return h.ArmProxy(ctx, sb)
}

// ArmProxy serves the proxy on the pre-bound doors, binding them when refill could not, once the effective policy permits anything.
func (h *Host) ArmProxy(ctx context.Context, sb *types.Sandbox) error {
	v := h.o.View()
	if !v.guarded || sb.VsockSocket == "" {
		return nil
	}
	policy, ok := v.Resolve(sb, h.o.Pooled)
	if !ok {
		h.ClosePrebound(sb.VMName)
		return nil
	}
	id, tenant := sb.ID, sb.Tenant
	evCtx := context.WithoutCancel(ctx)
	h.mu.Lock()
	d := h.prebound[sb.VMName]
	delete(h.prebound, sb.VMName)
	h.mu.Unlock()
	// only an intercepting pool pays for the TLS transport and leaf cache
	ca := h.o.CA
	if !v.Intercepts(sb.PolicyKey()) {
		ca = nil
	}
	c := newClaim(h, sb, v, policy)
	proxy := egress.New(c, c, ca, c, func(ev egress.Event) { h.o.Record(evCtx, id, tenant, ev) }, sb)
	if v.usageBytes {
		proxy.OnTransfer(func(ev egress.Event, sent, received int64) { h.o.Transfer(evCtx, id, tenant, ev, sent, received) })
	}
	if d != nil && (reached(d.ln) || (d.socks != nil && reached(d.socks)) || (d.socks != nil) != policy.ServesSocks()) {
		d.close()
		d = nil
	}
	if d == nil {
		var err error
		if d, err = bindDoors(sb.VsockSocket, policy.ServesSocks()); err != nil {
			return err
		}
	}
	d.srv = &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	d.proxy = proxy
	h.mu.Lock()
	displaced := h.doors[id]
	h.doors[id] = d
	h.mu.Unlock()
	if displaced != nil {
		displaced.close()
	}
	go func() { _ = d.srv.Serve(d.ln) }()
	if d.socks != nil {
		go proxy.ServeSOCKS(evCtx, d.socks)
	}
	return nil
}

// Prebind binds a warm VM's doors at refill, so a claim only has to start serving them.
func (h *Host) Prebind(ctx context.Context, sb *types.Sandbox) {
	v := h.o.View()
	policy := v.poolEgress[sb.Key]
	if !v.guarded || policy == nil || sb.VsockSocket == "" {
		return
	}
	d, err := bindDoors(sb.VsockSocket, policy.ServesSocks())
	if err != nil {
		log.WithFunc("outbound.Prebind").Warnf(ctx, "pre-bind %s: %v; the claim binds instead", sb.VMName, err)
		return
	}
	h.mu.Lock()
	if !h.o.View().guarded {
		h.mu.Unlock()
		d.close()
		return
	}
	h.prebound[sb.VMName] = d
	h.mu.Unlock()
}

// ClosePrebound drops the doors bound for a warm VM that is going away or will not be served.
func (h *Host) ClosePrebound(name string) {
	h.mu.Lock()
	d := h.prebound[name]
	delete(h.prebound, name)
	h.mu.Unlock()
	if d != nil {
		d.close()
	}
}

// Disarm tears down a sandbox's doors, and its NIC lock only when the VM is removed.
func (h *Host) Disarm(id string, removed bool) {
	h.mu.Lock()
	d := h.doors[id]
	delete(h.doors, id)
	var tap string
	if removed {
		tap = h.taps[id]
		delete(h.taps, id)
	}
	h.mu.Unlock()
	if d != nil {
		d.close()
	}
	if tap != "" {
		_ = h.o.Unlock(tap)
	}
}

// Reloaded follows a view swap: doors pre-bound for a node that now guards nothing close, and live proxies drop their pooled connections.
func (h *Host) Reloaded(v *View) {
	h.mu.Lock()
	live := slices.Collect(maps.Values(h.doors))
	var stale []*doors
	if !v.guarded {
		stale = slices.Collect(maps.Values(h.prebound))
		clear(h.prebound)
	}
	h.mu.Unlock()
	for _, d := range stale {
		d.close()
	}
	for _, d := range live {
		d.proxy.ResetPools()
	}
}

// KeepLock records sandbox id's locked tap, so its removal unlocks it.
func (h *Host) KeepLock(id, tap string) {
	h.mu.Lock()
	h.taps[id] = tap
	h.mu.Unlock()
}

// UnlockTap drops tap's lock, for a removed VM no sandbox tracks.
func (h *Host) UnlockTap(tap string) error {
	return h.o.Unlock(tap)
}

// Route is how sb's guest reaches the network now: its own NIC, the proxy behind a bound door, or nothing.
func (h *Host) Route(sb *types.Sandbox) types.NetRoute {
	if h.Lane(sb.Key) == engine.LaneDirect {
		return types.NetRouteDirect
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.doors[sb.ID] != nil {
		return types.NetRouteRelay
	}
	return types.NetRouteNone
}

// Lane is the verdict a guest receives; the none lane has no NIC to route through and gets none.
func (h *Host) Lane(key types.PoolKey) engine.Lane {
	switch {
	case key.Net != types.NetEgress:
		return ""
	case h.LocksNIC(key):
		return engine.LaneRelay
	default:
		return engine.LaneDirect
	}
}

// LocksNIC reports whether key's VMs get an nft-locked NIC.
func (h *Host) LocksNIC(key types.PoolKey) bool {
	return h.o.LockNIC && key.Net == types.NetEgress
}

// LockedTaps lists the taps a lock already holds, nil when this node locks no NIC.
func (h *Host) LockedTaps() (map[string]bool, error) {
	if !h.o.LockNIC {
		return nil, nil
	}
	return netfilter.LockedTaps()
}

// Relock locks a restarted node's egress-lane claim to tap again and re-marks its guest's lane.
func (h *Host) Relock(ctx context.Context, sb *types.Sandbox, tap string) error {
	if err := h.o.Lock(tap); err != nil {
		return err
	}
	return h.markLane(ctx, sb.Key, sb.VsockSocket)
}

// Sweep drops every egress table whose tap keep does not name.
func (h *Host) Sweep(keep map[string]bool) error {
	return h.o.Sweep(keep)
}

// PrepareGuest tells a new guest its lane and, when v intercepts key, installs the egress root.
func (h *Host) PrepareGuest(ctx context.Context, v *View, key types.PoolKey, sock string) error {
	if err := h.markLane(ctx, key, sock); err != nil {
		return err
	}
	if v.Intercepts(key) {
		if err := h.o.Engine.InstallCACert(ctx, sock, h.o.CA.CertPEM()); err != nil {
			return fmt.Errorf("install egress ca: %w", err)
		}
	}
	return nil
}

// CAFingerprint is the egress root's fingerprint, or "" when the node intercepts nothing.
func (h *Host) CAFingerprint() string {
	if h.o.CA == nil {
		return ""
	}
	return h.o.CA.Fingerprint()
}

// RefuseInterceptOn turns intercept on only for a key whose guests can trust the CA: one the node has not served, with the CA loaded at boot.
func (h *Host) RefuseInterceptOn(old, next *View, served func(types.PoolKey) bool) error {
	for _, key := range slices.SortedFunc(maps.Keys(next.poolEgress), types.PoolKey.Compare) {
		if old.Intercepts(key) || !next.Intercepts(key) {
			continue
		}
		switch {
		case h.o.CA == nil:
			return fmt.Errorf("pool %s turns intercept on, which needs egress_ca loaded at boot: restart", key.Template)
		case served(key):
			return fmt.Errorf("pool %s turns intercept on, but its guests do not trust the CA: use a new pool key", key.Template)
		}
	}
	return nil
}

func (h *Host) markLane(ctx context.Context, key types.PoolKey, sock string) error {
	lane := h.Lane(key)
	if lane == "" {
		return nil
	}
	if err := h.o.Engine.MarkLane(ctx, sock, lane); err != nil {
		return fmt.Errorf("mark lane: %w", err)
	}
	return nil
}

func (h *Host) lockNIC(ctx context.Context, sb *types.Sandbox) error {
	if !h.LocksNIC(sb.Key) {
		return nil
	}
	tap := sb.TAP
	if tap == "" {
		var err error
		if tap, err = h.tapOf(ctx, sb.VMName); err != nil {
			return err
		}
	}
	if err := h.o.Lock(tap); err != nil {
		return fmt.Errorf("nft lock %s: %w", tap, err)
	}
	h.KeepLock(sb.ID, tap)
	return nil
}

func (h *Host) tapOf(ctx context.Context, vmName string) (string, error) {
	vm, ok, err := h.o.Engine.Inspect(ctx, vmName)
	if err != nil {
		return "", fmt.Errorf("list %s: %w", vmName, err)
	}
	if !ok {
		return "", fmt.Errorf("no NIC config for %s", vmName)
	}
	if tap := vm.TapDevice(); tap != "" {
		return tap, nil
	}
	return "", fmt.Errorf("no tap for %s", vmName)
}

// LoadCA reads cfg's egress root and this node's intermediate, nil when no pool intercepts.
func LoadCA(cfg *config.Config) (*egress.CA, error) {
	if !slices.ContainsFunc(cfg.Pools, func(s config.PoolSpec) bool { return s.Egress.Intercepts() }) {
		return nil, nil
	}
	ca := cfg.EgressCA
	root, err := os.ReadFile(ca.RootCert)
	if err != nil {
		return nil, fmt.Errorf("read root cert: %w", err)
	}
	interCert, err := os.ReadFile(ca.IntermediateCert)
	if err != nil {
		return nil, fmt.Errorf("read intermediate cert: %w", err)
	}
	interKey, err := os.ReadFile(ca.IntermediateKey)
	if err != nil {
		return nil, fmt.Errorf("read intermediate key: %w", err)
	}
	return egress.LoadCA(root, interCert, interKey)
}
