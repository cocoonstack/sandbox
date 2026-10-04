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
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/netfilter"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// Engine is the VM engine surface the host needs: a guest's lane verdict and a VM's tap.
type Engine interface {
	Inspect(ctx context.Context, name string) (types.VMRecord, bool, error)
	MarkLane(ctx context.Context, vsockSocket string, lane engine.Lane) error
}

// TapFunc applies or drops the nft lock on one tap.
type TapFunc func(tap string) error

// Options wires a Host to the node: its engine, the live egress view, and where events go.
type Options struct {
	Engine Engine
	View   func() *View
	// Pooled reports whether sb's key had a pool when it was claimed; it may take the caller's lock.
	Pooled   func(sb *types.Sandbox) bool
	Record   func(ctx context.Context, id, tenant string, ev egress.Event)
	Transfer func(ctx context.Context, id, tenant string, ev egress.Event, sent, received int64)
	CA       *egress.CA
	// LockNIC nft-locks egress-lane NICs, which only a node with bridges has.
	LockNIC bool
	// Dial, Verdict, Lock and Unlock default to the internal-address guard and netfilter; tests swap them.
	Dial    egress.DialFunc
	Verdict func(ip netip.Addr, port uint16) (bool, error)
	Lock    TapFunc
	Unlock  TapFunc
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
	policy, ok := v.Resolve(sb, func() bool { return h.o.Pooled(sb) })
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
	proxy := egress.New(newLivePolicy(h, sb, v, policy), claimSecrets{h: h, sb: sb}, ca, claimRouter{h: h, sb: sb},
		func(ev egress.Event) { h.o.Record(evCtx, id, tenant, ev) }, sb)
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

// LockTap nft-locks tap's guest egress to the proxy.
func (h *Host) LockTap(tap string) error {
	return h.o.Lock(tap)
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

// LocksNICs reports whether this node nft-locks egress-lane NICs at all.
func (h *Host) LocksNICs() bool {
	return h.o.LockNIC
}

// MarkLane tells key's guest at sock its lane verdict; a non-egress key gets none.
func (h *Host) MarkLane(ctx context.Context, key types.PoolKey, sock string) error {
	lane := h.Lane(key)
	if lane == "" {
		return nil
	}
	if err := h.o.Engine.MarkLane(ctx, sock, lane); err != nil {
		return fmt.Errorf("mark lane: %w", err)
	}
	return nil
}

// CA is the egress root the node intercepts with, nil when no pool intercepted at boot.
func (h *Host) CA() *egress.CA {
	return h.o.CA
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
	for _, key := range slices.SortedFunc(maps.Keys(next.poolEgress), func(a, b types.PoolKey) int { return strings.Compare(a.Hash(), b.Hash()) }) {
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

// LoadCA reads the egress root and this node's intermediate.
func LoadCA(cfg *config.EgressCAConfig) (*egress.CA, error) {
	root, err := os.ReadFile(cfg.RootCert)
	if err != nil {
		return nil, fmt.Errorf("read root cert: %w", err)
	}
	interCert, err := os.ReadFile(cfg.IntermediateCert)
	if err != nil {
		return nil, fmt.Errorf("read intermediate cert: %w", err)
	}
	interKey, err := os.ReadFile(cfg.IntermediateKey)
	if err != nil {
		return nil, fmt.Errorf("read intermediate key: %w", err)
	}
	return egress.LoadCA(root, interCert, interKey)
}
