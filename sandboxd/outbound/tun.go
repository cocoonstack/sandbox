package outbound

import (
	"cmp"
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

// tunVerbTimeout bounds a tun_up or tun_down; an old silkd that never answers costs the caller no more.
const tunVerbTimeout = 3 * time.Second

// tunDoor is a claim's packet door and the byte counter its router claim charges.
type tunDoor struct {
	ln   net.Listener
	addr netip.Addr
	used *atomic.Int64
	sb   *types.Sandbox
}

// RearmTun re-arms an armed claim's packet door after its tun switch or profile changed; its proxy doors stay as they are.
func (h *Host) RearmTun(ctx context.Context, sb *types.Sandbox) {
	if h.o.Router == nil {
		return
	}
	h.mu.Lock()
	d := h.doors[sb.ID]
	old := d.takeTun()
	h.mu.Unlock()
	h.dropTun(old)
	v := h.o.View()
	policy, ok := v.Resolve(sb)
	if d == nil || !ok {
		return
	}
	h.armTun(ctx, sb, d, v, newClaim(h, sb, v, policy))
}

// DisarmTun revokes a claim's packet path at once, then asks the guest at sock, when it runs, to drop tun0.
func (h *Host) DisarmTun(ctx context.Context, id, sock string) {
	h.mu.Lock()
	t := h.doors[id].takeTun()
	h.mu.Unlock()
	if t == nil {
		return
	}
	h.dropTun(t)
	if sock == "" {
		return
	}
	vctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tunVerbTimeout)
	defer cancel()
	if err := h.o.Engine.TunDown(vctx, sock); err != nil {
		log.WithFunc("outbound.DisarmTun").Warnf(ctx, "tun_down %s: %v", id, err)
	}
}

// TunUsed reads the live tun byte count of claim id; false when no packet door is armed.
func (h *Host) TunUsed(id string) (int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if d := h.doors[id]; d != nil && d.tun != nil {
		return d.tun.used.Load(), true
	}
	return 0, false
}

// armTun binds the packet door of sb, whose doors d hold none, when its pool has tun and the claim is on and holds an address; then it arms the router and turns the guest's tun0 on.
func (h *Host) armTun(ctx context.Context, sb *types.Sandbox, d *doors, v *View, c *claim) {
	key := sb.PolicyKey()
	if h.o.Router == nil || !v.Tun(key) || !sb.TunAddr.IsValid() || sb.VsockSocket == "" || !h.tunOn(sb) {
		return
	}
	logger := log.WithFunc("outbound.armTun")
	ln, err := listenUnix(engine.TunSocketPath(sb.VsockSocket))
	if err != nil {
		logger.Warnf(ctx, "bind the packet door of %s, it runs without tun: %v", sb.ID, err)
		return
	}
	t := &tunDoor{ln: limitDoor(ln), addr: sb.TunAddr, used: new(atomic.Int64), sb: sb}
	t.used.Store(sb.TunBytes)
	id, tenant, profile, sock, usage := sb.ID, sb.Tenant, sb.TunProfile, sb.VsockSocket, v.usageBytes
	evCtx := context.WithoutCancel(ctx)
	tc := tunnet.Claim{
		ID:      id,
		Addr:    t.addr,
		Enabled: func() bool { return h.o.View().Tun(key) && h.tunLive(id, t) && h.tunOn(sb) },
		Profile: profile,
		Allow: func(dst netip.Addr, port uint16) bool {
			if c.current().EvalFlow(port) != egress.DecisionAllow {
				return false
			}
			_, refused := h.o.Verdict(dst, port)
			return refused == nil
		},
		Limits: v.tunLimits,
		Used:   t.used,
		Record: func(r tunnet.FlowRecord) {
			if h.o.Flow != nil {
				h.o.Flow(evCtx, id, tenant, profile, r)
			}
			if usage && r.Allowed && r.Up+r.Down > 0 {
				h.o.Transfer(evCtx, id, tenant, egress.Event{Host: r.Dst.String(), Port: r.Port, Upstream: "tun/" + cmp.Or(profile, "default")}, r.Up, r.Down)
			}
		},
	}
	h.o.Router.Arm(tc)
	h.mu.Lock()
	live := h.doors[id] == d
	if live {
		d.tun = t
	}
	h.mu.Unlock()
	if !live {
		h.dropTun(t)
		return
	}
	go h.serveTun(evCtx, t)
	go func() {
		vctx, cancel := context.WithTimeout(evCtx, tunVerbTimeout)
		defer cancel()
		if err := h.o.Engine.TunUp(vctx, sock); err != nil {
			logger.Warnf(ctx, "tun_up %s: %v", id, err)
		}
		if !h.tunLive(id, t) {
			_ = h.o.Engine.TunDown(vctx, sock)
		}
	}()
}

func (h *Host) serveTun(ctx context.Context, t *tunDoor) {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return
		}
		go func() {
			if err := h.o.Router.Serve(conn, t.addr); err != nil {
				log.WithFunc("outbound.serveTun").Debugf(ctx, "packet stream of %s: %v", t.sb.ID, err)
			}
		}()
	}
}

func (h *Host) tunOn(sb *types.Sandbox) bool {
	return h.o.TunOn == nil || h.o.TunOn(sb)
}

// tunLive reports whether t is still the packet door of claim id.
func (h *Host) tunLive(id string, t *tunDoor) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := h.doors[id]
	return d != nil && d.tun == t
}

// dropTun closes t's door, disarms its address and hands the claim's byte count back.
func (h *Host) dropTun(t *tunDoor) {
	if t == nil {
		return
	}
	_ = t.ln.Close()
	h.o.Router.Disarm(t.addr)
	if h.o.TunBytes != nil {
		h.o.TunBytes(t.sb, t.used.Load())
	}
}
