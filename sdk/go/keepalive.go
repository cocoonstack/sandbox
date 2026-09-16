package sandbox

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

const (
	keepAliveIdle  = 30 * time.Second
	keepAliveConns = 8
)

// WithKeepAlive bounds how long a handle keeps an idle relay connection for
// its next call (default 30s; 0 dials per call). An open connection holds
// the sandbox's idle-hibernate clock, so keep it below the deployment's
// idle_hibernate_seconds.
func WithKeepAlive(idle time.Duration) ClientOption {
	return func(c *Client) { c.keepAlive = idle }
}

// agentPool parks a handle's idle relay connections between calls; silkd serves RPCs back to back from proto 2.
type agentPool struct {
	mu    sync.Mutex
	idle  []*agentConn
	proto atomic.Uint32
}

// take returns a parked connection whose peer is still there, or nil.
func (p *agentPool) take() *agentConn {
	for {
		p.mu.Lock()
		n := len(p.idle)
		if n == 0 {
			p.mu.Unlock()
			return nil
		}
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		if c.timer.Stop() && peerQuiet(c.raw.tcp) {
			return c
		}
		_ = c.Close()
	}
}

// park keeps c for the next call and closes it once idle has passed.
func (p *agentPool) park(c *agentConn, idle time.Duration) {
	p.mu.Lock()
	if len(p.idle) >= keepAliveConns {
		p.mu.Unlock()
		_ = c.Close()
		return
	}
	c.timer = time.AfterFunc(idle, func() { p.evict(c) })
	p.idle = append(p.idle, c)
	p.mu.Unlock()
}

// drain closes every parked connection.
func (p *agentPool) drain() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		c.timer.Stop()
		_ = c.Close()
	}
}

func (p *agentPool) evict(c *agentConn) {
	p.mu.Lock()
	if i := slices.Index(p.idle, c); i >= 0 {
		p.idle = slices.Delete(p.idle, i, i+1)
	}
	p.mu.Unlock()
	_ = c.Close()
}

// agentConn is one relayed silkd connection, with the upgraded conn underneath for the liveness probe.
type agentConn struct {
	*silkd.Conn
	raw   *upgradedConn
	timer *time.Timer
}

// probeProto pipelines info ahead of req and reads the daemon's answer.
func (c *agentConn) probeProto(ctx context.Context, req wire.Request) (uint32, error) {
	if err := c.Send(wire.Info{}); err != nil {
		return 0, err
	}
	if err := c.Send(req); err != nil {
		return 0, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	info, err := expect[wire.InfoResp](ctx, c.Conn)
	if err != nil {
		return 0, err
	}
	return max(info.Proto, 1), nil
}

// lease is one RPC's hold on a relay connection: close drops it, reuse parks it for the next call.
type lease struct {
	s    *Sandbox
	conn *agentConn
	stop func() bool
	over atomic.Bool
}

// done ends the RPC: a terminal frame, error frames included, parks the connection; any other failure drops it.
func (l *lease) done(err error) error {
	var frame *wire.ErrorResp
	if err == nil || errors.As(err, &frame) {
		l.reuse()
	} else {
		l.close()
	}
	return err
}

func (l *lease) reuse() {
	if !l.over.CompareAndSwap(false, true) {
		return
	}
	if !l.stop() || !canProbe || l.s.c.keepAlive <= 0 {
		_ = l.conn.Close()
		return
	}
	l.s.pool.park(l.conn, l.s.c.keepAlive)
}

func (l *lease) close() {
	if !l.over.CompareAndSwap(false, true) {
		return
	}
	l.stop()
	_ = l.conn.Close()
}
