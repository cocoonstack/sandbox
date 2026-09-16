package sandbox

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
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

// agentPool parks a handle's idle relay connections between calls.
type agentPool struct {
	mu   sync.Mutex
	idle []*agentConn
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
		if c.timer.Stop() && c.quiet() {
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
	if c.timer == nil {
		c.timer = time.AfterFunc(idle, func() { p.evict(c) })
	} else {
		c.timer.Reset(idle)
	}
	p.idle = append(p.idle, c)
	p.mu.Unlock()
}

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

// agentConn is one relayed silkd connection, with the TCP socket's raw handle for the liveness peek.
type agentConn struct {
	*silkd.Conn
	sock   syscall.RawConn
	peeked bool
	peekFn func(uintptr) bool
	timer  *time.Timer
}

func newAgentConn(raw *upgradedConn) *agentConn {
	c := &agentConn{Conn: silkd.NewConn(raw)}
	if sc, ok := raw.tcp.(syscall.Conn); ok {
		c.sock, _ = sc.SyscallConn()
	}
	c.peekFn = c.peek
	return c
}

// quiet reports whether the parked connection's peer has neither hung up nor spoken.
func (c *agentConn) quiet() bool {
	return c.sock != nil && c.sock.Read(c.peekFn) == nil && c.peeked
}

// sent sends req on c and hands it back, closing it on failure.
func (c *agentConn) sent(req wire.Request) (*agentConn, error) {
	if err := c.Send(req); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
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
	if _, frame := errors.AsType[*wire.ErrorResp](err); err == nil || frame {
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
	if !l.stop() || !canProbe || l.s.c.keepAlive <= 0 || l.s.proto.Load() < wire.KeepAliveProto {
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
