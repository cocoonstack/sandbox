package outbound

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

// doorConns bounds one sandbox's open connections per door; the guest's next dial waits in the backlog.
const doorConns = 256

// doors are a sandbox's proxy door and optional SOCKS5 and DNS doors, served once armed.
type doors struct {
	srv      *http.Server
	proxy    *egress.Proxy
	resolver *resolver
	ln       net.Listener
	socks    net.Listener
	dns      net.Listener
}

func (d *doors) close() {
	if d.srv != nil {
		_ = d.srv.Close()
		d.proxy.Close()
	}
	_ = d.ln.Close()
	if d.socks != nil {
		_ = d.socks.Close()
	}
	if d.dns != nil {
		_ = d.dns.Close()
	}
	if d.resolver != nil {
		d.resolver.close()
	}
}

// doorListener caps a door's live connections; doorConn's NetConn hands splice the *net.UnixConn the kernel copy needs.
type doorListener struct {
	*net.UnixListener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

func limitDoor(ln *net.UnixListener) *doorListener {
	return &doorListener{UnixListener: ln, slots: make(chan struct{}, doorConns), done: make(chan struct{})}
}

func (l *doorListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.AcceptUnix()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &doorConn{UnixConn: conn, slots: l.slots}, nil
}

func (l *doorListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.UnixListener.Close()
}

type doorConn struct {
	*net.UnixConn
	slots chan struct{}
	once  sync.Once
}

func (c *doorConn) Close() error {
	err := c.UnixConn.Close()
	c.once.Do(func() { <-c.slots })
	return err
}

func (c *doorConn) NetConn() net.Conn { return c.UnixConn }

func bindDoors(vsock string, socks, dns bool) (*doors, error) {
	ln, err := listenUnix(engine.EgressSocketPath(vsock))
	if err != nil {
		return nil, fmt.Errorf("listen egress: %w", err)
	}
	d := &doors{ln: limitDoor(ln)}
	if socks {
		if ln, err = listenUnix(engine.SocksSocketPath(vsock)); err != nil {
			d.close()
			return nil, fmt.Errorf("listen socks: %w", err)
		}
		d.socks = limitDoor(ln)
	}
	if dns {
		if ln, err = listenUnix(engine.DNSSocketPath(vsock)); err != nil {
			d.close()
			return nil, fmt.Errorf("listen dns: %w", err)
		}
		d.dns = limitDoor(ln)
	}
	return d, nil
}

// listenUnix binds path after clearing a stale socket from a prior life that would block the bind.
func listenUnix(path string) (*net.UnixListener, error) {
	_ = os.Remove(path)
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// reached reports whether a guest connected to a door before it was armed; a failed poll counts as reached.
func reached(ln net.Listener) bool {
	rc, err := ln.(syscall.Conn).SyscallConn()
	if err != nil {
		return true
	}
	waiting := true
	_ = rc.Control(func(fd uintptr) {
		ready, pollErr := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0) //nolint:gosec // a listener's fd fits
		waiting = pollErr != nil || ready > 0
	})
	return waiting
}
