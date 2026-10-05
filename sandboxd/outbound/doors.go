package outbound

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

// doorConns bounds one sandbox's open connections per door; the guest's next dial waits in the backlog.
const doorConns = 256

// doors are a sandbox's proxy door and optional SOCKS5 door, with the proxy serving them once armed.
type doors struct {
	srv       *http.Server
	proxy     *egress.Proxy
	ln        net.Listener
	socks     net.Listener
	path      string
	socksPath string
}

func (d *doors) close() {
	if d.srv != nil {
		_ = d.srv.Close()
		d.proxy.Close()
	}
	_ = d.ln.Close()
	_ = os.Remove(d.path)
	if d.socks != nil {
		_ = d.socks.Close()
		_ = os.Remove(d.socksPath)
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

func bindDoors(vsock string, socks bool) (*doors, error) {
	d := &doors{path: engine.EgressSocketPath(vsock)}
	ln, err := listenUnix(d.path)
	if err != nil {
		return nil, fmt.Errorf("listen egress: %w", err)
	}
	d.ln = limitDoor(ln)
	if socks {
		d.socksPath = engine.SocksSocketPath(vsock)
		if ln, err = listenUnix(d.socksPath); err != nil {
			d.close()
			return nil, fmt.Errorf("listen socks: %w", err)
		}
		d.socks = limitDoor(ln)
	}
	return d, nil
}

// listenUnix binds path after clearing a stale socket from a prior life that would block the bind.
func listenUnix(path string) (*net.UnixListener, error) {
	_ = os.Remove(path)
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// reached reports whether a guest connected to a door before it was armed; only a clean EAGAIN says no.
func reached(ln net.Listener) bool {
	rc, err := ln.(syscall.Conn).SyscallConn()
	if err != nil {
		return true
	}
	waiting := true
	_ = rc.Control(func(fd uintptr) {
		syscall.ForkLock.RLock()
		defer syscall.ForkLock.RUnlock()
		nfd, _, err := syscall.Accept(int(fd))
		if err == nil {
			_ = syscall.Close(nfd)
			return
		}
		waiting = !errors.Is(err, syscall.EAGAIN)
	})
	return waiting
}
