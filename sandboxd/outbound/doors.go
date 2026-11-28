package outbound

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
)

const (
	// doorConns bounds one sandbox's open connections per door; a connection past it is closed on accept.
	doorConns = 256
	// The kernel clamps a listen backlog to somaxconn.
	doorBacklog = math.MaxInt32
)

type unixBinder func(path string) (*net.UnixListener, error)

type doorSpec struct {
	ln   *net.Listener
	path string
	name string
	on   bool
}

// doors are a sandbox's proxy door and optional SOCKS5, DNS and packet doors, served once armed.
type doors struct {
	srv      *http.Server
	proxy    *egress.Proxy
	resolver *resolver
	ln       net.Listener
	socks    net.Listener
	dns      net.Listener
	dnsGram  net.Listener
	// tun is guarded by the host mutex.
	tun *tunDoor
}

func (d *doors) close() {
	if d.srv != nil {
		_ = d.srv.Close()
		d.proxy.Close()
	}
	for _, ln := range d.listeners() {
		if ln != nil {
			_ = ln.Close()
		}
	}
	if d.resolver != nil {
		d.resolver.cancel()
	}
}

func (d *doors) listen() error {
	for _, ln := range d.listeners() {
		if ln == nil {
			continue
		}
		if err := listenBound(ln); err != nil {
			return err
		}
	}
	return nil
}

func (d *doors) listeners() [4]net.Listener {
	return [...]net.Listener{d.ln, d.socks, d.dns, d.dnsGram}
}

func (d *doors) serves(p egress.PolicyEvaluator) bool {
	return (d.socks != nil) == p.ServesSocks() && (d.dns != nil) == p.ServesDNS()
}

func (d *doors) specs(vsock string, p egress.PolicyEvaluator) [4]doorSpec {
	return [...]doorSpec{
		{&d.ln, engine.DoorPath(vsock, engine.EgressPort), "egress", true},
		{&d.socks, engine.DoorPath(vsock, engine.SocksPort), "socks", p.ServesSocks()},
		{&d.dns, engine.DoorPath(vsock, engine.DNSPort), "dns", p.ServesDNS()},
		{&d.dnsGram, engine.DoorPath(vsock, engine.DNSGramPort), "dns datagram", p.ServesDNS()},
	}
}

func (d *doors) takeTun() (t *tunDoor) {
	if d != nil {
		t, d.tun = d.tun, nil
	}
	return t
}

// doorListener caps a door's live connections; doorConn's NetConn hands splice the *net.UnixConn the kernel copy needs.
type doorListener struct {
	*net.UnixListener
	slots chan struct{}
}

func limitDoor(ln *net.UnixListener) *doorListener {
	return &doorListener{UnixListener: ln, slots: make(chan struct{}, doorConns)}
}

func (l *doorListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &doorConn{UnixConn: conn, slots: l.slots}, nil
		default:
			_ = conn.Close()
		}
	}
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

func bindDoors(vsock string, p egress.PolicyEvaluator, bind unixBinder) (*doors, error) {
	d := &doors{}
	for _, door := range d.specs(vsock, p) {
		if !door.on {
			continue
		}
		ln, err := bind(door.path)
		if err != nil {
			d.close()
			return nil, fmt.Errorf("listen %s: %w", door.name, err)
		}
		*door.ln = limitDoor(ln)
	}
	return d, nil
}

func listenBound(ln net.Listener) error {
	rc, err := ln.(syscall.Conn).SyscallConn()
	if err != nil {
		return err
	}
	if ctlErr := rc.Control(func(fd uintptr) { err = syscall.Listen(int(fd), doorBacklog) }); ctlErr != nil {
		return ctlErr
	}
	if err != nil {
		return os.NewSyscallError("listen", err)
	}
	return nil
}

// listenUnix binds path after clearing a stale socket from a prior life that would block the bind.
func listenUnix(path string) (*net.UnixListener, error) {
	_ = os.Remove(path)
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// bindUnix binds path without listening, so a guest dial is refused until the door opens.
func bindUnix(path string) (*net.UnixListener, error) {
	_ = os.Remove(path)
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer func() { _ = f.Close() }()
	if err = syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		return nil, os.NewSyscallError("bind", err)
	}
	ln, err := net.FileListener(f)
	if err != nil {
		return nil, err
	}
	ul := ln.(*net.UnixListener)
	ul.SetUnlinkOnClose(true)
	return ul, nil
}
