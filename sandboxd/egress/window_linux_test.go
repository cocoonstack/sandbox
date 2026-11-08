//go:build linux

package egress

import (
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

func TestALimitedDownloadClampsTheUpstreamWindowAndOpensItAgain(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	served := make(chan net.Conn, 1)
	go func() {
		c, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		served <- c
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	server := <-served
	t.Cleanup(func() { _ = server.Close() })
	go func() { _, _ = server.Write(make([]byte, 256<<10)) }()

	p := New(Policy{}, nil, nil, fixedDial(""), nil, nil)
	m := &meter{r: conn, p: p, rx: true, n: new(atomic.Int64), win: tcpOf(conn)}
	buf := make([]byte, 32<<10)
	read := func() {
		t.Helper()
		if _, readErr := m.Read(buf); readErr != nil {
			t.Fatalf("read: %v", readErr)
		}
	}
	for _, tt := range []struct {
		name  string
		limit types.EgressLimit
		want  int
	}{
		{"limited", types.EgressLimit{RxBytesPerSecond: 1 << 20}, 512 << 10},
		{"lifted", types.EgressLimit{}, openWindow},
		{"low cap", types.EgressLimit{RxBytesPerSecond: 64 << 10}, minWindow},
		{"tx only", types.EgressLimit{TxBytesPerSecond: 64 << 10}, openWindow},
	} {
		p.SetLimit(tt.limit)
		read()
		if got := windowClamp(t, conn); got != tt.want {
			t.Errorf("%s: window clamp %d, want %d", tt.name, got, tt.want)
		}
	}

	p.SetLimit(types.EgressLimit{RxBytesPerSecond: 64 << 10})
	read()
	_ = server.Close()
	for {
		if _, readErr := m.Read(buf); readErr == io.EOF {
			break
		}
	}
	if got := windowClamp(t, conn); got != openWindow {
		t.Errorf("a conn that ended limited keeps window clamp %d, want it open at %d", got, openWindow)
	}
}

func windowClamp(t *testing.T, c net.Conn) int {
	t.Helper()
	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatalf("syscall conn: %v", err)
	}
	var v int
	var getErr error
	if err = raw.Control(func(fd uintptr) {
		v, getErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP)
	}); err != nil || getErr != nil {
		t.Fatalf("get window clamp: %v %v", err, getErr)
	}
	return v
}
