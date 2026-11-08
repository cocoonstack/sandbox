//go:build linux

package egress

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

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
		{"lifted", types.EgressLimit{}, maxScaledWindow},
		{"low cap", types.EgressLimit{RxBytesPerSecond: 64 << 10}, minWindow},
		{"tx only", types.EgressLimit{TxBytesPerSecond: 64 << 10}, maxScaledWindow},
	} {
		p.SetLimit(tt.limit)
		read()
		if got := windowClamp(t, conn); got != tt.want {
			t.Errorf("%s: window clamp %d, want %d", tt.name, got, tt.want)
		}
	}

	p.SetLimit(types.EgressLimit{RxBytesPerSecond: 64 << 10})
	read()
	_ = conn.SetReadDeadline(time.Unix(1, 0))
	if _, readErr := m.Read(buf); !errors.Is(readErr, os.ErrDeadlineExceeded) {
		t.Fatalf("read past a kick: %v", readErr)
	}
	if got := windowClamp(t, conn); got != minWindow {
		t.Errorf("a kick reopened the window to %d, want it held at %d", got, minWindow)
	}
	_ = conn.SetReadDeadline(time.Time{})
	_ = server.Close()
	for {
		if _, readErr := m.Read(buf); readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				t.Fatalf("read: %v", readErr)
			}
			break
		}
	}
	if got := windowClamp(t, conn); got != maxScaledWindow {
		t.Errorf("a conn that ended limited keeps window clamp %d, want it open at %d", got, maxScaledWindow)
	}
}

func TestAGuestHangingUpOnAScrubbedTailLeavesThePooledConnOpen(t *testing.T) {
	const size = 16000
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tail" {
			_, _ = io.WriteString(w, "hello")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write(bytes.Repeat([]byte("x"), size))
	}))
	upstream.TLS = &tls.Config{MinVersion: tls.VersionTLS12, DynamicRecordSizingDisabled: true}
	upstream.StartTLS()
	t.Cleanup(upstream.Close)
	dialed := make(chan net.Conn, 4)
	dial := DialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, network, upstream.Listener.Addr().String())
		if err == nil {
			dialed <- c
		}
		return c, err
	})
	ca, _ := testCA(t)
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "SERP", Query: "api_key", Value: "real", Placeholder: "@SERP@"}}}}
	p := New(Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, secrets, ca, dial, nil, nil)
	received := make(chan int64, 4)
	p.OnTransfer(func(_ Event, _, n int64) { received <- n })
	front := serveTrusting(t, p, upstream)
	guestRoots := rootsFor(t, ca)
	p.SetLimit(types.EgressLimit{RxBytesPerSecond: 10 << 10})
	p.limits.Load().rx.AllowN(time.Now(), limitBurst)

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	req := newRequest(t, http.MethodGet, "/tail?api_key=%40SERP%40", "")
	if err := req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err = io.ReadFull(resp.Body, make([]byte, 1)); err != nil {
		t.Fatalf("read the first body byte: %v", err)
	}
	_ = tc.NetConn().(*net.TCPConn).SetLinger(0)
	_ = tc.NetConn().Close()
	select {
	case n := <-received:
		if n >= size {
			t.Fatalf("the relay moved all %d bytes, want the hang-up to cut it short", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the relay never finished after the guest hung up")
	}
	conn := <-dialed

	p.SetLimit(types.EgressLimit{})
	again := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	t.Cleanup(func() { _ = again.Close() })
	next := roundTripTLS(t, again, http.MethodGet)
	if body, readErr := io.ReadAll(next.Body); readErr != nil || string(body) != "hello" {
		t.Fatalf("second request body %q, %v", body, readErr)
	}
	_ = next.Body.Close()
	select {
	case <-dialed:
		t.Fatal("the second request dialed a new conn, want it to reuse the pooled one")
	default:
	}
	if got := windowClamp(t, conn); got != maxScaledWindow {
		t.Errorf("the pooled conn serves an unlimited request with window clamp %d, want it open at %d", got, maxScaledWindow)
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
		v, getErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_WINDOW_CLAMP)
	}); err != nil || getErr != nil {
		t.Fatalf("get window clamp: %v %v", err, getErr)
	}
	return v
}
