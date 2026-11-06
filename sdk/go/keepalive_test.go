package sandbox

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

func TestCallsShareOneConnection(t *testing.T) {
	sb, upgrades := countingSandbox(t, silkdtest.NewFake(t.TempDir()).ServeConn)
	for range 3 {
		if _, err := sb.Stat(t.Context(), "/"); err != nil {
			t.Fatalf("stat: %v", err)
		}
	}
	if out, err := sb.Exec(t.Context(), "echo", "42"); err != nil || out != "42\n" {
		t.Fatalf("exec: %q, %v", out, err)
	}
	if err := sb.WriteFile(t.Context(), "/f", []byte("hi"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := sb.ReadFile(t.Context(), "/f"); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := upgrades.Load(); got != 1 {
		t.Errorf("upgrades = %d, want 1", got)
	}
}

func TestRefusedStreamOpenKeepsItsConnection(t *testing.T) {
	sb, upgrades := countingSandbox(t, silkdtest.NewFake(t.TempDir()).ServeConn)
	for range 3 {
		if _, err := sb.Watch(t.Context(), "/missing", false); err == nil {
			t.Fatal("watch on a missing path succeeded")
		}
	}
	if got := upgrades.Load(); got != 1 {
		t.Errorf("upgrades = %d, want 1: an error frame ends the RPC, not the connection", got)
	}
}

func TestCanceledCallSendsNothingOnAParkedConnection(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, silkdtest.NewFake(t.TempDir()).ServeConn))
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sb.Mkdir(ctx, "/d", false); !errors.Is(err, context.Canceled) {
		t.Fatalf("mkdir on a canceled ctx: %v, want context.Canceled", err)
	}
	if _, err := sb.Stat(t.Context(), "/d"); err == nil {
		t.Fatal("a canceled mkdir still ran in the guest")
	}
}

func TestKeepAliveOffDialsPerCall(t *testing.T) {
	sb, upgrades := countingSandbox(t, silkdtest.ServeConn, WithKeepAlive(0))
	for range 3 {
		if _, err := sb.Exec(t.Context(), "echo", "42"); err != nil {
			t.Fatalf("exec: %v", err)
		}
	}
	if got := upgrades.Load(); got != 3 {
		t.Errorf("upgrades = %d, want 3", got)
	}
}

func TestIdleConnectionCloses(t *testing.T) {
	served := make(chan struct{}, 1)
	fake := silkdtest.NewFake(t.TempDir())
	sb := testSandbox(t, newAgentServer(t, func(c net.Conn) {
		fake.ServeConn(c)
		served <- struct{}{}
	}), WithKeepAlive(50*time.Millisecond))
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("idle connection still open after the keep-alive window")
	}
}

func TestStalledStdinKeepsItsConnectionOut(t *testing.T) {
	sb, upgrades := countingSandbox(t, silkdtest.ServeConn)
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })
	if code, err := sb.Run(t.Context(), Cmd{Argv: []string{"echo", "hi"}, Stdin: pr}); err != nil || code != 0 {
		t.Fatalf("run: %d, %v", code, err)
	}
	if _, err := sb.Exec(t.Context(), "echo", "42"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if got := upgrades.Load(); got != 2 {
		t.Errorf("upgrades = %d, want 2", got)
	}
}

func TestPeerHangUpIsNoticedBeforeReuse(t *testing.T) {
	if !canProbe {
		t.Skip("no parked-connection probe on this platform")
	}
	gone := make(chan struct{}, 2)
	fake := silkdtest.NewFake(t.TempDir())
	sb, upgrades := countingSandbox(t, func(c net.Conn) {
		fake.ServeConn(&hangUpAfterReply{Conn: c})
		gone <- struct{}{}
	})
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	<-gone
	if _, err := sb.Stat(t.Context(), "/"); err != nil {
		t.Fatalf("stat after the peer hung up: %v", err)
	}
	if got := upgrades.Load(); got != 2 {
		t.Errorf("upgrades = %d, want 2", got)
	}
}

func TestPeerQuiet(t *testing.T) {
	if !canProbe {
		t.Skip("no parked-connection probe on this platform")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		if c, acceptErr := l.Accept(); acceptErr == nil {
			accepted <- c
		}
	}()
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	server := <-accepted
	probe := newAgentConn(&upgradedConn{Conn: client, tcp: client, r: bufio.NewReader(client)})
	if !probe.quiet() {
		t.Error("a silent peer reported as gone")
	}
	if _, err := server.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitUntil(t, func() bool { return !probe.quiet() }, "unprompted byte not noticed")
	var b [1]byte
	if n, err := client.Read(b[:]); err != nil || n != 1 || b[0] != 'x' {
		t.Errorf("read after the probe = %q, %v; want the peeked byte intact", b[:n], err)
	}
	_ = server.Close()
	waitUntil(t, func() bool { return !probe.quiet() }, "hang-up not noticed")
}

func countingSandbox(t *testing.T, serve func(net.Conn), opts ...ClientOption) (*Sandbox, *atomic.Int32) {
	t.Helper()
	var upgrades atomic.Int32
	sb := testSandbox(t, newAgentServer(t, func(c net.Conn) {
		upgrades.Add(1)
		serve(c)
	}), opts...)
	return sb, &upgrades
}

func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	for start := time.Now(); time.Since(start) < 3*time.Second; time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal(msg)
}

type hangUpAfterReply struct {
	net.Conn
	replied atomic.Bool
}

func (c *hangUpAfterReply) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if c.replied.CompareAndSwap(false, true) {
		_ = c.Close()
	}
	return n, err
}
