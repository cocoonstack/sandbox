package sandbox

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

func TestDialPortEchoAndHalfClose(t *testing.T) {
	sb := fakeSandbox(t)
	pc, err := sb.DialPort(t.Context(), 8080)
	if err != nil {
		t.Fatalf("DialPort: %v", err)
	}
	defer pc.Close()

	if _, err = pc.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(pc, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("read %q, %v — want the echo", buf, err)
	}

	if err := pc.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	if _, err := pc.Read(buf); !errors.Is(err, io.EOF) {
		t.Errorf("read after close: %v, want EOF", err)
	}
}

func TestDialPortRefusedPort(t *testing.T) {
	sb := fakeSandbox(t)
	if _, err := sb.DialPort(t.Context(), 1); err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Errorf("got %v, want the not_found error", err)
	}
}

func TestDialPortCtxCancelUnblocksUnreadOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = server.Close() }()
		defer func() { _ = client.Close() }()
		pr, pw := io.Pipe()
		defer func() { _ = pr.Close() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		p := &PortConn{conn: silkd.NewConn(client)}
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			p.drain(ctx, pw)
		}()
		if _, err := io.WriteString(server, `{"type":"data","data":"aGk="}`+"\n"); err != nil {
			t.Fatalf("send output: %v", err)
		}
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case <-exited:
		default:
			t.Fatal("drain still parked on the unread output after ctx cancel")
		}
		if _, err := pr.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
			t.Errorf("Read after ctx cancel = %v, want context.Canceled", err)
		}
	})
}

func TestProxyPortPipesLocalConnections(t *testing.T) {
	sb := fakeSandbox(t)
	l, err := sb.ProxyPort(t.Context(), "127.0.0.1:0", 8080)
	if err != nil {
		t.Fatalf("ProxyPort: %v", err)
	}
	defer l.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Errorf("proxied read %q, %v — want the echo", buf, err)
	}
}
