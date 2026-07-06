package sandbox

import (
	"io"
	"net"
	"strings"
	"testing"
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
	// Half-close: the fake's server closes, surfacing as EOF.
	if err := pc.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	if _, err := pc.Read(buf); err != io.EOF {
		t.Errorf("read after close: %v, want EOF", err)
	}
}

func TestDialPortRefusedPort(t *testing.T) {
	sb := fakeSandbox(t)
	if _, err := sb.DialPort(t.Context(), 1); err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Errorf("got %v, want the not_found error", err)
	}
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
