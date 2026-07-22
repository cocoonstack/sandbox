package engine

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	infoFrame = `{"type":"info","version":"test","proto":1,"uptime_secs":0,"procs":0,"sessions":0}` + "\n"
	errFrame  = `{"type":"error","kind":"internal","message":"boom"}` + "\n"
)

func TestDialSilkdConsumesOnlyHandshake(t *testing.T) {
	path := sockPath(t)
	// Sentinel rides in the same write as the handshake: if DialSilkd
	// over-reads past the newline, the first Read below loses it.
	listenMuxer(t, path, "OK 2048\nX")

	conn, err := New("cocoon", "", "", false, "").DialSilkd(t.Context(), path)
	if err != nil {
		t.Fatalf("DialSilkd: %v", err)
	}
	defer conn.Close()
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read sentinel: %v", err)
	}
	if buf[0] != 'X' {
		t.Errorf("got %q, want %q", buf[0], byte('X'))
	}
}

func TestDialSilkdRejectedHandshake(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "ERR no guest listener\n")

	_, err := New("cocoon", "", "", false, "").DialSilkd(t.Context(), path)
	if err == nil || !strings.Contains(err.Error(), "no guest listener") {
		t.Errorf("got %v, want handshake rejection", err)
	}
}

func TestProbeSucceeds(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", infoFrame)

	if err := New("cocoon", "", "", false, "").Probe(t.Context(), path, 2*time.Second); err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestInfoRoundTripRejectsErrorFrame(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", errFrame)

	err := New("cocoon", "", "", false, "").infoRoundTrip(t.Context(), path)
	if err == nil || !strings.Contains(err.Error(), `info reply type "error"`) {
		t.Errorf("got %v, want error-frame rejection", err)
	}
}

func TestProbeRetriesPastFailures(t *testing.T) {
	path := sockPath(t)
	listenMuxer(t, path, "OK 2048\n", errFrame, errFrame, infoFrame)

	if err := New("cocoon", "", "", false, "").Probe(t.Context(), path, 2*time.Second); err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestProbeRetriesUntilListenerAppears(t *testing.T) {
	path := sockPath(t)
	done := make(chan error, 1)
	go func() {
		done <- New("cocoon", "", "", false, "").Probe(t.Context(), path, 2*time.Second)
	}()

	time.Sleep(60 * time.Millisecond)
	listenMuxer(t, path, "OK 2048\n", infoFrame)
	if err := <-done; err != nil {
		t.Errorf("Probe: %v", err)
	}
}

func TestDialGuestPortCtxCancel(t *testing.T) {
	path := sockPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("setup listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
		if _, err := io.WriteString(conn, "OK 2048\n"); err != nil {
			return
		}
		_, _ = r.ReadString('\n') // consume port_forward, never answer
		<-hold
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if _, err := New("cocoon", "", "", false, "").DialGuestPort(ctx, path, 8080); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestProbeTimeout(t *testing.T) {
	path := sockPath(t)

	err := New("cocoon", "", "", false, "").Probe(t.Context(), path, 150*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "silkd probe") {
		t.Errorf("got %v, want probe timeout", err)
	}
}

// listenMuxer fakes the hybrid-vsock muxer on a UDS: per connection it
// consumes the CONNECT line, replies with handshake, then answers the first
// frame line of connection i with frames[i] (last frame repeats; no frames
// means handshake only).
func listenMuxer(t *testing.T, path, handshake string, frames ...string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("setup listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for i := 0; ; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			frame := ""
			if len(frames) > 0 {
				frame = frames[min(i, len(frames)-1)]
			}
			go serveConn(conn, handshake, frame)
		}
	}()
}

func serveConn(conn net.Conn, handshake, frame string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	if _, err := r.ReadString('\n'); err != nil {
		return
	}
	if _, err := io.WriteString(conn, handshake); err != nil || frame == "" {
		return
	}
	if _, err := r.ReadString('\n'); err != nil {
		return
	}
	_, _ = io.WriteString(conn, frame)
}

// sockPath returns a socket path in a fresh short-prefix temp dir —
// t.TempDir() paths can exceed darwin's 104-byte sun_path limit.
func sockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sbx")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "mux.sock")
}
