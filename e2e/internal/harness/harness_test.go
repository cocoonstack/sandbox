package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPortRelayPreservesGreetingAndHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	served := make(chan error, 1)
	go func() { served <- serveGuestFirstHalfClose(listener) }()

	relay := PortRelay{Owner: listener.Addr().String(), ID: "sb_1", Token: "tok"}
	conn, err := relay.Dial(t.Context(), 49983)
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var greeting strings.Builder
	if _, err := io.Copy(&greeting, conn); err != nil {
		t.Fatalf("read greeting: %v", err)
	}
	if got := greeting.String(); got != "hello" {
		t.Errorf("greeting %q, want hello", got)
	}
	if _, err := io.WriteString(conn, "tail"); err != nil {
		t.Fatalf("write after guest EOF: %v", err)
	}
	cw, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("relay connection does not support CloseWrite")
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

func TestPoll(t *testing.T) {
	errTry := errors.New("not yet")

	calls := 0
	if err := Poll(t.Context(), time.Second, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errTry
		}
		return nil
	}); err != nil || calls != 3 {
		t.Errorf("success: err %v calls %d, want nil and 3", err, calls)
	}

	if err := Poll(t.Context(), 5*time.Millisecond, time.Millisecond, func() error { return errTry }); !errors.Is(err, errTry) {
		t.Errorf("exhausted: err %v, want the last try error", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Poll(ctx, time.Minute, time.Minute, func() error { return errTry }); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled: err %v, want context.Canceled", err)
	}
}

func serveGuestFirstHalfClose(listener net.Listener) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadlineErr := conn.SetDeadline(time.Now().Add(3 * time.Second)); deadlineErr != nil {
		return deadlineErr
	}
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}
	_ = req.Body.Close()
	if _, writeErr := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\nhello"); writeErr != nil {
		return writeErr
	}
	if closeErr := conn.(*net.TCPConn).CloseWrite(); closeErr != nil {
		return closeErr
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if string(tail) != "tail" {
		return fmt.Errorf("client tail %q, want tail", tail)
	}
	return nil
}
