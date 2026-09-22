package harness

import (
	"bufio"
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

func serveGuestFirstHalfClose(listener net.Listener) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}
	_ = req.Body.Close()
	if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\nhello"); err != nil {
		return err
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		return err
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
