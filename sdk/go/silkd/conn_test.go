package silkd_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

// TestSendBulkMatchesJSONEncoding pins the hand-built data/stdin envelope to
// byte-for-byte identity with the json.Marshal path — the fast path must not
// drift from the wire contract silkd parses.
func TestSendBulkMatchesJSONEncoding(t *testing.T) {
	for _, req := range []silkd.Request{
		&silkd.Data{Data: []byte("hello\x00\xff binary")},
		&silkd.Stdin{Data: []byte{}},
		&silkd.Data{Data: bytes.Repeat([]byte{0xAB}, 300*1024)},
	} {
		want, err := silkd.EncodeRequest(req)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		want = append(want, '\n')
		var buf bufferConn
		if err := silkd.NewConn(&buf).Send(req); err != nil {
			t.Fatalf("send: %v", err)
		}
		if !bytes.Equal(buf.Bytes(), want) {
			t.Errorf("%s: fast frame != json frame\n got %q\nwant %q", req.Op(), buf.Bytes(), want)
		}
	}
}

type bufferConn struct{ bytes.Buffer }

func (bufferConn) Close() error { return nil }

func TestConnInfoRoundTrip(t *testing.T) {
	conn := dialFake(t)
	if err := conn.Send(silkd.Info{}); err != nil {
		t.Fatalf("send: %v", err)
	}
	resp, err := conn.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	info, ok := resp.(*silkd.InfoResp)
	if !ok || info.Version != "silkdtest" {
		t.Errorf("got %#v, want silkdtest info", resp)
	}
	if _, err := conn.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("got %v after terminal frame, want EOF", err)
	}
}

func TestConnExecStdinEcho(t *testing.T) {
	conn := dialFake(t)
	if err := conn.Send(&silkd.Exec{Argv: []string{"cat"}}); err != nil {
		t.Fatalf("send exec: %v", err)
	}
	if resp, err := conn.Recv(); err != nil {
		t.Fatalf("recv started: %v", err)
	} else if _, ok := resp.(*silkd.Started); !ok {
		t.Fatalf("got %#v, want started", resp)
	}
	if err := conn.Send(&silkd.Stdin{Data: []byte("ping")}); err != nil {
		t.Fatalf("send stdin: %v", err)
	}
	if resp, err := conn.Recv(); err != nil {
		t.Fatalf("recv echo: %v", err)
	} else if out, ok := resp.(*silkd.Stdout); !ok || string(out.Data) != "ping" {
		t.Fatalf("got %#v, want stdout ping", resp)
	}
	if err := conn.Send(silkd.StdinClose{}); err != nil {
		t.Fatalf("send stdin_close: %v", err)
	}
	if resp, err := conn.Recv(); err != nil {
		t.Fatalf("recv exit: %v", err)
	} else if exit, ok := resp.(*silkd.Exit); !ok || exit.Code != 0 {
		t.Fatalf("got %#v, want exit 0", resp)
	}
}

func TestConnRejectsOversizedFrame(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	conn := silkd.NewConn(client)
	go func() {
		huge := strings.Repeat("a", silkd.MaxFrame+2)
		_, _ = io.WriteString(server, huge+"\n")
	}()

	if _, err := conn.Recv(); err == nil {
		t.Error("oversized frame accepted")
	}
}

func dialFake(t *testing.T) *silkd.Conn {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go silkdtest.Serve(l)
	raw, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn := silkd.NewConn(raw)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
