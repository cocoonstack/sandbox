package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

func TestPtyEchoAndExit(t *testing.T) {
	sb := fakeSandbox(t)
	pty, err := sb.OpenPty(t.Context(), PtyOpts{Cols: 80, Rows: 24})
	if err != nil {
		t.Fatalf("OpenPty: %v", err)
	}
	if pty.PID == 0 {
		t.Fatal("no pid")
	}
	defer pty.Close()

	if _, err = pty.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(pty).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "ping\n" {
		t.Errorf("read %q, want ping", line)
	}

	if err := pty.Resize(t.Context(), 120, 40); err != nil {
		t.Fatalf("resize: %v", err)
	}

	if _, err := pty.Write([]byte("exit\n")); err != nil {
		t.Fatalf("write exit: %v", err)
	}
	if _, err := io.ReadAll(pty); err != nil {
		t.Fatalf("drain to EOF: %v", err)
	}
	code, ok := pty.ExitCode()
	if !ok || code != 0 {
		t.Errorf("exit code %d ok=%v, want 0 true", code, ok)
	}
}

func TestPtyCtxCancelIsTyped(t *testing.T) {
	sb := fakeSandbox(t)
	ctx, cancel := context.WithCancel(t.Context())
	pty, err := sb.OpenPty(ctx, PtyOpts{Cols: 80, Rows: 24})
	if err != nil {
		t.Fatalf("OpenPty: %v", err)
	}
	defer pty.Close()

	cancel()
	_, err = io.ReadAll(pty)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Read after ctx cancel = %v, want context.Canceled", err)
	}
}

func TestPtyCloseUnblocksUnreadOutput(t *testing.T) {
	sb := fakeSandbox(t)
	pty, err := sb.OpenPty(t.Context(), PtyOpts{Cols: 80, Rows: 24})
	if err != nil {
		t.Fatalf("OpenPty: %v", err)
	}
	if _, err = pty.Write([]byte("unread\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	stops := 0
	stop := pty.stop
	pty.stop = func() { stops++; stop() }

	if err = pty.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = pty.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err = pty.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Read after Close = %v, want io.ErrClosedPipe", err)
	}
	if stops != 1 {
		t.Errorf("stop called %d times, want 1", stops)
	}
}

func TestPtyWriteChunksOversizedInput(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	p := &Pty{conn: silkd.NewConn(client)}
	payload := bytes.Repeat([]byte("x"), stdinChunk*2+1)

	errc := make(chan error, 1)
	go func() {
		n, err := p.Write(payload)
		if err == nil && n != len(payload) {
			err = fmt.Errorf("Write returned %d, want %d", n, len(payload))
		}
		errc <- err
	}()

	sc := wire.NewFrameScanner(server)
	var got []byte
	for len(got) < len(payload) {
		if !sc.Scan() {
			t.Fatalf("scan stdin frame: %v", sc.Err())
		}
		if len(sc.Bytes()) > wire.MaxFrame {
			t.Fatalf("frame of %d bytes exceeds the %d cap", len(sc.Bytes()), wire.MaxFrame)
		}
		req, err := wire.DecodeRequest(sc.Bytes())
		if err != nil {
			t.Fatalf("decode stdin frame: %v", err)
		}
		in, ok := req.(*wire.Stdin)
		if !ok {
			t.Fatalf("frame %q, want stdin", req.Op())
		}
		if len(in.Data) > stdinChunk {
			t.Fatalf("stdin frame carries %d bytes, want at most %d", len(in.Data), stdinChunk)
		}
		got = append(got, in.Data...)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("reassembled %d bytes, want the %d written", len(got), len(payload))
	}
}
