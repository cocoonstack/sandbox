package sandbox

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"testing"
	"testing/iotest"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

var errLimit = errors.New("limit")

func TestFilesRoundTrip(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := sb.WriteFile(ctx, "/work/a.txt", []byte("silk body"), nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := sb.ReadFile(ctx, "/work/a.txt")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "silk body" {
		t.Errorf("read %q, want silk body", got)
	}

	info, err := sb.Stat(ctx, "/work/a.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Kind != "file" || info.Size != 9 {
		t.Errorf("stat %+v", info)
	}

	entries, err := sb.ListDir(ctx, "/work")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "a.txt" {
		t.Errorf("list %+v", entries)
	}

	if err := sb.Rename(ctx, "/work/a.txt", "/work/b.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := sb.Remove(ctx, "/work/b.txt", false); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := sb.ReadFile(ctx, "/work/b.txt"); err == nil {
		t.Error("read of removed file succeeded")
	}
}

func TestAnUploadFillsEachFrameFromSmallReads(t *testing.T) {
	frames := make(chan int, 1)
	sb := testSandbox(t, newAgentServer(t, func(conn net.Conn) {
		defer conn.Close()
		sc := wire.NewFrameScanner(conn)
		data := 0
		for sc.Scan() {
			req, err := wire.DecodeRequest(sc.Bytes())
			if err != nil {
				return
			}
			var resp wire.Response
			switch req.(type) {
			case *wire.Info:
				resp = &wire.InfoResp{Proto: wire.KeepAliveProto}
			case *wire.Data:
				data++
			case *wire.DataEnd:
				frames <- data
				resp = wire.Done{}
			}
			if resp != nil {
				line, _ := wire.EncodeResponse(resp)
				_, _ = conn.Write(append(line, '\n'))
			}
		}
	}))
	if err := sb.Push(t.Context(), "/d", iotest.OneByteReader(bytes.NewReader(make([]byte, 4096)))); err != nil {
		t.Fatalf("push: %v", err)
	}
	if got := <-frames; got != 1 {
		t.Errorf("a 4 KiB upload read a byte at a time went out in %d data frames, want 1", got)
	}
}

func TestReadFileToStopsWhenTheWriterRefuses(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()
	body := bytes.Repeat([]byte("x"), 3*wire.BulkChunk)
	if err := sb.WriteFile(ctx, "/big", body, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	var whole bytes.Buffer
	if err := sb.ReadFileTo(ctx, "/big", &whole); err != nil || whole.Len() != len(body) {
		t.Fatalf("ReadFileTo: %d bytes, %v; want the whole file", whole.Len(), err)
	}
	limit := &limitWriter{max: wire.BulkChunk}
	if err := sb.ReadFileTo(ctx, "/big", limit); !errors.Is(err, errLimit) {
		t.Fatalf("ReadFileTo past the limit = %v, want the writer's error", err)
	}
}

func TestReadMissingFileErrors(t *testing.T) {
	sb := fakeSandbox(t)
	if _, err := sb.ReadFile(t.Context(), "/nope"); err == nil {
		t.Error("read of missing file succeeded")
	}
}

func TestSessionLifecycle(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()

	sess, err := sb.NewSession(ctx, WithSessionCwd("/work"))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	if sess.ID == "" {
		t.Fatal("empty session id")
	}
	ids, err := sb.Sessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(ids) != 1 || ids[0] != sess.ID {
		t.Errorf("sessions %v, want [%s]", ids, sess.ID)
	}

	out, err := sess.Exec(ctx, "echo", "hi")
	if err != nil {
		t.Fatalf("session exec: %v", err)
	}
	if out != "hi\n" {
		t.Errorf("exec %q, want hi", out)
	}
	if err := sess.Close(ctx); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if err := sess.Close(ctx); err == nil {
		t.Error("second close succeeded")
	}
}

func TestReadFileMultiChunk(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()
	want := bytes.Repeat([]byte("abcdefgh"), 80*1024)
	if err := sb.WriteFile(ctx, "/big.bin", want, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := sb.ReadFile(ctx, "/big.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("read %d bytes, want %d", len(got), len(want))
	}
}

func TestUploadToAnOldDaemonReturnsItsRejection(t *testing.T) {
	sb := testSandbox(t, newAgentServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			return
		}
		req, err := wire.DecodeRequest(line)
		if err != nil {
			return
		}
		var resp wire.Response = &wire.ErrorResp{Kind: wire.KindNotFound, Message: "create /missing/f: no such file or directory"}
		if _, ok := req.(*wire.Info); ok {
			resp = &wire.InfoResp{Proto: 1}
		}
		frame, _ := wire.EncodeResponse(resp)
		_, _ = conn.Write(append(frame, '\n'))
	}))
	err := sb.WriteFile(t.Context(), "/missing/f", make([]byte, 64<<20), nil)
	if e, ok := errors.AsType[*wire.ErrorResp](err); !ok || e.Kind != wire.KindNotFound {
		t.Fatalf("upload rejected by a proto 1 daemon: %v, want its not_found frame", err)
	}
}

func fakeSandbox(t testing.TB) *Sandbox {
	t.Helper()
	fake := silkdtest.NewFake(t.TempDir())
	return testSandbox(t, newAgentServer(t, fake.ServeConn))
}

type limitWriter struct {
	n, max int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.max {
		return 0, errLimit
	}
	w.n += len(p)
	return len(p), nil
}
