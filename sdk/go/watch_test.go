package sandbox

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

func TestWatchRelaysTheFakesSyntheticEventAndClosesCleanly(t *testing.T) {
	sb := fakeSandbox(t)
	ctx := t.Context()
	if err := sb.Mkdir(ctx, "/work", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	w, err := sb.Watch(ctx, "/work", true)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	ev, ok := <-w.Events()
	if !ok {
		t.Fatalf("event stream ended early: %v", w.Err())
	}
	if ev.Kind != "created" || !strings.Contains(ev.Path, "/work") {
		t.Errorf("event %+v, want created under /work", ev)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for ev := range w.Events() {
		t.Logf("drained %+v", ev)
	}
	if err := w.Err(); err != nil {
		t.Errorf("Err after clean close: %v", err)
	}
}

func TestWatchReleasesTheRelayWhenTheSandboxEnds(t *testing.T) {
	released := make(chan struct{})
	ts := newAgentServer(t, func(conn net.Conn) {
		defer conn.Close()
		r := bufio.NewReader(conn)
		if _, err := r.ReadString('\n'); err != nil {
			t.Errorf("read watch request: %v", err)
			return
		}
		_, _ = io.WriteString(conn, `{"type":"ready"}`+"\n"+
			`{"type":"event","kind":"created","path":"/work/a"}`+"\n"+
			`{"type":"error","kind":"internal","message":"watcher died"}`+"\n")
		_, _ = r.ReadByte()
		close(released)
	})
	sb := testSandbox(t, ts, WithKeepAlive(0))
	w, err := sb.Watch(t.Context(), "/work", true)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	for ev := range w.Events() {
		t.Logf("drained %+v", ev)
	}
	if e, ok := errors.AsType[*wire.ErrorResp](w.Err()); !ok || e.Message != "watcher died" {
		t.Fatalf("Err = %v, want the sandbox's error frame", w.Err())
	}
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("relay connection still open after the watch ended on its own")
	}
}

func TestWatchCtxCancelUnblocksUnreadEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = server.Close() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		w := &Watcher{events: make(chan wire.Event), stop: func() { _ = client.Close() }, closed: make(chan struct{})}
		defer w.Close()
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			w.drain(ctx, silkd.NewConn(client))
		}()
		if _, err := io.WriteString(server, `{"type":"event","kind":"created","path":"/work/a"}`+"\n"); err != nil {
			t.Fatalf("send event: %v", err)
		}
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case <-exited:
		default:
			t.Fatal("drain still parked on the unread event after ctx cancel")
		}
		if err := w.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("Err after ctx cancel = %v, want context.Canceled", err)
		}
	})
}

func TestWatchMissingPathFailsSynchronously(t *testing.T) {
	sb := fakeSandbox(t)
	_, err := sb.Watch(t.Context(), "/nope", false)
	e, ok := errors.AsType[*wire.ErrorResp](err)
	if !ok || e.Kind != wire.KindNotFound {
		t.Errorf("Watch = %v, want synchronous not_found (no ready frame)", err)
	}
}
