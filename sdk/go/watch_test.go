package sandbox

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestWatchDeliversEventsUntilClose(t *testing.T) {
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
	sb := legacySandbox(t, ts)
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

func TestWatchMissingPathFailsSynchronously(t *testing.T) {
	sb := fakeSandbox(t)
	_, err := sb.Watch(t.Context(), "/nope", false)
	e, ok := errors.AsType[*wire.ErrorResp](err)
	if !ok || e.Kind != wire.KindNotFound {
		t.Errorf("Watch = %v, want synchronous not_found (no ready frame)", err)
	}
}
