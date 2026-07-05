package sandbox

import (
	"errors"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sdk/silkd"
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
	for range w.Events() {
	}
	if err := w.Err(); err != nil {
		t.Errorf("Err after clean close: %v", err)
	}
}

func TestWatchMissingPathSurfacesError(t *testing.T) {
	sb := fakeSandbox(t)
	w, err := sb.Watch(t.Context(), "/nope", false)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = w.Close() }()

	if ev, ok := <-w.Events(); ok {
		t.Fatalf("got event %+v from a missing path", ev)
	}
	var e *silkd.ErrorResp
	if !errors.As(w.Err(), &e) || e.Kind != silkd.KindNotFound {
		t.Errorf("Err = %v, want not_found error frame", w.Err())
	}
}
