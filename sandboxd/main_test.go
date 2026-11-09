package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

const previewChildAddr = "SANDBOXD_TEST_PREVIEW_ADDR"

func TestServePreviewExitsWhenTheListenerCannotBind(t *testing.T) {
	if addr := os.Getenv(previewChildAddr); addr != "" {
		servePreview(t.Context(), &http.Server{Addr: addr})
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestServePreviewExitsWhenTheListenerCannotBind$")
	cmd.Env = append(os.Environ(), previewChildAddr+"="+ln.Addr().String())
	err = cmd.Run()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() == 0 {
		t.Fatalf("child err = %v, want a non-zero exit when the preview port is taken", err)
	}
}

func TestServePreviewReturnsAfterShutdown(t *testing.T) {
	serving := make(chan net.Addr, 1)
	srv := &http.Server{Addr: "127.0.0.1:0", BaseContext: func(ln net.Listener) context.Context {
		serving <- ln.Addr()
		return t.Context()
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		servePreview(t.Context(), srv)
	}()
	var addr net.Addr
	select {
	case addr = <-serving:
	case <-time.After(5 * time.Second):
		t.Fatal("servePreview never started serving")
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr.String()+"/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request the running preview server: %v", err)
	}
	_ = resp.Body.Close()
	if err := srv.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("servePreview did not return after Shutdown")
	}
}
