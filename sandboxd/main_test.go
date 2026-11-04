package main

import (
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
	srv := &http.Server{Addr: "127.0.0.1:0"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		servePreview(t.Context(), srv)
	}()
	if err := srv.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("servePreview did not return after Shutdown")
	}
}
