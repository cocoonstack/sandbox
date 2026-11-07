// guestserver is the in-guest HTTP/1.1 and h2c listener portsmoke uploads to test the port relay.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
)

type halfCloseSink struct {
	mu  sync.Mutex
	got string
}

func (h *halfCloseSink) serve(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "guestserver: half-close listen:", err)
		return
	}
	for {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		go h.greetThenRead(conn)
	}
}

func (h *halfCloseSink) greetThenRead(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if _, err := io.WriteString(tcp, "greeting"); err != nil {
		return
	}
	if err := tcp.CloseWrite(); err != nil {
		return
	}
	rest, _ := io.ReadAll(tcp)
	h.mu.Lock()
	h.got = string(rest)
	h.mu.Unlock()
}

func (h *halfCloseSink) read() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.got
}

func main() {
	addr := flag.String("addr", "127.0.0.1:49983", "loopback address to listen on")
	halfClose := flag.String("half-close-addr", "", "raw address that greets, shuts its write side, then reads")
	flag.Parse()

	var seen halfCloseSink
	if *halfClose != "" {
		go seen.serve(*halfClose)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /half-close-seen", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, seen.read())
	})
	mux.HandleFunc("/", report)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{Addr: *addr, Handler: mux, Protocols: &protocols} //nolint:gosec // loopback test server inside a sandbox; no header timeout by design
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, "guestserver:", err)
		os.Exit(1)
	}
}

// report answers with what the guest received, so a caller can assert the relay changed nothing.
func report(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprintf(w, "proto=%d\nmethod=%s\npath=%s\nhost=%s\n", r.ProtoMajor, r.Method, r.URL.RequestURI(), r.Host)
	for _, name := range []string{"X-Access-Token", "X-API-KEY", "X-Probe"} {
		_, _ = fmt.Fprintf(w, "header[%s]=%s\n", name, r.Header.Get(name))
	}
}
