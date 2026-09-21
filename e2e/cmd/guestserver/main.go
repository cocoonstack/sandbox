// guestserver is the in-guest listener the port-relay e2e drives: a loopback
// HTTP server that reports the protocol it was reached over. It stands in for
// e2b's envd during hardware tests — the relay carries bytes, so proving
// HTTP/1.1 and h2c both survive it needs a server that speaks both.
//
// It is uploaded into a claimed sandbox by portsmoke; nothing in the stock
// image provides an HTTP listener.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:49983", "loopback address to listen on")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
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

// report answers with what the guest actually received, so the caller can
// assert that the relay changed nothing on the way in.
func report(w http.ResponseWriter, r *http.Request) {
	body := new(strings.Builder)
	fmt.Fprintf(body, "proto=%d\nmethod=%s\npath=%s\nhost=%s\n", r.ProtoMajor, r.Method, r.URL.RequestURI(), r.Host)
	for _, name := range []string{"X-Access-Token", "X-API-KEY", "X-Probe"} {
		fmt.Fprintf(body, "header[%s]=%s\n", name, r.Header.Get(name))
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, body.String())
}
