package egress

import (
	"crypto/tls"
	"io"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	interceptTimeout = 30 * time.Second
	leafRenewBefore  = 24 * time.Hour

	// maxLeaves bounds one sandbox's leaf cache so a wildcard intercept rule
	// can't let its guest mint unbounded per-host leaves; the flush is scoped to
	// this sandbox, never a neighbor's.
	maxLeaves = 1024
)

// serveIntercept terminates a matched CONNECT's TLS with a leaf the node CA
// signs, then serves the decrypted HTTP/1.1 stream through interceptHandler:
// the guest trusts the CA (baked into its golden), so it accepts the leaf, and
// the proxy now sees method and path and can inject the rule's secret. Upstream
// is re-originated with real-root verification (mitmTr), never blindly trusted.
func (p *Proxy) serveIntercept(w http.ResponseWriter, r *http.Request, host string) {
	leaf, err := p.leafFor(host)
	if err != nil {
		http.Error(w, "egress: intercept setup failed", http.StatusInternalServerError)
		return
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"},
		Certificates: []tls.Certificate{*leaf},
	}
	client, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, "egress: connection cannot be hijacked", http.StatusInternalServerError)
		return
	}
	defer func() { _ = client.Close() }()
	if !p.track(client) {
		return
	}
	defer p.untrack(client)
	if _, err := io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	// A guest that never sends a ClientHello would otherwise pin this goroutine
	// and its FD until the sandbox dies; the deadline is cleared once the
	// handshake lands, leaving http.Server's per-request timeouts in charge.
	_ = client.SetDeadline(time.Now().Add(interceptTimeout))
	tlsConn := tls.Server(client, cfg)
	if err := tlsConn.HandshakeContext(r.Context()); err != nil {
		return
	}
	_ = client.SetDeadline(time.Time{})
	ln := &singleConnListener{conn: tlsConn, done: make(chan struct{})}
	srv := &http.Server{
		Handler:           &interceptHandler{proxy: p, host: host, authority: r.Host},
		ReadHeaderTimeout: interceptTimeout,
	}
	_ = srv.Serve(ln)
}

// leafFor returns this sandbox's cached leaf for host, signing a fresh one on
// miss or near-expiry; the cache and its flush are per-sandbox.
func (p *Proxy) leafFor(host string) (*tls.Certificate, error) {
	p.leafMu.Lock()
	defer p.leafMu.Unlock()
	if crt, ok := p.leaves[host]; ok && time.Now().Before(crt.Leaf.NotAfter.Add(-leafRenewBefore)) {
		return crt, nil
	}
	crt, err := p.ca.SignLeaf(host)
	if err != nil {
		return nil, err
	}
	if len(p.leaves) >= maxLeaves {
		clear(p.leaves)
	}
	p.leaves[host] = crt
	return crt, nil
}

// interceptHandler forwards one decrypted request per call: host is the CONNECT
// authority's hostname (policy + leaf key), authority its host:port (upstream).
type interceptHandler struct {
	proxy     *Proxy
	host      string
	authority string
}

func (h *interceptHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := h.proxy
	rule, decision := p.policy.Eval(h.host, r.Method)
	if decision == DecisionDeny {
		p.record(Event{Method: r.Method, Host: h.host, Decision: decision})
		denied(w, h.host)
		return
	}
	out := r.Clone(r.Context())
	out.URL.Scheme = "https"
	out.URL.Host = h.authority
	// Keep the guest's Host when it names the CONNECT host (SigV4-style signing
	// breaks on the ":443" rewrite); a foreign Host snaps to the authority.
	if !strings.EqualFold(hostOnly(r.Host), h.host) {
		out.Host = h.authority
	}
	out.RequestURI = ""
	stripHop(out.Header)
	injected := p.inject(rule, out.Header)
	p.record(Event{Method: r.Method, Host: h.host, Decision: decision, Injected: injected})

	resp, err := p.mitmTr.RoundTrip(out)
	if err != nil {
		http.Error(w, "egress: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	maps.Copy(w.Header(), resp.Header)
	stripHop(w.Header())
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// singleConnListener feeds one already-accepted conn to an http.Server and then
// blocks until that conn closes, so Serve exits once the tunnel ends.
type singleConnListener struct {
	conn   net.Conn
	handed atomic.Bool
	once   sync.Once
	done   chan struct{}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.handed.CompareAndSwap(false, true) {
		return &closeOnceConn{Conn: l.conn, ln: l}, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *singleConnListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }

type closeOnceConn struct {
	net.Conn
	ln *singleConnListener
}

func (c *closeOnceConn) Close() error {
	_ = c.ln.Close()
	return c.Conn.Close()
}
