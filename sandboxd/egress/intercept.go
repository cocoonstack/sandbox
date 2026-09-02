package egress

import (
	"crypto/tls"
	"io"
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

	// maxLeaves bounds one sandbox's leaf cache against a wildcard rule.
	maxLeaves = 1024
)

// serveIntercept terminates a matched CONNECT's TLS with a node-signed leaf.
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
	// a guest that never sends a ClientHello would pin this goroutine until the sandbox dies.
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

// leafFor returns the cached leaf for host, signing a fresh one on miss or near-expiry.
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

// interceptHandler forwards one decrypted request per call; host is bare, authority host:port.
type interceptHandler struct {
	proxy     *Proxy
	host      string
	authority string
}

func (h *interceptHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := h.proxy
	rule, decision := p.policy.EvalInner(h.host, r.Method)
	p.relay(w, r, h.host, rule, decision, p.mitmTr, func(out *http.Request) {
		out.URL.Scheme = "https"
		out.URL.Host = h.authority
		// keep the guest's Host when it names the CONNECT host: SigV4 signing breaks on a rewrite.
		if !strings.EqualFold(hostOnly(r.Host), h.host) {
			out.Host = h.authority
		}
	})
}

// singleConnListener feeds one accepted conn to an http.Server, then blocks until it closes.
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
