package egress

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const idleConnTimeout = 90 * time.Second

// hopHeaders are hop-by-hop and proxy-scoped headers this hop owns, never passed on.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
}

// DialFunc opens the upstream connection for a permitted request.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Secrets resolves a rule's Secret name to the header it injects.
type Secrets interface {
	Header(name string) (header, value string, ok bool)
}

// Event is one audited egress attempt; Injected names the credential, never its value.
type Event struct {
	Sandbox  string
	Tenant   string
	Method   string
	Host     string
	Decision Decision
	Injected string
}

// Proxy is one sandbox's forward proxy, gated by Policy and audited per request.
type Proxy struct {
	sandbox string
	tenant  string
	policy  Evaluator
	secrets Secrets
	ca      *CA
	audit   func(Event)
	dial    DialFunc
	tr      *http.Transport
	mitmTr  *http.Transport

	// leaves caches this sandbox's interception leaves; per-proxy, never shared.
	leafMu sync.Mutex
	leaves map[string]*tls.Certificate

	// conns tracks hijacked tunnels, which http.Server.Close no longer reaches.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Proxy for one sandbox; secrets, ca, and audit may be nil, dial must not.
func New(sandbox, tenant string, policy Evaluator, secrets Secrets, ca *CA, dial DialFunc, audit func(Event)) *Proxy {
	p := &Proxy{
		sandbox: sandbox,
		tenant:  tenant,
		policy:  policy,
		secrets: secrets,
		ca:      ca,
		audit:   audit,
		dial:    dial,
		// the stdlib default of 2 idle conns per host re-dials bursty same-host traffic.
		tr:    &http.Transport{DialContext: dial, MaxIdleConnsPerHost: 8, IdleConnTimeout: idleConnTimeout},
		conns: map[net.Conn]struct{}{},
	}
	if ca != nil {
		p.mitmTr = p.tr.Clone()
		p.mitmTr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		p.leaves = map[string]*tls.Certificate{}
	}
	return p
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.serveConnect(w, r)
		return
	}
	p.serveForward(w, r)
}

// Close ends every hijacked tunnel, which outlives http.Server.Close; idempotent.
func (p *Proxy) Close() {
	p.connMu.Lock()
	conns := slices.Collect(maps.Keys(p.conns))
	clear(p.conns)
	p.closed = true
	p.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	p.tr.CloseIdleConnections()
	if p.mitmTr != nil {
		p.mitmTr.CloseIdleConnections()
	}
}

// track registers conn for Close; false means already closed, conn closed instead.
func (p *Proxy) track(conn net.Conn) bool {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.closed {
		_ = conn.Close()
		return false
	}
	p.conns[conn] = struct{}{}
	return true
}

func (p *Proxy) untrack(conn net.Conn) {
	p.connMu.Lock()
	delete(p.conns, conn)
	p.connMu.Unlock()
}

// serveConnect gates an HTTPS/opaque tunnel by host.
func (p *Proxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	host := hostOnly(r.Host)
	// host-gate interception: the tunnel's CONNECT verb is not the request method.
	if rule, d := p.policy.EvalHost(host); d == DecisionAllow && rule.Intercept && p.ca != nil {
		p.serveIntercept(w, r, host)
		return
	}
	_, decision := p.policy.Eval(host, r.Method)
	p.record(Event{Method: r.Method, Host: host, Decision: decision})
	if decision == DecisionDeny {
		denied(w, host)
		return
	}
	upstream, err := p.dial(r.Context(), "tcp", r.Host)
	if err != nil {
		http.Error(w, "egress: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer func() { _ = upstream.Close() }()
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
	splice(client, upstream)
}

func (p *Proxy) serveForward(w http.ResponseWriter, r *http.Request) {
	if !r.URL.IsAbs() {
		http.Error(w, "egress: proxy requires an absolute-form request URI", http.StatusBadRequest)
		return
	}
	host := hostOnly(r.URL.Host)
	rule, decision := p.policy.Eval(host, r.Method)
	p.relay(w, r, host, rule, decision, p.tr, nil)
}

// relay forwards one policy-evaluated request upstream and mirrors the response.
func (p *Proxy) relay(w http.ResponseWriter, r *http.Request, host string, rule Rule, decision Decision, tr *http.Transport, prepare func(*http.Request)) {
	if decision == DecisionDeny {
		p.record(Event{Method: r.Method, Host: host, Decision: decision})
		denied(w, host)
		return
	}

	out := r.Clone(r.Context())
	if prepare != nil {
		prepare(out)
	}
	out.RequestURI = ""
	stripHop(out.Header)
	injected := p.inject(rule, out.Header)
	p.record(Event{Method: r.Method, Host: host, Decision: decision, Injected: injected})

	resp, err := tr.RoundTrip(out)
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

// inject sets the rule's secret header, overwriting any guest-supplied value.
func (p *Proxy) inject(rule Rule, h http.Header) string {
	if rule.Secret == "" || p.secrets == nil {
		return ""
	}
	header, value, ok := p.secrets.Header(rule.Secret)
	if !ok {
		return ""
	}
	h.Set(header, value)
	return rule.Secret
}

func (p *Proxy) record(ev Event) {
	if p.audit == nil {
		return
	}
	ev.Sandbox, ev.Tenant = p.sandbox, p.tenant
	p.audit(ev)
}

func stripHop(h http.Header) {
	// Connection may name additional hop headers this hop must consume.
	for _, v := range h.Values("Connection") {
		for f := range strings.SplitSeq(v, ",") {
			if f = textproto.TrimString(f); f != "" {
				h.Del(f)
			}
		}
	}
	for _, k := range hopHeaders {
		h.Del(k)
	}
}

// splice copies both ways until either side ends, half-closing the peer so EOF propagates.
func splice(a, b net.Conn) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(a, b)
		utils.CloseWrite(a)
	}()
	_, _ = io.Copy(b, a)
	utils.CloseWrite(b)
	<-done
}

// denied answers a policy rejection with a 403 rather than a hang.
func denied(w http.ResponseWriter, host string) {
	http.Error(w, fmt.Sprintf("egress denied: %s", host), http.StatusForbidden)
}

// hostOnly strips a port from an authority, tolerating a bare host and IPv6 literals.
func hostOnly(authority string) string {
	if host, _, err := net.SplitHostPort(authority); err == nil {
		return host
	}
	return authority
}
