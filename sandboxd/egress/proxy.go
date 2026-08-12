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

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// hopHeaders are hop-by-hop and proxy-scoped headers this hop owns: stripped
// from forwarded requests and from relayed responses rather than passed on.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
}

// DialFunc opens the upstream connection for a permitted request. The proxy
// stays transport-agnostic: the node wires the real egress path (a bridge
// dial, or a checked resolver) behind it.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Secrets resolves a rule's Secret name to the header it injects. The value
// lives node-side and the proxy is its only holder, so a matched request
// carries the credential's effect without the guest ever seeing it.
type Secrets interface {
	Header(name string) (header, value string, ok bool)
}

// Event is one audited egress attempt, keyed by sandbox and tenant. Injected
// names the credential added to a permitted request, never its value.
type Event struct {
	Sandbox  string
	Tenant   string
	Method   string
	Host     string
	Decision Decision
	Injected string
}

// Proxy is one sandbox's forward proxy: an http.Handler serving CONNECT
// tunnels and absolute-form HTTP, gated by Policy and audited per request.
// One Proxy carries one sandbox's identity, so the connection a listener
// hands it needs no in-band token.
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

	// leaves caches this sandbox's interception leaves; per-proxy, so one
	// sandbox's host churn never evicts or blocks another's.
	leafMu sync.Mutex
	leaves map[string]*tls.Certificate

	// conns tracks hijacked tunnels, which http.Server.Close no longer reaches.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Proxy for one sandbox. secrets, ca, and audit may be nil (no
// injection, no HTTPS interception, no audit). dial must be set.
func New(sandbox, tenant string, policy Evaluator, secrets Secrets, ca *CA, dial DialFunc, audit func(Event)) *Proxy {
	p := &Proxy{
		sandbox: sandbox,
		tenant:  tenant,
		policy:  policy,
		secrets: secrets,
		ca:      ca,
		audit:   audit,
		dial:    dial,
		// The stdlib default of 2 idle conns per host re-dials bursty
		// same-host plaintext traffic.
		tr:    &http.Transport{DialContext: dial, MaxIdleConnsPerHost: 8},
		conns: map[net.Conn]struct{}{},
	}
	if ca != nil {
		p.mitmTr = &http.Transport{
			DialContext:         dial,
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			MaxIdleConnsPerHost: 8,
		}
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

// Close ends every hijacked tunnel, which outlives http.Server.Close, so a
// released claim's guest cannot keep an authorized session. Idempotent.
func (p *Proxy) Close() {
	p.connMu.Lock()
	conns := slices.Collect(maps.Keys(p.conns))
	clear(p.conns)
	p.closed = true
	p.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
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
	// Host-gate the interception decision: the tunnel's CONNECT verb is not the
	// request method, so a matched intercept rule enforces its methods on the
	// decrypted inner request. The plain splice keeps the host+method gate.
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

// relay forwards one policy-evaluated request upstream and mirrors the
// response; prepare, when set, rewrites the clone before it leaves.
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

// inject sets the header the rule's Secret resolves to, overwriting any
// guest-supplied value, and returns the secret name it applied (empty when
// the rule names none or the store cannot resolve it).
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

// splice copies bytes both ways until either side ends, then returns; each
// direction's end half-closes the peer so its EOF propagates.
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

// denied answers a policy rejection with a typed 403 the guest's client
// surfaces as a distinct failure rather than a hang.
func denied(w http.ResponseWriter, host string) {
	http.Error(w, fmt.Sprintf("egress denied: %s", host), http.StatusForbidden)
}

// hostOnly strips a port from an authority, tolerating a bare host and IPv6
// literals.
func hostOnly(authority string) string {
	if host, _, err := net.SplitHostPort(authority); err == nil {
		return host
	}
	return authority
}
