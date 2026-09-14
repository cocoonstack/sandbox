package egress

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/textproto"
	"slices"
	"strconv"
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

// AuditFunc receives every egress decision the proxy takes.
type AuditFunc func(Event)

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
	Port     uint16
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
	audit   AuditFunc
	dial    DialFunc
	tr      *http.Transport
	mitmTr  *http.Transport

	// leaves caches this sandbox's interception leaves; per-proxy, never shared.
	leafMu sync.Mutex
	leaves map[string]*tls.Certificate

	// conns tracks hijacked tunnels and SOCKS5 connections, which http.Server.Close does not reach.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Proxy for one sandbox; secrets, ca, and audit may be nil, dial must not.
func New(sandbox, tenant string, policy Evaluator, secrets Secrets, ca *CA, dial DialFunc, audit AuditFunc) *Proxy {
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

func (p *Proxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	host, port, ok := hostPort(r.Host, 443)
	if !ok {
		p.record(Event{Method: r.Method, Host: host, Decision: DecisionDeny})
		denied(w, host)
		return
	}
	decision, intercept := p.tunnelDecision(host, port)
	if intercept {
		p.serveIntercept(w, r, host, port)
		return
	}
	p.record(Event{Method: r.Method, Host: host, Port: port, Decision: decision})
	if decision == DecisionDeny {
		denied(w, host)
		return
	}
	upstream, err := p.dial(r.Context(), "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
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

func (p *Proxy) tunnelDecision(host string, port uint16) (decision Decision, intercept bool) {
	// host-gate interception: the tunnel's CONNECT verb is not the request method.
	if p.ca != nil {
		if rule, d := p.policy.EvalHost(host, port); d == DecisionAllow && rule.Intercept {
			return DecisionAllow, true
		}
	}
	_, decision = p.policy.Eval(host, http.MethodConnect, port)
	return decision, false
}

func (p *Proxy) serveForward(w http.ResponseWriter, r *http.Request) {
	if !r.URL.IsAbs() {
		http.Error(w, "egress: proxy requires an absolute-form request URI", http.StatusBadRequest)
		return
	}
	def := uint16(80)
	if r.URL.Scheme == "https" {
		def = 443
	}
	host, port, ok := hostPort(r.URL.Host, def)
	if !ok {
		p.record(Event{Method: r.Method, Host: host, Decision: DecisionDeny})
		denied(w, host)
		return
	}
	rule, decision := p.policy.Eval(host, r.Method, port)
	p.relay(w, r, Event{Method: r.Method, Host: host, Port: port, Decision: decision}, rule, p.tr, nil)
}

// relay forwards one policy-evaluated request upstream and mirrors the response.
func (p *Proxy) relay(w http.ResponseWriter, r *http.Request, ev Event, rule Rule, tr *http.Transport, prepare func(*http.Request)) {
	if ev.Decision == DecisionDeny {
		p.record(ev)
		denied(w, ev.Host)
		return
	}

	out := r.Clone(r.Context())
	if prepare != nil {
		prepare(out)
	}
	out.RequestURI = ""
	stripHop(out.Header)
	ev.Injected = p.inject(rule, out.Header)
	p.record(ev)

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

// hostPort splits an authority; a bare host takes def, a port outside 1-65535 or a malformed authority is refused rather than defaulted.
func hostPort(authority string, def uint16) (host string, port uint16, ok bool) {
	host, text, err := net.SplitHostPort(authority)
	if err != nil {
		if !strings.Contains(authority, ":") {
			return authority, def, true
		}
		literal, bracketed := strings.CutPrefix(authority, "[")
		literal, closed := strings.CutSuffix(literal, "]")
		addr, parseErr := netip.ParseAddr(literal)
		return literal, def, bracketed && closed && parseErr == nil && addr.Is6()
	}
	n, err := strconv.ParseUint(text, 10, 16)
	return host, uint16(n), err == nil && n != 0
}
