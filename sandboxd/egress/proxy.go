package egress

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	idleConnTimeout = 90 * time.Second
	// maxIdleConns bounds the upstream pool per sandbox; a guest walking many hosts cannot park a descriptor per host.
	maxIdleConns = 64
)

// hopHeaders are hop-by-hop and proxy-scoped headers this hop owns, never passed on.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
}

// TransferFunc receives the payload bytes an allowed tunnel or request moved, once it ends.
type TransferFunc func(ev Event, sent, received int64)

// AuditFunc receives every egress decision the proxy takes.
type AuditFunc func(Event)

// Secrets resolves a rule's Secret name to the header it injects.
type Secrets interface {
	Header(name string) (header, value string, ok bool)
}

// Holder is held for the life of every request and tunnel the proxy serves.
type Holder interface {
	Hold()
	Unhold()
}

// Event is one audited egress attempt; Injected names the credential and Upstream the proxy host, never their secrets.
type Event struct {
	Method   string
	Host     string
	Port     uint16
	Decision Decision
	Injected string
	Upstream string
}

type routeKey struct {
	route string
	mitm  bool
}

type connPools struct {
	tr   *http.Transport
	mitm *http.Transport

	// routed pools each upstream's connections apart, so a route change never reuses another path's conn.
	mu     sync.Mutex
	routed map[routeKey]*http.Transport
}

// DialFunc opens the upstream connection for a permitted request; as a Router it sends every connection direct.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func (d DialFunc) Route() string { return "" }

func (d DialFunc) Dial(ctx context.Context, _, network, addr string) (net.Conn, error) {
	return d(ctx, network, addr)
}

// Proxy is one sandbox's forward proxy, gated by Policy and audited per request.
type Proxy struct {
	policy   Evaluator
	secrets  Secrets
	ca       *CA
	audit    AuditFunc
	transfer TransferFunc
	router   Router
	holder   Holder
	pools    atomic.Pointer[connPools]

	// leaves caches this sandbox's interception leaves; per-proxy, never shared.
	leafMu sync.Mutex
	leaves map[string]*tls.Certificate

	// conns tracks both halves of every tunnel and SOCKS5 connection, which http.Server.Close does not reach.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Proxy for one sandbox; secrets, ca, audit and holder may be nil, router must not.
func New(policy Evaluator, secrets Secrets, ca *CA, router Router, audit AuditFunc, holder Holder) *Proxy {
	direct := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return router.Dial(ctx, "", network, addr)
	}
	p := &Proxy{
		policy:  policy,
		secrets: secrets,
		ca:      ca,
		audit:   audit,
		router:  router,
		holder:  holder,
		conns:   map[net.Conn]struct{}{},
	}
	// the stdlib default of 2 idle conns per host re-dials bursty same-host traffic.
	pools := &connPools{tr: &http.Transport{DialContext: direct, MaxIdleConns: maxIdleConns, MaxIdleConnsPerHost: 8, IdleConnTimeout: idleConnTimeout}, routed: map[routeKey]*http.Transport{}}
	if ca != nil {
		pools.mitm = pools.tr.Clone()
		pools.mitm.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		p.leaves = map[string]*tls.Certificate{}
	}
	p.pools.Store(pools)
	return p
}

// OnTransfer reports every allowed tunnel's and request's payload bytes to f; call it before serving.
func (p *Proxy) OnTransfer(f TransferFunc) { p.transfer = f }

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer p.hold()()
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
	p.ResetPools()
}

// ResetPools moves new requests to fresh pools; the old ones close each connection as its in-flight request ends.
func (p *Proxy) ResetPools() {
	cur := p.pools.Load()
	next := &connPools{tr: cur.tr.Clone(), routed: map[routeKey]*http.Transport{}}
	if cur.mitm != nil {
		next.mitm = cur.mitm.Clone()
	}
	old := p.pools.Swap(next)
	old.mu.Lock()
	idle := append(slices.Collect(maps.Values(old.routed)), old.tr)
	old.mu.Unlock()
	if old.mitm != nil {
		idle = append(idle, old.mitm)
	}
	for _, tr := range idle {
		tr.CloseIdleConnections()
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

func (p *Proxy) hold() func() {
	if p.holder == nil {
		return func() {}
	}
	p.holder.Hold()
	return p.holder.Unhold
}

func (p *Proxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	host, port, ok := hostPort(r.Host, 443)
	if !ok {
		p.record(Event{Method: r.Method, Host: host, Decision: DecisionDeny})
		denied(w, host)
		return
	}
	decision, intercept := p.tunnelDecision(host, port)
	ev, route := p.routeEvent(Event{Method: r.Method, Host: host, Port: port, Decision: decision})
	if intercept {
		p.record(ev)
		p.serveIntercept(w, r, host, port)
		return
	}
	if decision == DecisionDeny {
		p.record(ev)
		denied(w, host)
		return
	}
	upstream, err := p.router.Dial(r.Context(), route, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	ev.Upstream = upstreamOf(upstream, err)
	p.record(ev)
	if err != nil {
		http.Error(w, "egress: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer func() { _ = upstream.Close() }()
	if !p.track(upstream) {
		return
	}
	defer p.untrack(upstream)
	client, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, "egress: connection cannot be hijacked", http.StatusInternalServerError)
		return
	}
	defer func() { _ = client.Close() }()
	if !p.track(client) {
		return
	}
	defer p.untrack(client)
	if _, err = io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	early, err := flushBuffered(brw.Reader, upstream)
	if err != nil {
		p.reportTransfer(ev, early, 0)
		return
	}
	sent, received := splice(client, upstream)
	p.reportTransfer(ev, early+sent, received)
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
	ev, route := p.routeEvent(Event{Method: r.Method, Host: host, Port: port, Decision: decision})
	p.relay(w, r, ev, rule, route, false, nil)
}

func (p *Proxy) relay(w http.ResponseWriter, r *http.Request, ev Event, rule Rule, route string, mitm bool, prepare func(*http.Request)) {
	if ev.Decision == DecisionDeny {
		p.record(ev)
		denied(w, ev.Host)
		return
	}

	ctx := r.Context()
	var upstream *string
	if route != "" {
		upstream = new(string)
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { *upstream = upstreamOf(info.Conn, nil) },
		})
	}
	out := r.Clone(ctx)
	if prepare != nil {
		prepare(out)
	}
	out.RequestURI = ""
	out.Close = false // the guest's Connection: close is its own; the upstream pool keeps the conn
	stripHop(out.Header)
	ev.Injected = p.inject(rule, out.Header)
	var body *countingBody
	if p.transfer != nil && out.Body != nil && out.Body != http.NoBody {
		body = &countingBody{ReadCloser: out.Body}
		out.Body = body
	}
	var received int64
	defer func() {
		var sent int64
		if body != nil {
			sent = body.n.Load()
		}
		p.reportTransfer(ev, sent, received)
	}()

	resp, err := p.transport(route, mitm).RoundTrip(out)
	if upstream != nil {
		ev.Upstream = cmp.Or(*upstream, upstreamOf(nil, err))
	}
	p.record(ev)
	if err != nil {
		http.Error(w, "egress: upstream unreachable", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	maps.Copy(w.Header(), resp.Header)
	stripHop(w.Header())
	w.WriteHeader(resp.StatusCode)
	received, _ = io.Copy(w, resp.Body)
}

// transport returns the pool for route; the direct route keeps the base transports.
func (p *Proxy) transport(route string, mitm bool) *http.Transport {
	pools := p.pools.Load()
	base := pools.tr
	if mitm {
		base = pools.mitm
	}
	if route == "" {
		return base
	}
	pools.mu.Lock()
	defer pools.mu.Unlock()
	key := routeKey{route: route, mitm: mitm}
	tr := pools.routed[key]
	if tr == nil {
		tr = base.Clone()
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return p.router.Dial(ctx, route, network, addr)
		}
		pools.routed[key] = tr
	}
	return tr
}

func (p *Proxy) routeEvent(ev Event) (Event, string) {
	if ev.Decision != DecisionAllow {
		return ev, ""
	}
	return ev, p.router.Route()
}

func (p *Proxy) reportTransfer(ev Event, sent, received int64) {
	if p.transfer != nil {
		p.transfer(ev, sent, received)
	}
}

// inject sets the rule's secret header, overwriting any guest-supplied value.
func (p *Proxy) inject(rule Rule, h http.Header) string {
	if rule.Secret == "" || p.secrets == nil {
		return ""
	}
	header, value, ok := p.secrets.Header(rule.Secret)
	if !ok || value == "" {
		return ""
	}
	h.Set(header, value)
	return rule.Secret
}

func (p *Proxy) record(ev Event) {
	if p.audit != nil {
		p.audit(ev)
	}
}

// countingBody counts the request body bytes the transport reads, possibly after RoundTrip returns.
type countingBody struct {
	io.ReadCloser
	n atomic.Int64
}

func (c *countingBody) Read(b []byte) (int, error) {
	n, err := c.ReadCloser.Read(b)
	c.n.Add(int64(n))
	return n, err
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	if c.r.Buffered() > 0 {
		return c.r.Read(b)
	}
	return c.Conn.Read(b)
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

// splice copies both ways until either side ends, half-closing the peer so EOF propagates; it returns the bytes a sent to b and b sent to a.
func splice(a, b net.Conn) (aToB, bToA int64) {
	ra := spliceable(a)
	done := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(ra, b)
		utils.CloseWrite(a)
		done <- n
	}()
	aToB, _ = io.Copy(b, ra)
	utils.CloseWrite(b)
	return aToB, <-done
}

func flushBuffered(r *bufio.Reader, w io.Writer) (int64, error) {
	if r.Buffered() == 0 {
		return 0, nil
	}
	b, _ := r.Peek(r.Buffered())
	n, err := w.Write(b)
	return int64(n), err
}

func withBuffered(c net.Conn, r *bufio.Reader) net.Conn {
	if r.Buffered() == 0 {
		return c
	}
	return &bufferedConn{Conn: c, r: r}
}

func spliceable(c net.Conn) net.Conn {
	if d, ok := c.(interface{ NetConn() net.Conn }); ok {
		if u, ok := d.NetConn().(*net.UnixConn); ok {
			return u
		}
	}
	return c
}

func denied(w http.ResponseWriter, host string) {
	http.Error(w, fmt.Sprintf("egress denied: %s", host), http.StatusForbidden)
}

// hostPort splits an authority; a bare host takes def, a port outside 1-65535 or a malformed authority is refused rather than defaulted.
func hostPort(authority string, def uint16) (host string, port uint16, ok bool) {
	host, text, err := net.SplitHostPort(authority)
	if err != nil {
		literal, bracketed := strings.CutPrefix(authority, "[")
		if !bracketed {
			return authority, def, authority != "" && !strings.ContainsAny(authority, ":[]")
		}
		literal, closed := strings.CutSuffix(literal, "]")
		addr, parseErr := netip.ParseAddr(literal)
		return literal, def, closed && parseErr == nil && addr.Is6()
	}
	n, err := strconv.ParseUint(text, 10, 16)
	return host, uint16(n), err == nil && n != 0
}
