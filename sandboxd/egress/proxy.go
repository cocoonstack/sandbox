package egress

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/textproto"
	"net/url"
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
	maxIdleConns  = 64
	maxInjectBody = 64 << 10
	scrubChunk    = 8 << 10
)

var (
	// hopHeaders are hop-by-hop and proxy-scoped headers this hop owns, never passed on.
	hopHeaders = []string{
		"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
	}

	copyBufs = sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}
)

// TransferFunc receives the payload bytes an allowed tunnel or request moved, once it ends.
type TransferFunc func(ev Event, sent, received int64)

// AuditFunc receives every egress decision the proxy takes.
type AuditFunc func(Event)

// Credential is a claim-supplied value the proxy sets on an intercepted request where the guest sent its Placeholder.
type Credential struct {
	Name        string
	Header      string
	Query       string
	Body        bool
	Value       string
	Placeholder string
}

// Secrets resolves a rule's Secret name to the header it injects, and a host to the claim's own credentials for it.
type Secrets interface {
	Header(name string) (header, value string, ok bool)
	Credentials(host string) []Credential
}

// Holder is held for the life of every request and tunnel the proxy serves.
type Holder interface {
	Hold()
	Unhold()
}

// Event is one audited egress attempt; Injected lists the credentials set, comma-separated, and Upstream the proxy host, never their secrets.
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
	// tickets lets a guest resume an intercepted TLS session on its next CONNECT.
	tickets [][32]byte

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
		p.tickets = make([][32]byte, 1)
		_, _ = rand.Read(p.tickets[0][:])
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
	decision, intercept := p.tunnelDecision(host, port, true)
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

// tunnelDecision gates a tunnel by host; injects is false on a door that never injects, where an inject rule is a plain one.
func (p *Proxy) tunnelDecision(host string, port uint16, injects bool) (decision Decision, intercept bool) {
	// host-gate interception: the tunnel's CONNECT verb is not the request method.
	if p.ca != nil {
		switch rule, d := p.policy.EvalHost(host, port); {
		case d != DecisionAllow:
		case rule.Intercept == InterceptAlways, rule.Intercept == InterceptInject && injects && p.holdsCredential(host):
			return DecisionAllow, true
		}
	}
	_, decision = p.policy.Eval(host, http.MethodConnect, port)
	return decision, false
}

func (p *Proxy) holdsCredential(host string) bool {
	return p.secrets != nil && slices.ContainsFunc(p.secrets.Credentials(strings.ToLower(host)), func(c Credential) bool { return c.Value != "" })
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
	var used []Credential
	ev.Injected, used = p.inject(rule, out, ev.Host, mitm)
	olds, news := echoes(used)
	if len(olds) > 0 {
		// the transport then decodes compression itself, and no range splits a value, so the scrub sees every echo whole.
		for _, h := range []string{"Accept-Encoding", "Range", "If-Range"} {
			out.Header.Del(h)
		}
	}
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
	var src io.Reader = resp.Body
	if len(olds) > 0 {
		scrubHeader(w.Header(), olds, news)
		w.Header().Del("Content-Length")
		src = &scrubReader{src: resp.Body, olds: olds, news: news, chunk: make([]byte, scrubChunk)}
	}
	w.WriteHeader(resp.StatusCode)
	if streamed(resp) {
		received = copyFlushing(w, src)
		return
	}
	received, _ = io.Copy(w, src)
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

// inject sets the rule's secret, then on an intercepted request the claim credentials the guest asked for, and returns the names set and the credentials used.
func (p *Proxy) inject(rule Rule, out *http.Request, host string, mitm bool) (string, []Credential) {
	if p.secrets == nil {
		return "", nil
	}
	var names, set []string
	if rule.Secret != "" {
		if header, value, ok := p.secrets.Header(rule.Secret); ok && value != "" {
			out.Header.Set(header, value)
			names, set = append(names, rule.Secret), append(set, header)
		}
	}
	if !mitm {
		return strings.Join(names, ","), nil
	}
	var used, body []Credential
	for _, c := range p.secrets.Credentials(strings.ToLower(host)) {
		switch {
		case c.Value == "":
		case c.Query != "":
			if c.Body {
				body = append(body, c)
			}
			if q, ok := replaceParam(out.URL.RawQuery, c.Query, c.Placeholder, c.Value); ok {
				out.URL.RawQuery = q
				used = append(used, c)
			}
		case c.Placeholder != "" && out.Header.Get(c.Header) != c.Placeholder,
			slices.ContainsFunc(set, func(s string) bool { return strings.EqualFold(s, c.Header) }):
		default:
			out.Header.Set(c.Header, c.Value)
			set, used = append(set, c.Header), append(used, c)
		}
	}
	for _, c := range substituteBody(out, body) {
		if !slices.ContainsFunc(used, func(u Credential) bool { return u.Name == c.Name }) {
			used = append(used, c)
		}
	}
	for _, c := range used {
		names = append(names, "claim:"+c.Name)
	}
	return strings.Join(names, ","), used
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

// scrubReader replaces each of olds with its news as a response body streams; it holds back only a tail that may begin a match.
type scrubReader struct {
	src        io.Reader
	olds, news [][]byte
	chunk      []byte
	pending    []byte
	out        []byte
	off        int
	err        error
}

func (s *scrubReader) Read(b []byte) (int, error) {
	for s.off == len(s.out) && s.err == nil {
		s.out, s.off = s.out[:0], 0
		n, err := s.src.Read(s.chunk)
		s.pending, s.err = append(s.pending, s.chunk[:n]...), err
		s.scan()
	}
	if s.off == len(s.out) {
		return 0, s.err
	}
	n := copy(b, s.out[s.off:])
	s.off += n
	return n, nil
}

func (s *scrubReader) scan() {
	buf, i := s.pending, 0
	for {
		at, k := -1, 0
		for j, old := range s.olds {
			if idx := bytes.Index(buf[i:], old); idx >= 0 && (at < 0 || idx < at) {
				at, k = idx, j
			}
		}
		if at < 0 {
			break
		}
		s.out = append(append(s.out, buf[i:i+at]...), s.news[k]...)
		i += at + len(s.olds[k])
	}
	hold := 0
	if s.err == nil {
		hold = s.partial(buf[i:])
	}
	s.out = append(s.out, buf[i:len(buf)-hold]...)
	s.pending = s.pending[:copy(s.pending, buf[len(buf)-hold:])]
}

func (s *scrubReader) partial(tail []byte) int {
	longest := 0
	for _, old := range s.olds {
		longest = max(longest, len(old))
	}
	for k := min(len(tail), longest-1); k > 0; k-- {
		if slices.ContainsFunc(s.olds, func(old []byte) bool { return len(old) > k && bytes.HasPrefix(old, tail[len(tail)-k:]) }) {
			return k
		}
	}
	return 0
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

func spliceable(c net.Conn) net.Conn {
	if d, ok := c.(interface{ NetConn() net.Conn }); ok {
		if u, ok := d.NetConn().(*net.UnixConn); ok {
			return u
		}
	}
	return c
}

func streamed(resp *http.Response) bool {
	return resp.ContentLength < 0 || mediaType(resp.Header) == "text/event-stream"
}

func mediaType(h http.Header) string {
	mt, _, _ := strings.Cut(h.Get("Content-Type"), ";")
	return strings.ToLower(strings.TrimSpace(mt))
}

// replaceParam swaps the value of each name=placeholder pair of a query or form string for the escaped value, leaving every other byte as sent.
func replaceParam(raw, name, placeholder, value string) (string, bool) {
	if raw == "" {
		return raw, false
	}
	pairs := strings.Split(raw, "&")
	escaped := ""
	for i, pair := range pairs {
		k, v, _ := strings.Cut(pair, "=")
		if uk, err := url.QueryUnescape(k); err != nil || uk != name {
			continue
		}
		if uv, err := url.QueryUnescape(v); err == nil && uv == placeholder {
			escaped = cmp.Or(escaped, url.QueryEscape(value))
			pairs[i] = k + "=" + escaped
		}
	}
	if escaped == "" {
		return raw, false
	}
	return strings.Join(pairs, "&"), true
}

// replaceMember swaps each top-level string member named name whose value is placeholder, leaving every other byte as sent.
func replaceMember(b []byte, name, placeholder, value string) ([]byte, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(b))
	var ends []int64
	var lens []int
	for {
		kind, n := dec.StackIndex(dec.StackDepth())
		if dec.StackDepth() != 1 || kind != '{' || n%2 == 0 || dec.PeekKind() != '"' {
			if _, err := dec.ReadToken(); err != nil {
				break
			}
			continue
		}
		raw, err := dec.ReadValue()
		if err != nil {
			break
		}
		if v, err := jsontext.AppendUnquote(nil, raw); err == nil && string(v) == placeholder && dec.StackPointer().LastToken() == name {
			ends, lens = append(ends, dec.InputOffset()), append(lens, len(raw))
		}
	}
	if len(ends) == 0 {
		return b, false
	}
	quoted, _ := jsontext.AppendQuote(nil, value)
	out, at := make([]byte, 0, len(b)+len(ends)*len(quoted)), int64(0)
	for i, end := range ends {
		out = append(append(out, b[at:end-int64(lens[i])]...), quoted...)
		at = end
	}
	return append(out, b[at:]...), true
}

func substituteBody(out *http.Request, creds []Credential) []Credential {
	if len(creds) == 0 || out.ContentLength <= 0 || out.ContentLength > maxInjectBody {
		return nil
	}
	mt := mediaType(out.Header)
	form := mt == "application/x-www-form-urlencoded"
	if !form && mt != "application/json" {
		return nil
	}
	b := make([]byte, out.ContentLength)
	if n, err := io.ReadFull(out.Body, b); err != nil {
		out.Body = io.NopCloser(io.MultiReader(bytes.NewReader(b[:n]), out.Body))
		return nil
	}
	var used []Credential
	for _, c := range creds {
		if form {
			if s, ok := replaceParam(string(b), c.Query, c.Placeholder, c.Value); ok {
				b, used = []byte(s), append(used, c)
			}
		} else if nb, ok := replaceMember(b, c.Query, c.Placeholder, c.Value); ok {
			b, used = nb, append(used, c)
		}
	}
	out.Body, out.ContentLength = io.NopCloser(bytes.NewReader(b)), int64(len(b))
	return used
}

// echoes pairs each used query credential's value, as sent, URL-escaped and JSON-escaped in their common forms, with its placeholder in the same form, so a response that reflects the URL cannot hand the guest the value.
func echoes(used []Credential) (olds, news [][]byte) {
	for _, c := range used {
		if c.Query == "" {
			continue
		}
		pairs := [][2]string{
			{c.Value, c.Placeholder},
			{url.QueryEscape(c.Value), url.QueryEscape(c.Placeholder)},
			{url.PathEscape(c.Value), url.PathEscape(c.Placeholder)},
		}
		if v, err := jsontext.AppendQuote(nil, c.Value); err == nil {
			ph, _ := jsontext.AppendQuote(nil, c.Placeholder)
			v, ph = v[1:len(v)-1], ph[1:len(ph)-1]
			pairs = append(pairs, [2]string{string(v), string(ph)},
				[2]string{strings.ReplaceAll(string(v), "/", `\/`), strings.ReplaceAll(string(ph), "/", `\/`)})
		}
		for _, p := range pairs {
			if !slices.ContainsFunc(olds, func(o []byte) bool { return string(o) == p[0] }) {
				olds, news = append(olds, []byte(p[0])), append(news, []byte(p[1]))
			}
		}
	}
	return olds, news
}

func scrubHeader(h http.Header, olds, news [][]byte) {
	pairs := make([]string, 0, 2*len(olds))
	for i := range olds {
		pairs = append(pairs, string(olds[i]), string(news[i]))
	}
	r := strings.NewReplacer(pairs...)
	for _, vs := range h {
		for i, v := range vs {
			vs[i] = r.Replace(v)
		}
	}
}

func copyFlushing(w http.ResponseWriter, body io.Reader) int64 {
	rc := http.NewResponseController(w)
	if rc.Flush() != nil {
		n, _ := io.Copy(w, body)
		return n
	}
	buf := copyBufs.Get().(*[]byte)
	defer copyBufs.Put(buf)
	var total int64
	for {
		n, err := body.Read(*buf)
		if n > 0 {
			written, werr := w.Write((*buf)[:n])
			total += int64(written)
			if werr != nil || rc.Flush() != nil {
				return total
			}
		}
		if err != nil {
			return total
		}
	}
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
