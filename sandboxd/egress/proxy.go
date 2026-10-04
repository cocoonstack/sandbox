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
	scrubChunk    = 16 << 10
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

// tunnelDecision treats an inject rule as a plain one when injects is false, on a door that never injects.
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
	var sc *scrubber
	ev.Injected, sc = p.inject(rule, out, ev.Host, mitm)
	if sc != nil {
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
	if sc != nil {
		sc.header(w.Header())
		w.Header().Del("Content-Length")
		buf := copyBufs.Get().(*[]byte)
		defer copyBufs.Put(buf)
		src = newScrubReader(resp.Body, sc, *buf)
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

// inject sets the rule's secret, then on an intercepted request the claim credentials the guest asked for, and returns a scrubber for the query credentials it filled.
func (p *Proxy) inject(rule Rule, out *http.Request, host string, mitm bool) (string, *scrubber) {
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
	var filled, body []Credential
	for _, c := range p.secrets.Credentials(strings.ToLower(host)) {
		if c.Value == "" {
			continue
		}
		if c.Query != "" {
			if q := replaceParam(out.URL.RawQuery, c.Query, c.Placeholder, c.Value); q != out.URL.RawQuery {
				out.URL.RawQuery, filled = q, append(filled, c)
			} else if c.Body {
				body = append(body, c)
			}
			continue
		}
		if (c.Placeholder != "" && out.Header.Get(c.Header) != c.Placeholder) ||
			slices.ContainsFunc(set, func(s string) bool { return strings.EqualFold(s, c.Header) }) {
			continue
		}
		out.Header.Set(c.Header, c.Value)
		names, set = append(names, "claim:"+c.Name), append(set, c.Header)
	}
	filled = append(filled, substituteBody(out, body)...)
	for _, c := range filled {
		names = append(names, "claim:"+c.Name)
	}
	return strings.Join(names, ","), newScrubber(filled)
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

type echo struct {
	old         string
	raw         []byte
	placeholder string
}

// scrubber turns each filled query credential's value, in the escapings a reflected URL carries, back into its placeholder.
type scrubber struct {
	echoes  []echo
	first   [256]bool
	longest int
}

func newScrubber(filled []Credential) *scrubber {
	if len(filled) == 0 {
		return nil
	}
	sc := &scrubber{}
	for _, c := range filled {
		values, placeholders := escapings(c.Value), escapings(c.Placeholder)
		for i, old := range values {
			if !slices.ContainsFunc(sc.echoes, func(e echo) bool { return e.old == old }) {
				sc.echoes = append(sc.echoes, echo{old: old, raw: []byte(old), placeholder: placeholders[i]})
				sc.first[old[0]], sc.longest = true, max(sc.longest, len(old))
			}
		}
	}
	slices.SortStableFunc(sc.echoes, func(a, b echo) int { return cmp.Compare(len(b.old), len(a.old)) })
	return sc
}

func (sc *scrubber) header(h http.Header) {
	for _, vs := range h {
		for i, v := range vs {
			for _, e := range sc.echoes {
				if strings.Contains(v, e.old) {
					v = strings.ReplaceAll(v, e.old, e.placeholder)
				}
			}
			vs[i] = v
		}
	}
}

// scrubReader streams a response body through a scrubber, holding back only a tail that may begin a match.
type scrubReader struct {
	src     io.Reader
	sc      *scrubber
	pending []byte
	out     bytes.Buffer
	err     error
}

// newScrubReader splits buf, at least two chunks long, between the bytes read and the bytes scrubbed.
func newScrubReader(src io.Reader, sc *scrubber, buf []byte) *scrubReader {
	return &scrubReader{src: src, sc: sc, pending: buf[:0:scrubChunk], out: *bytes.NewBuffer(buf[scrubChunk:scrubChunk])}
}

func (s *scrubReader) Read(b []byte) (int, error) {
	for s.out.Len() == 0 && s.err == nil {
		if len(s.pending) == cap(s.pending) {
			s.pending = slices.Grow(s.pending, scrubChunk)
		}
		n, err := s.src.Read(s.pending[len(s.pending):cap(s.pending)])
		s.pending, s.err = s.pending[:len(s.pending)+n], err
		s.scan()
	}
	if s.out.Len() == 0 {
		return 0, s.err
	}
	return s.out.Read(b)
}

func (s *scrubReader) scan() {
	buf, i := s.pending, 0
	for {
		at, k := -1, 0
		for j, e := range s.sc.echoes {
			if idx := bytes.Index(buf[i:], e.raw); idx >= 0 && (at < 0 || idx < at) {
				at, k = idx, j
			}
		}
		if at < 0 {
			break
		}
		s.out.Write(buf[i : i+at])
		s.out.WriteString(s.sc.echoes[k].placeholder)
		i += at + len(s.sc.echoes[k].raw)
	}
	hold := 0
	if s.err == nil {
		hold = s.partial(buf[i:])
	}
	s.out.Write(buf[i : len(buf)-hold])
	s.pending = s.pending[:copy(s.pending, buf[len(buf)-hold:])]
}

func (s *scrubReader) partial(tail []byte) int {
	for k := min(len(tail), s.sc.longest-1); k > 0; k-- {
		if s.sc.first[tail[len(tail)-k]] && slices.ContainsFunc(s.sc.echoes, func(e echo) bool {
			return len(e.raw) > k && bytes.HasPrefix(e.raw, tail[len(tail)-k:])
		}) {
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

// replaceParam fills the first name=placeholder pair of a query or form string, leaving every other byte as sent.
func replaceParam(raw, name, placeholder, value string) string {
	for rest, at := raw, 0; rest != ""; {
		pair, next, _ := strings.Cut(rest, "&")
		k, v, _ := strings.Cut(pair, "=")
		if uk, err := url.QueryUnescape(k); err == nil && uk == name {
			if uv, err := url.QueryUnescape(v); err == nil && uv == placeholder {
				return raw[:at] + k + "=" + url.QueryEscape(value) + raw[at+len(pair):]
			}
		}
		at, rest = at+len(pair)+1, next
	}
	return raw
}

// replaceMember fills the first top-level string member name holding placeholder, leaving every other byte as sent.
func replaceMember(doc, name, placeholder, value string) string {
	if !strings.Contains(doc, name) {
		return doc
	}
	dec := jsontext.NewDecoder(strings.NewReader(doc))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return doc
	}
	for dec.PeekKind() == '"' {
		key, err := dec.ReadToken()
		if err != nil {
			return doc
		}
		if key.String() != name || dec.PeekKind() != '"' {
			if dec.SkipValue() != nil {
				return doc
			}
			continue
		}
		raw, err := dec.ReadValue()
		if err != nil {
			return doc
		}
		if v, unquoteErr := jsontext.AppendUnquote(nil, raw); unquoteErr != nil || string(v) != placeholder {
			continue
		}
		quoted, err := jsontext.AppendQuote(nil, value)
		if err != nil {
			return doc
		}
		end := int(dec.InputOffset())
		return doc[:end-len(raw)] + string(quoted) + doc[end:]
	}
	return doc
}

func substituteBody(out *http.Request, creds []Credential) []Credential {
	if len(creds) == 0 || out.ContentLength <= 0 || out.ContentLength > maxInjectBody {
		return nil
	}
	replace := replaceMember
	switch mediaType(out.Header) {
	case "application/x-www-form-urlencoded":
		replace = replaceParam
	case "application/json":
	default:
		return nil
	}
	b := make([]byte, out.ContentLength)
	if n, err := io.ReadFull(out.Body, b); err != nil {
		out.Body = io.NopCloser(io.MultiReader(bytes.NewReader(b[:n]), out.Body))
		return nil
	}
	body := string(b)
	var filled []Credential
	for _, c := range creds {
		if next := replace(body, c.Query, c.Placeholder, c.Value); next != body {
			body, filled = next, append(filled, c)
		}
	}
	out.Body, out.ContentLength = io.NopCloser(strings.NewReader(body)), int64(len(body))
	return filled
}

// escapings lists s as sent, query- and path-escaped, and JSON-escaped with and without \/; newScrubber pairs a value's list with its placeholder's by index.
func escapings(s string) [5]string {
	j := s
	if strings.ContainsAny(s, "\"\\\t") {
		if q, err := jsontext.AppendQuote(nil, s); err == nil {
			j = string(q[1 : len(q)-1])
		}
	}
	return [5]string{s, url.QueryEscape(s), url.PathEscape(s), j, strings.ReplaceAll(j, "/", `\/`)}
}
