package egress

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInterceptLeafCaches(t *testing.T) {
	ca, _ := testCA(t)
	p := New(Policy{}, nil, ca, fixedDial("127.0.0.1:1"), nil, nil)
	a, err := p.leafFor("example.com")
	if err != nil {
		t.Fatalf("leaf a: %v", err)
	}
	b, err := p.leafFor("example.com")
	if err != nil {
		t.Fatalf("leaf b: %v", err)
	}
	if a != b {
		t.Error("per-sandbox leaf cache missed for the same host")
	}
}

func TestInterceptLeafClampedToTheIntermediateStaysCached(t *testing.T) {
	ca, _ := testCA(t)
	ca.interCert.NotAfter = time.Now().Add(time.Hour).Truncate(time.Second)
	p := New(Policy{}, nil, ca, fixedDial("127.0.0.1:1"), nil, nil)
	a, err := p.leafFor("example.com")
	if err != nil {
		t.Fatalf("leafFor: %v", err)
	}
	b, err := p.leafFor("example.com")
	if err != nil {
		t.Fatalf("leafFor again: %v", err)
	}
	if a != b {
		t.Error("leaf re-signed although its end is the intermediate's own")
	}
}

func TestInterceptLeafCacheBounded(t *testing.T) {
	ca, _ := testCA(t)
	p := New(Policy{}, nil, ca, fixedDial("127.0.0.1:1"), nil, nil)
	for i := range maxLeaves + 50 {
		if _, err := p.leafFor(fmt.Sprintf("h%d.example.com", i)); err != nil {
			t.Fatalf("leaf %d: %v", i, err)
		}
	}
	if n := len(p.leaves); n > maxLeaves {
		t.Errorf("leaf cache grew to %d, want <= %d", n, maxLeaves)
	}
}

func TestInterceptInjectsSecretIntoHTTPS(t *testing.T) {
	events := make(chan Event, 4)
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Secret: "gh", Intercept: true}, events)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLS(t, tc, http.MethodGet)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Fatalf("got %d %q, want 200 hello", resp.StatusCode, body)
	}
	if seen := resp.Header.Get("X-Auth-Seen"); seen != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the injected secret", seen)
	}
	if seen := resp.Header.Get("X-Host-Seen"); seen != "example.com" {
		t.Errorf("upstream saw Host %q, want the guest's own header preserved", seen)
	}
	if ev := recvEvent(t, events); ev.Method != http.MethodConnect || ev.Host != "example.com" || ev.Decision != DecisionAllow {
		t.Errorf("tunnel audit event = %+v, want CONNECT/example.com/allow", ev)
	}
	if ev := recvEvent(t, events); ev.Method != http.MethodGet || ev.Injected != "gh" || ev.Decision != DecisionAllow {
		t.Errorf("audit event = %+v, want GET/gh/allow", ev)
	}
}

func TestInterceptKeepsAClientHelloSentBeforeThe200(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Intercept: true}, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	raw, err := net.Dial("tcp", front.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = raw.Close() }()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	tc := tls.Client(&earlyConn{Conn: raw, preamble: "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"}, &tls.Config{ServerName: "example.com", RootCAs: guestRoots})
	if err := tc.Handshake(); err != nil {
		t.Fatalf("handshake with the ClientHello sent alongside CONNECT: %v", err)
	}
	resp := roundTripTLS(t, tc, http.MethodGet)
	defer func() { _ = resp.Body.Close() }()
	if body, _ := io.ReadAll(resp.Body); resp.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Errorf("got %d %q, want 200 hello", resp.StatusCode, body)
	}
}

func TestInterceptBindsInnerRequestToInterceptRule(t *testing.T) {
	policy := Policy{Allow: []Rule{
		{Host: "*.example.com"},
		{Host: "api.example.com", Secret: "gh", Intercept: true},
	}}
	p, guestRoots, upstream := interceptProxyPolicy(t, policy, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "api.example.com:443", "api.example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLSTo(t, tc, http.MethodGet, "api.example.com")
	defer func() { _ = resp.Body.Close() }()

	if seen := resp.Header.Get("X-Auth-Seen"); seen != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the intercept rule's secret (shadowing plain rule stole the decision)", seen)
	}
}

func TestInterceptCaptureEnforcesRuleMethod(t *testing.T) {
	policy := Policy{Allow: []Rule{
		{Host: "*.example.com"},
		{Host: "api.example.com", Methods: []string{"GET"}, Intercept: true},
	}}
	p, guestRoots, upstream := interceptProxyPolicy(t, policy, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "api.example.com:443", "api.example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLSTo(t, tc, http.MethodPost, "api.example.com")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST over the intercepted tunnel got %d, want 403 (intercept rule is GET-only)", resp.StatusCode)
	}
}

func TestInterceptReachesLaterInterceptRuleByMethod(t *testing.T) {
	policy := Policy{Allow: []Rule{
		{Host: "*.example.com", Methods: []string{"GET"}, Intercept: true},
		{Host: "api.example.com", Methods: []string{"POST"}, Intercept: true},
	}}
	p, guestRoots, upstream := interceptProxyPolicy(t, policy, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "api.example.com:443", "api.example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLSTo(t, tc, http.MethodPost, "api.example.com")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST got %d, want 200 (the POST-only intercept rule must be reachable)", resp.StatusCode)
	}
}

func TestInterceptForcesAuthorityOnForeignHost(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: true}, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLSHost(t, tc, http.MethodGet, "evil.example.net")
	defer func() { _ = resp.Body.Close() }()

	if seen := resp.Header.Get("X-Host-Seen"); seen != "example.com:443" {
		t.Errorf("foreign Host forwarded as %q, want the CONNECT authority", seen)
	}
}

func TestCloseRevokesInterceptedSession(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: true}, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLS(t, tc, http.MethodGet)
	_ = resp.Body.Close()

	p.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if err := req.Write(tc); err == nil {
		revived, err := http.ReadResponse(bufio.NewReader(tc), req)
		if err == nil {
			_ = revived.Body.Close()
			t.Error("established intercepted session survived proxy Close")
		}
	}
}

func TestInterceptFiltersByMethod(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Intercept: true}, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLS(t, tc, http.MethodPost)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST over intercepted TLS got %d, want 403 (method filtered)", resp.StatusCode)
	}
}

func TestInterceptRejectsUntrustedUpstream(t *testing.T) {
	p, guestRoots, _ := interceptProxy(t, Rule{Host: "example.com", Intercept: true}, nil)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	resp := roundTripTLS(t, tc, http.MethodGet)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("untrusted upstream got %d, want 502 (proxy verifies against real roots)", resp.StatusCode)
	}
}

func TestInterceptInjectsClaimCredentialsBelowThePoolSecret(t *testing.T) {
	events := make(chan Event, 4)
	secrets := credSecrets{fakeSecrets: fakeSecrets{"gh": {"Authorization", "Bearer SECRET"}}, creds: map[string][]Credential{"example.com": {
		{Name: "API", Header: "X-Api-Key", Value: "k1"},
		{Name: "AUTH", Header: "authorization", Value: "claim"},
		{Name: "EMPTY", Header: "X-Empty", Value: ""},
		{Name: "LATER", Header: "X-Api-Key", Value: "k2"},
	}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Secret: "gh", Intercept: true}}}, secrets, events)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "Example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Api-Key", "placeholder")
	req.Header.Set("X-Empty", "guest")
	if err = req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("X-Key-Seen"); got != "k1" {
		t.Errorf("upstream saw X-Api-Key %q, want the first claim credential in order", got)
	}
	if got := resp.Header.Get("X-Auth-Seen"); got != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the pool secret kept over the claim's", got)
	}
	if got := resp.Header.Get("X-Empty-Seen"); got != "guest" {
		t.Errorf("upstream saw X-Empty %q, want the guest's value kept for an empty credential", got)
	}
	_ = recvEvent(t, events)
	if ev := recvEvent(t, events); ev.Injected != "gh,claim:API" {
		t.Errorf("audit Injected %q, want gh,claim:API", ev.Injected)
	}
}

func TestInterceptInjectsAPlaceholderCredentialOnlyWhereTheGuestSentIt(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {
		{Name: "GIT", Header: "X-Api-Key", Value: "real", Placeholder: "@GIT@"},
	}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Intercept: true}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for sent, want := range map[string]string{"@GIT@": "real", "": "", "@OTHER@": "@OTHER@"} {
		tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/x", nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if sent != "" {
			req.Header.Set("X-Api-Key", sent)
		}
		if err = req.Write(tc); err != nil {
			t.Fatalf("write request: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(tc), req)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		_ = resp.Body.Close()
		_ = tc.Close()
		if got := resp.Header.Get("X-Key-Seen"); got != want {
			t.Errorf("guest sent %q: upstream saw %q, want %q", sent, got, want)
		}
	}
}

func TestInterceptStreamsAnEventStreamAsItArrives(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewTLSServer(sseHandler(release))
	defer upstream.Close()
	defer close(release)
	ca, _ := testCA(t)
	p := New(Policy{Allow: []Rule{{Host: "example.com", Intercept: true}}}, nil, ca, fixedDial(upstream.Listener.Addr().String()), nil, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	guestRoots := x509.NewCertPool()
	guestRoots.AppendCertsFromPEM(ca.CertPEM())
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	_ = tc.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp := roundTripTLS(t, tc, http.MethodGet)
	defer func() { _ = resp.Body.Close() }()
	if line, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil || line != "data: one\n" {
		t.Errorf("first event %q, %v; want it before the origin ends the stream", line, err)
	}
}

func TestInterceptResumesTheGuestsTLSSession(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: true}, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()
	cache := tls.NewLRUClientSessionCache(4)
	for i := range 2 {
		tc := connectTLSCached(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots, cache)
		resp := roundTripTLS(t, tc, http.MethodGet)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resumed := tc.ConnectionState().DidResume; resumed != (i == 1) {
			t.Errorf("connection %d resumed=%v, want only the second to resume", i, resumed)
		}
		_ = tc.Close()
	}
}

func interceptProxy(t *testing.T, rule Rule, events chan Event) (*Proxy, *x509.CertPool, *httptest.Server) {
	return interceptProxyPolicy(t, Policy{Allow: []Rule{rule}}, events)
}

func interceptProxyPolicy(t *testing.T, policy Policy, events chan Event) (*Proxy, *x509.CertPool, *httptest.Server) {
	t.Helper()
	return interceptProxySecrets(t, policy, fakeSecrets{"gh": {"Authorization", "Bearer SECRET"}}, events)
}

func interceptProxySecrets(t *testing.T, policy Policy, secrets Secrets, events chan Event) (*Proxy, *x509.CertPool, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Auth-Seen", r.Header.Get("Authorization"))
		w.Header().Set("X-Key-Seen", r.Header.Get("X-Api-Key"))
		w.Header().Set("X-Empty-Seen", r.Header.Get("X-Empty"))
		w.Header().Set("X-Host-Seen", r.Host)
		_, _ = io.WriteString(w, "hello")
	}))
	t.Cleanup(upstream.Close)

	ca, _ := testCA(t)
	audit := func(ev Event) {
		if events != nil {
			events <- ev
		}
	}
	p := New(policy, secrets, ca, fixedDial(upstream.Listener.Addr().String()), audit, nil)

	guestRoots := x509.NewCertPool()
	if !guestRoots.AppendCertsFromPEM(ca.CertPEM()) {
		t.Fatal("guest trust: append ca cert")
	}
	return p, guestRoots, upstream
}

func trustUpstream(upstream *httptest.Server) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	return pool
}

func connectTLS(tb testing.TB, proxyAddr, target, serverName string, roots *x509.CertPool) *tls.Conn {
	tb.Helper()
	return connectTLSCached(tb, proxyAddr, target, serverName, roots, nil)
}

func connectTLSCached(tb testing.TB, proxyAddr, target, serverName string, roots *x509.CertPool, cache tls.ClientSessionCache) *tls.Conn {
	tb.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		tb.Fatalf("dial proxy: %v", err)
	}
	if _, err := io.WriteString(conn, "CONNECT "+target+" HTTP/1.1\r\nHost: "+target+"\r\n\r\n"); err != nil {
		tb.Fatalf("write CONNECT: %v", err)
	}
	if status := readPreamble(tb, conn); !strings.Contains(status, "200") {
		tb.Fatalf("CONNECT status = %q, want 200", status)
	}
	tc := tls.Client(conn, &tls.Config{ServerName: serverName, RootCAs: roots, ClientSessionCache: cache})
	if err := tc.Handshake(); err != nil {
		tb.Fatalf("guest tls handshake: %v", err)
	}
	return tc
}

func roundTripTLS(t *testing.T, tc *tls.Conn, method string) *http.Response {
	t.Helper()
	return roundTripTLSHost(t, tc, method, "")
}

func roundTripTLSTo(t *testing.T, tc *tls.Conn, method, host string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "https://"+host+"/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if err = req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp
}

func roundTripTLSHost(t *testing.T, tc *tls.Conn, method, host string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "https://example.com/x", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if host != "" {
		req.Host = host
	}
	if err = req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp
}

func readPreamble(tb testing.TB, conn net.Conn) string {
	tb.Helper()
	var sb strings.Builder
	var b [1]byte
	for sb.Len() < 512 {
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			tb.Fatalf("read CONNECT preamble: %v", err)
		}
		sb.WriteByte(b[0])
		if strings.HasSuffix(sb.String(), "\r\n\r\n") {
			break
		}
	}
	return sb.String()
}

type earlyConn struct {
	net.Conn
	preamble string
	answered bool
}

func (c *earlyConn) Write(b []byte) (int, error) {
	if c.preamble == "" {
		return c.Conn.Write(b)
	}
	if _, err := io.WriteString(c.Conn, c.preamble+string(b)); err != nil {
		return 0, err
	}
	c.preamble = ""
	return len(b), nil
}

func (c *earlyConn) Read(b []byte) (int, error) {
	if !c.answered {
		var reply strings.Builder
		var one [1]byte
		for !strings.HasSuffix(reply.String(), "\r\n\r\n") {
			if _, err := io.ReadFull(c.Conn, one[:]); err != nil {
				return 0, err
			}
			reply.WriteByte(one[0])
		}
		c.answered = true
	}
	return c.Conn.Read(b)
}
