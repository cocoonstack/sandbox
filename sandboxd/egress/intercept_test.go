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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Secret: "gh", Intercept: InterceptAlways}, events)
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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Intercept: InterceptAlways}, nil)
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
		{Host: "api.example.com", Secret: "gh", Intercept: InterceptAlways},
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
		{Host: "api.example.com", Methods: []string{"GET"}, Intercept: InterceptAlways},
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
		{Host: "*.example.com", Methods: []string{"GET"}, Intercept: InterceptAlways},
		{Host: "api.example.com", Methods: []string{"POST"}, Intercept: InterceptAlways},
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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: InterceptAlways}, nil)
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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: InterceptAlways}, nil)
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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Intercept: InterceptAlways}, nil)
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
	p, guestRoots, _ := interceptProxy(t, Rule{Host: "example.com", Intercept: InterceptAlways}, nil)
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
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Secret: "gh", Intercept: InterceptAlways}}}, secrets, events)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "Example.com:443", "example.com", guestRoots)
	defer func() { _ = tc.Close() }()
	header, seen := echoTLS(t, tc, http.MethodGet, "/echo", "", "", map[string]string{"X-Api-Key": "placeholder", "X-Empty": "guest"})

	if seen["key"] != "k1" {
		t.Errorf("upstream saw X-Api-Key %q, want the first claim credential in order", seen["key"])
	}
	if got := header.Get("X-Auth-Seen"); got != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the pool secret kept over the claim's", got)
	}
	if seen["empty"] != "guest" {
		t.Errorf("upstream saw X-Empty %q, want the guest's value kept for an empty credential", seen["empty"])
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
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for sent, want := range map[string]string{"@GIT@": "real", "": "", "@OTHER@": "@OTHER@"} {
		tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		headers := map[string]string{}
		if sent != "" {
			headers["X-Api-Key"] = sent
		}
		header, seen := echoTLS(t, tc, http.MethodGet, "/echo", "", "", headers)
		_ = tc.Close()
		if seen["key"] != want {
			t.Errorf("guest sent %q: upstream saw %q, want %q", sent, seen["key"], want)
		}
		if echo := header.Get("X-Key-Echo"); echo != sent {
			t.Errorf("guest sent %q: the response echo reached it as %q, want what it sent", sent, echo)
		}
	}
}

func TestAnInjectRuleInterceptsOnlyForAClaimWithACredentialForTheHost(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "KEY", Header: "X-Api-Key", Value: "real"}}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	tc := connectTLS(t, front.Listener.Addr().String(), "Example.com:443", "example.com", guestRoots)
	_, seen := echoTLS(t, tc, http.MethodGet, "/echo", "", "", nil)
	_ = tc.Close()
	if seen["key"] != "real" {
		t.Errorf("a host with a credential: upstream saw X-Api-Key %q, want it intercepted and injected", seen["key"])
	}
	tc = connectTLS(t, front.Listener.Addr().String(), "other.test:443", "example.com", trustUpstream(upstream))
	_, seen = echoTLS(t, tc, http.MethodGet, "/echo", "", "", map[string]string{"X-Api-Key": "guest"})
	_ = tc.Close()
	if seen["key"] != "guest" {
		t.Errorf("a host without a credential: upstream saw X-Api-Key %q, want the guest's own value over a splice", seen["key"])
	}
	if _, intercept := New(Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, nil, p.ca, fixedDial(""), nil, nil).tunnelDecision("example.com", 443); intercept {
		t.Error("a claim with no secrets intercepted on an inject rule")
	}
	if d, intercept := p.tunnelDecision("plain.test", 80); d != DecisionAllow || intercept {
		t.Errorf("an inject rule without a credential: decision %v intercept %t, want a plain allow", d, intercept)
	}
}

func TestInterceptSubstitutesAQueryCredentialAndScrubsItsEcho(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "SERP", Query: "api_key", Value: "real key/+", Placeholder: "@SERP@"}}}}
	events := make(chan Event, 8)
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, secrets, events)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for _, tc := range []struct {
		query, want, injected string
	}{
		{"b=2&api_key=%40SERP%40&a=1", "b=2&api_key=real+key%2F%2B&a=1", "claim:SERP"},
		{"api_key=@SERP@", "api_key=real+key%2F%2B", "claim:SERP"},
		{"api_key=other&x=%40SERP%40", "api_key=other&x=%40SERP%40", ""},
		{"", "", ""},
	} {
		conn := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		header, seen := echoTLS(t, conn, http.MethodGet, "/echo?"+tc.query, "", "", nil)
		_ = conn.Close()
		if seen["query"] != tc.want {
			t.Errorf("query %q reached the upstream as %q, want %q", tc.query, seen["query"], tc.want)
		}
		if loc := header.Get("Location"); strings.Contains(loc, "real") {
			t.Errorf("query %q: the guest received Location %q holding the value", tc.query, loc)
		}
		_ = recvEvent(t, events)
		if ev := recvEvent(t, events); ev.Injected != tc.injected {
			t.Errorf("query %q: audit Injected %q, want %q", tc.query, ev.Injected, tc.injected)
		}
	}
}

func TestInterceptSubstitutesABodyCredentialInASmallFormOrJSONBody(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {
		{Name: "FB", Query: "access_token", Body: true, Value: `real "tok"&`, Placeholder: "@FB@"},
		{Name: "URLONLY", Query: "key", Value: "other", Placeholder: "@URL@"},
	}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	big := "access_token=%40FB%40&pad=" + strings.Repeat("x", maxInjectBody)
	for _, tc := range []struct {
		name, ctype, body, want string
	}{
		{"form", "application/x-www-form-urlencoded", "a=1&access_token=%40FB%40&key=%40URL%40", "a=1&access_token=real+%22tok%22%26&key=%40URL%40"},
		{"json", "application/json; charset=utf-8", `{"access_token":"@FB@","n":"@FBX@"}`, `{"access_token":"real \"tok\"&","n":"@FBX@"}`},
		{"other type", "text/plain", "access_token=%40FB%40", "access_token=%40FB%40"},
		{"oversized", "application/x-www-form-urlencoded", big, big},
	} {
		conn := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		_, seen := echoTLS(t, conn, http.MethodPost, "/echo", tc.ctype, tc.body, nil)
		_ = conn.Close()
		if seen["body"] != tc.want || seen["length"] != fmt.Sprint(len(tc.want)) {
			t.Errorf("%s: upstream saw body %.80q length %s, want %.80q length %d", tc.name, seen["body"], seen["length"], tc.want, len(tc.want))
		}
	}
}

func TestInterceptStreamsAnEventStreamAsItArrives(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewTLSServer(sseHandler(release))
	defer upstream.Close()
	defer close(release)
	ca, _ := testCA(t)
	p := New(Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, nil, ca, fixedDial(upstream.Listener.Addr().String()), nil, nil)
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
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Intercept: InterceptAlways}, nil)
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
		w.Header().Set("X-Host-Seen", r.Host)
		if r.URL.Path != "/echo" {
			_, _ = io.WriteString(w, "hello")
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Location", "/next?"+r.URL.RawQuery)
		w.Header().Set("X-Key-Echo", r.Header.Get("X-Api-Key"))
		_, _ = fmt.Fprintf(w, "key=%s\nempty=%s\nquery=%s\nlength=%d\nbody=%s", r.Header.Get("X-Api-Key"), r.Header.Get("X-Empty"), r.URL.RawQuery, r.ContentLength, body)
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

// echoTLS sends one request on tc to the upstream's /echo, which reports what it saw in the body.
func echoTLS(t *testing.T, tc *tls.Conn, method, target, ctype, body string, headers map[string]string) (http.Header, map[string]string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "https://example.com"+target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if err = req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	seen := map[string]string{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok && seen[k] == "" {
			seen[k] = v
		}
	}
	return resp.Header, seen
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
