package egress

import (
	"bufio"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	req := newRequest(t, http.MethodGet, "/echo", "")
	req.Header.Set("X-Api-Key", "placeholder")
	req.Header.Set("X-Empty", "guest")
	r := send(t, tc, req)
	seen, _ := r.seen(t)

	if seen.Key != "k1" {
		t.Errorf("upstream saw X-Api-Key %q, want the first claim credential in order", seen.Key)
	}
	if got := r.header.Get("X-Auth-Seen"); got != "Bearer SECRET" {
		t.Errorf("upstream saw Authorization %q, want the pool secret kept over the claim's", got)
	}
	if seen.Empty != "guest" {
		t.Errorf("upstream saw X-Empty %q, want the guest's value kept for an empty credential", seen.Empty)
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
		req := newRequest(t, http.MethodGet, "/echo", "")
		if sent != "" {
			req.Header.Set("X-Api-Key", sent)
		}
		seen, _ := send(t, tc, req).seen(t)
		_ = tc.Close()
		if seen.Key != want {
			t.Errorf("guest sent %q: upstream saw %q, want %q", sent, seen.Key, want)
		}
	}
}

func TestAnInjectRuleInterceptsOnlyForAClaimWithACredentialForTheHost(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "KEY", Header: "X-Api-Key", Value: "real"}}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for _, tt := range []struct {
		target    string
		roots     *x509.CertPool
		sent, key string
	}{
		{"Example.com:443", guestRoots, "", "real"},
		{"other.test:443", trustUpstream(upstream), "guest", "guest"},
	} {
		tc := connectTLS(t, front.Listener.Addr().String(), tt.target, "example.com", tt.roots)
		req := newRequest(t, http.MethodGet, "/echo", "")
		if tt.sent != "" {
			req.Header.Set("X-Api-Key", tt.sent)
		}
		seen, _ := send(t, tc, req).seen(t)
		_ = tc.Close()
		if seen.Key != tt.key {
			t.Errorf("CONNECT %s: upstream saw X-Api-Key %q, want %q", tt.target, seen.Key, tt.key)
		}
	}
}

func TestTunnelDecisionOnAnInjectRule(t *testing.T) {
	ca, _ := testCA(t)
	creds := credSecrets{creds: map[string][]Credential{
		"empty.test": {{Name: "E", Header: "X-Key", Value: ""}},
		"graph.test": {{Name: "G", Query: "access_token", Value: "g", Placeholder: "@G@"}},
	}}
	for _, tt := range []struct {
		name      string
		secrets   Secrets
		host      string
		injects   bool
		intercept bool
	}{
		{"a claim with no secrets", nil, "graph.test", true, false},
		{"an empty credential", creds, "empty.test", true, false},
		{"a host without a credential", creds, "plain.test", true, false},
		{"a credential on the proxy door", creds, "graph.test", true, true},
		{"a credential on the SOCKS door", creds, "graph.test", false, false},
	} {
		p := New(Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, tt.secrets, ca, fixedDial(""), nil, nil)
		if d, intercept := p.tunnelDecision(tt.host, 443, tt.injects); d != DecisionAllow || intercept != tt.intercept {
			t.Errorf("%s: decision %v intercept %t, want an allow with intercept %t", tt.name, d, intercept, tt.intercept)
		}
	}
}

func TestAnAlwaysRuleOutranksAnEarlierInjectRule(t *testing.T) {
	ca, _ := testCA(t)
	policy := Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}, {Host: "api.github.com", Methods: []string{"GET"}, Secret: "gh", Intercept: InterceptAlways}}}
	if _, intercept := New(policy, fakeSecrets{}, ca, fixedDial(""), nil, nil).tunnelDecision("api.github.com", 443, true); !intercept {
		t.Error("a claim without a credential spliced a host an always rule intercepts")
	}
	for _, tt := range []struct {
		host, method string
		decision     Decision
		mode         InterceptMode
	}{
		{"api.github.com", http.MethodGet, DecisionAllow, InterceptAlways},
		{"api.github.com", http.MethodPost, DecisionDeny, InterceptOff},
		{"graph.test", http.MethodPost, DecisionAllow, InterceptInject},
	} {
		if rule, d := policy.EvalInner(tt.host, tt.method, 443); d != tt.decision || rule.Intercept != tt.mode {
			t.Errorf("EvalInner %s %s = %+v/%v, want %v with mode %d", tt.method, tt.host, rule, d, tt.decision, tt.mode)
		}
	}
}

func TestInterceptSubstitutesAQueryCredentialAndScrubsItsEcho(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "SERP", Query: "api_key", Value: "real key/+", Placeholder: "@SERP@"}}}}
	events := make(chan Event, 8)
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "example.com", Intercept: InterceptAlways}}}, secrets, events)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for _, tt := range []struct {
		query, want, injected string
	}{
		{"b=2&api_key=%40SERP%40&a=1", "b=2&api_key=real+key%2F%2B&a=1", "claim:SERP"},
		{"api_key=@SERP@&api_key=%40SERP%40", "api_key=real+key%2F%2B&api_key=%40SERP%40", "claim:SERP"},
		{"api_key=other&x=%40SERP%40", "api_key=other&x=%40SERP%40", ""},
		{"", "", ""},
	} {
		tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		req := newRequest(t, http.MethodGet, "/echo?"+tt.query, "")
		if tt.injected != "" {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		r := send(t, tc, req)
		_ = tc.Close()
		seen, reflected := r.seen(t)
		if seen.Query != tt.want {
			t.Errorf("query %q reached the upstream as %q, want %q", tt.query, seen.Query, tt.want)
		}
		if loc := r.header.Get("Location"); strings.Contains(loc, "real") || strings.Contains(reflected, "real") {
			t.Errorf("query %q: the guest received Location %q and body %q, one holding the value", tt.query, loc, reflected)
		}
		_ = recvEvent(t, events)
		if ev := recvEvent(t, events); ev.Injected != tt.injected {
			t.Errorf("query %q: audit Injected %q, want %q", tt.query, ev.Injected, tt.injected)
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
	for _, tt := range []struct {
		name, ctype, body, want string
	}{
		{"form", "application/x-www-form-urlencoded", "a=1&access_token=%40FB%40&key=%40URL%40&access_token=%40FB%40", "a=1&access_token=real+%22tok%22%26&key=%40URL%40&access_token=%40FB%40"},
		{"json", "application/json; charset=utf-8", `{"message":"@FB@", "access_token":"@FB@","n":{"access_token": "@FB@"},"a":["@FB@"]}`, `{"message":"@FB@", "access_token":"real \"tok\"&","n":{"access_token": "@FB@"},"a":["@FB@"]}`},
		{"other type", "text/plain", "access_token=%40FB%40", "access_token=%40FB%40"},
		{"oversized", "application/x-www-form-urlencoded", big, big},
	} {
		tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		req := newRequest(t, http.MethodPost, "/echo", tt.body)
		req.Header.Set("Content-Type", tt.ctype)
		seen, _ := send(t, tc, req).seen(t)
		_ = tc.Close()
		if seen.Body != tt.want || seen.Length != int64(len(tt.want)) {
			t.Errorf("%s: upstream saw body %.80q length %d, want %.80q length %d", tt.name, seen.Body, seen.Length, tt.want, len(tt.want))
		}
	}
}

func TestACredentialFillsOnlyTheFirstMatchingParameter(t *testing.T) {
	value := strings.Repeat("v", 8<<10)
	form := strings.Repeat("access_token=%40FB%40&", 3600)
	if got := replaceParam(form, "access_token", "@FB@", value); len(got) != len(form)-len("%40FB%40")+len(value) {
		t.Errorf("3600 repeated pairs grew the form to %d bytes, want one fill on %d", len(got), len(form))
	}
	doc := `{"access_token":"@FB@","access_token":"@FB@"}`
	if got := replaceMember(doc, "access_token", "@FB@", "v"); got != `{"access_token":"v","access_token":"@FB@"}` {
		t.Errorf("a repeated member filled as %s, want only the first", got)
	}
}

func TestARangeCannotSplitAScrubbedQueryValue(t *testing.T) {
	secrets := credSecrets{creds: map[string][]Credential{"example.com": {{Name: "K", Query: "api_key", Value: "SECRETVALUE0123456789", Placeholder: "@K@"}}}}
	p, guestRoots, upstream := interceptProxySecrets(t, Policy{Allow: []Rule{{Host: "*", Intercept: InterceptInject}}}, secrets, nil)
	p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	front := httptest.NewServer(p)
	defer front.Close()

	for _, byteRange := range []string{"bytes=0-9", "bytes=10-20", "bytes=0-3,10-13"} {
		tc := connectTLS(t, front.Listener.Addr().String(), "example.com:443", "example.com", guestRoots)
		req := newRequest(t, http.MethodGet, "/range?api_key=%40K%40", "")
		req.Header.Set("Range", byteRange)
		r := send(t, tc, req)
		_ = tc.Close()
		if r.status != http.StatusOK || r.body != "@K@" {
			t.Errorf("Range %s: guest got %d %q, want the whole value scrubbed to its placeholder", byteRange, r.status, r.body)
		}
	}
}

func TestAScrubberCoversTheCommonEscapings(t *testing.T) {
	value := "ab cd/ef<g"
	sc := newScrubber([]Credential{{Name: "K", Query: "k", Value: value, Placeholder: "@K@"}})
	echoes := strings.Join([]string{value, url.QueryEscape(value), url.PathEscape(value), `ab cd\/ef<g`}, "|")
	out, _ := io.ReadAll(&scrubReader{src: strings.NewReader(echoes), sc: sc})
	h := http.Header{"Location": {"/next?k=" + url.QueryEscape(value)}}
	sc.header(h)
	if strings.Contains(string(out), "cd") || strings.Contains(h.Get("Location"), "cd") {
		t.Errorf("scrubbed body %q and Location %q, want every escaping of the value replaced", out, h.Get("Location"))
	}
}

func TestScrubReaderReplacesAcrossChunksAndHoldsOnlyAPossibleMatch(t *testing.T) {
	sc := newScrubber([]Credential{{Name: "S", Query: "s", Value: "SECRET", Placeholder: "@S@"}})
	for _, tt := range []struct {
		name   string
		chunks []string
		first  string
		all    string
	}{
		{"split match", []string{"a SEC", "RET b"}, "a ", "a @S@ b"},
		{"event tail", []string{"data: x\n\n", "data: SECRET\n\n"}, "data: x\n\n", "data: x\n\ndata: @S@\n\n"},
		{"prefix at the end", []string{"x SECR"}, "x ", "x SECR"},
	} {
		readers := make([]io.Reader, len(tt.chunks))
		for i, c := range tt.chunks {
			readers[i] = strings.NewReader(c)
		}
		s := &scrubReader{src: io.MultiReader(readers...), sc: sc}
		buf := make([]byte, 64)
		n, _ := s.Read(buf)
		if got := string(buf[:n]); got != tt.first {
			t.Errorf("%s: first read %q, want %q", tt.name, got, tt.first)
		}
		rest, _ := io.ReadAll(s)
		if got := string(buf[:n]) + string(rest); got != tt.all {
			t.Errorf("%s: scrubbed %q, want %q", tt.name, got, tt.all)
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
		if r.URL.Path == "/range" {
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(r.URL.Query().Get("api_key")))
			return
		}
		if r.URL.Path != "/echo" {
			_, _ = io.WriteString(w, "hello")
			return
		}
		body, _ := io.ReadAll(r.Body)
		var out io.Writer = w
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			defer func() { _ = gz.Close() }()
			out = gz
		}
		w.Header().Set("Location", "/next?"+r.URL.RawQuery)
		report, _ := json.Marshal(echoed{Key: r.Header.Get("X-Api-Key"), Empty: r.Header.Get("X-Empty"), Query: r.URL.RawQuery, Body: string(body), Length: r.ContentLength})
		_, _ = fmt.Fprintf(out, "%x\n%s", report, r.URL.RawQuery)
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

func newRequest(t *testing.T, method, target, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "https://example.com"+target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return req
}

func send(t *testing.T, tc *tls.Conn, req *http.Request) reply {
	t.Helper()
	if err := req.Write(tc); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tc), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return reply{status: resp.StatusCode, header: resp.Header, body: string(body)}
}

type echoed struct {
	Key, Empty, Query, Body string
	Length                  int64
}

type reply struct {
	status int
	header http.Header
	body   string
}

// seen decodes the upstream's /echo report; its hex survives the scrub, the query reflected after it does not.
func (r reply) seen(t *testing.T) (echoed, string) {
	t.Helper()
	report, reflected, _ := strings.Cut(r.body, "\n")
	var e echoed
	if raw, err := hex.DecodeString(report); err != nil || json.Unmarshal(raw, &e) != nil {
		t.Fatalf("echo report %q", r.body)
	}
	return e, reflected
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
