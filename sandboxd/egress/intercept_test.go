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
)

func TestInterceptLeafCaches(t *testing.T) {
	ca, _ := testCA(t)
	p := New("s", "", Policy{}, nil, ca, fixedDial("127.0.0.1:1"), nil)
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

func TestInterceptLeafCacheBounded(t *testing.T) {
	ca, _ := testCA(t)
	p := New("s", "", Policy{}, nil, ca, fixedDial("127.0.0.1:1"), nil)
	for i := range maxLeaves + 50 {
		if _, err := p.leafFor(fmt.Sprintf("h%d.example.com", i)); err != nil {
			t.Fatalf("leaf %d: %v", i, err)
		}
	}
	if n := len(p.leaves); n > maxLeaves {
		t.Errorf("leaf cache grew to %d, want <= %d", n, maxLeaves)
	}
}

func interceptProxy(t *testing.T, rule Rule, events chan Event) (*Proxy, *x509.CertPool, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Auth-Seen", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "hello")
	}))
	t.Cleanup(upstream.Close)

	rootCert, rootKey, err := GenerateRoot("test root")
	if err != nil {
		t.Fatalf("generate root: %v", err)
	}
	interCert, interKey, err := IssueIntermediate(rootCert, rootKey, "node1")
	if err != nil {
		t.Fatalf("issue intermediate: %v", err)
	}
	ca, err := LoadCA(rootCert, interCert, interKey)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	secrets := fakeSecrets{"gh": {"Authorization", "Bearer SECRET"}}
	audit := func(ev Event) {
		if events != nil {
			events <- ev
		}
	}
	p := New("sb_1", "acme", Policy{Allow: []Rule{rule}}, secrets, ca, fixedDial(upstream.Listener.Addr().String()), audit)

	guestRoots := x509.NewCertPool()
	if !guestRoots.AppendCertsFromPEM(ca.CertPEM()) {
		t.Fatal("guest trust: append ca cert")
	}
	return p, guestRoots, upstream
}

func TestInterceptInjectsSecretIntoHTTPS(t *testing.T) {
	events := make(chan Event, 4)
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Secret: "gh", Intercept: true}, events)
	p.mitmTr.TLSClientConfig.RootCAs = trustUpstream(upstream)
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
	if ev := recvEvent(t, events); ev.Method != http.MethodGet || ev.Injected != "gh" || ev.Decision != DecisionAllow {
		t.Errorf("audit event = %+v, want GET/gh/allow", ev)
	}
}

func TestInterceptFiltersByMethod(t *testing.T) {
	p, guestRoots, upstream := interceptProxy(t, Rule{Host: "example.com", Methods: []string{"GET"}, Intercept: true}, nil)
	p.mitmTr.TLSClientConfig.RootCAs = trustUpstream(upstream)
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

func trustUpstream(upstream *httptest.Server) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	return pool
}

// connectTLS opens a CONNECT tunnel through the proxy and completes the guest
// TLS handshake against the leaf the proxy presents.
func connectTLS(t *testing.T, proxyAddr, target, serverName string, roots *x509.CertPool) *tls.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	if _, err := io.WriteString(conn, "CONNECT "+target+" HTTP/1.1\r\nHost: "+target+"\r\n\r\n"); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	if status := readPreamble(t, conn); !strings.Contains(status, "200") {
		t.Fatalf("CONNECT status = %q, want 200", status)
	}
	tc := tls.Client(conn, &tls.Config{ServerName: serverName, RootCAs: roots})
	if err := tc.Handshake(); err != nil {
		t.Fatalf("guest tls handshake: %v", err)
	}
	return tc
}

func roundTripTLS(t *testing.T, tc *tls.Conn, method string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, "https://example.com/x", nil)
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

// readPreamble reads the CONNECT reply byte-wise up to the blank line, so no
// TLS bytes are consumed; the proxy sends nothing more until the ClientHello.
func readPreamble(t *testing.T, conn net.Conn) string {
	t.Helper()
	var sb strings.Builder
	var b [1]byte
	for sb.Len() < 512 {
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("read CONNECT preamble: %v", err)
		}
		sb.WriteByte(b[0])
		if strings.HasSuffix(sb.String(), "\r\n\r\n") {
			break
		}
	}
	return sb.String()
}
