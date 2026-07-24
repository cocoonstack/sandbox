package egress

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkSignLeaf(b *testing.B) {
	ca := benchCA(b)
	for b.Loop() {
		if _, err := ca.SignLeaf("bench.example.com"); err != nil {
			b.Fatalf("sign: %v", err)
		}
	}
}

func BenchmarkInterceptedRequest(b *testing.B) {
	addr, roots := benchFront(b, true)
	tc := benchSession(b, addr, roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkSplicedRequest(b *testing.B) {
	addr, roots := benchFront(b, false)
	tc := benchSession(b, addr, roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkInterceptedHandshake(b *testing.B) {
	addr, roots := benchFront(b, true)
	for b.Loop() {
		tc := benchSession(b, addr, roots)
		_ = tc.Close()
	}
}

func BenchmarkSplicedHandshake(b *testing.B) {
	addr, roots := benchFront(b, false)
	for b.Loop() {
		tc := benchSession(b, addr, roots)
		_ = tc.Close()
	}
}

func benchCA(b *testing.B) *CA {
	b.Helper()
	rootCert, rootKey, err := GenerateRoot("bench root")
	if err != nil {
		b.Fatalf("root: %v", err)
	}
	interCert, interKey, err := IssueIntermediate(rootCert, rootKey, "bench")
	if err != nil {
		b.Fatalf("intermediate: %v", err)
	}
	ca, err := LoadCA(rootCert, interCert, interKey)
	if err != nil {
		b.Fatalf("load ca: %v", err)
	}
	return ca
}

// benchFront serves an HTTPS echo behind the proxy; intercept toggles the rule,
// and guest roots match: the cluster root when intercepted, the origin's own
// cert when spliced.
func benchFront(b *testing.B, intercept bool) (proxyAddr string, roots *x509.CertPool) {
	b.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	b.Cleanup(upstream.Close)
	rule := Rule{Host: "example.com", Intercept: intercept}
	var ca *CA
	roots = x509.NewCertPool()
	if intercept {
		ca = benchCA(b)
		if !roots.AppendCertsFromPEM(ca.CertPEM()) {
			b.Fatal("append cluster root")
		}
	} else {
		roots.AddCert(upstream.Certificate())
	}
	p := New("sb_b", "", Policy{Allow: []Rule{rule}}, nil, ca, fixedDial(upstream.Listener.Addr().String()), nil)
	if ca != nil {
		p.mitmTr.TLSClientConfig.RootCAs = func() *x509.CertPool {
			pool := x509.NewCertPool()
			pool.AddCert(upstream.Certificate())
			return pool
		}()
	}
	front := httptest.NewServer(p)
	b.Cleanup(front.Close)
	return front.Listener.Addr().String(), roots
}

func benchSession(b *testing.B, proxyAddr string, roots *x509.CertPool) *tls.Conn {
	b.Helper()
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		b.Fatalf("dial proxy: %v", err)
	}
	if _, err := io.WriteString(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"); err != nil {
		b.Fatalf("write CONNECT: %v", err)
	}
	buf := make([]byte, 1)
	preamble := make([]byte, 0, 64)
	for {
		if _, err := io.ReadFull(conn, buf); err != nil {
			b.Fatalf("read CONNECT reply: %v", err)
		}
		preamble = append(preamble, buf[0])
		if len(preamble) >= 4 && string(preamble[len(preamble)-4:]) == "\r\n\r\n" {
			break
		}
	}
	tc := tls.Client(conn, &tls.Config{ServerName: "example.com", RootCAs: roots})
	if err := tc.Handshake(); err != nil {
		b.Fatalf("handshake: %v", err)
	}
	return tc
}

func benchRoundTrip(b *testing.B, tc *tls.Conn, br *bufio.Reader) {
	b.Helper()
	req, err := http.NewRequest(http.MethodGet, "https://example.com/x", nil)
	if err != nil {
		b.Fatalf("build request: %v", err)
	}
	if err = req.Write(tc); err != nil {
		b.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		b.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
