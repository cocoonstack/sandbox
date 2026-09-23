package egress

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkSignLeaf(b *testing.B) {
	ca := testCAOnly(b)
	for b.Loop() {
		if _, err := ca.SignLeaf("bench.example.com"); err != nil {
			b.Fatalf("sign: %v", err)
		}
	}
}

func BenchmarkInterceptedRequest(b *testing.B) {
	addr, roots := benchFront(b, true)
	tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkSplicedRequest(b *testing.B) {
	addr, roots := benchFront(b, false)
	tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkInterceptedHandshake(b *testing.B) {
	addr, roots := benchFront(b, true)
	for b.Loop() {
		tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
		_ = tc.Close()
	}
}

func BenchmarkSplicedHandshake(b *testing.B) {
	addr, roots := benchFront(b, false)
	for b.Loop() {
		tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
		_ = tc.Close()
	}
}

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
		ca = testCAOnly(b)
		if !roots.AppendCertsFromPEM(ca.CertPEM()) {
			b.Fatal("append cluster root")
		}
	} else {
		roots.AddCert(upstream.Certificate())
	}
	p := New(Policy{Allow: []Rule{rule}}, nil, ca, fixedDial(upstream.Listener.Addr().String()), nil, nil)
	if ca != nil {
		p.mitmTr.TLSClientConfig.RootCAs = trustUpstream(upstream)
	}
	front := httptest.NewServer(p)
	b.Cleanup(front.Close)
	return front.Listener.Addr().String(), roots
}

func benchRoundTrip(b *testing.B, tc *tls.Conn, br *bufio.Reader) {
	b.Helper()
	req, err := http.NewRequestWithContext(b.Context(), http.MethodGet, "https://example.com/x", nil)
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
