package egress

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

const benchKey = "EAABsbCS1iHgBAKZBZCx9ZAZBe7ZAZCYqZBfZC0ZD"

func BenchmarkSignLeaf(b *testing.B) {
	ca := testCAOnly(b)
	for b.Loop() {
		if _, err := ca.signLeaf("bench.example.com"); err != nil {
			b.Fatalf("sign: %v", err)
		}
	}
}

func BenchmarkInterceptedRequest(b *testing.B) {
	addr, roots := benchFront(b, InterceptAlways)
	tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkSplicedRequest(b *testing.B) {
	addr, roots := benchFront(b, InterceptOff)
	tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
	defer func() { _ = tc.Close() }()
	br := bufio.NewReader(tc)
	for b.Loop() {
		benchRoundTrip(b, tc, br)
	}
}

func BenchmarkInterceptedHandshake(b *testing.B) {
	addr, roots := benchFront(b, InterceptAlways)
	for b.Loop() {
		tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
		_ = tc.Close()
	}
}

func BenchmarkInterceptedConnection(b *testing.B) {
	for _, arm := range []string{"fresh", "session-cache"} {
		b.Run(arm, func(b *testing.B) {
			addr, roots := benchFront(b, InterceptAlways)
			var cache tls.ClientSessionCache
			if arm == "session-cache" {
				cache = tls.NewLRUClientSessionCache(8)
			}
			resumed := 0
			for b.Loop() {
				tc := connectTLSCached(b, addr, "example.com:443", "example.com", roots, cache)
				benchRoundTrip(b, tc, bufio.NewReader(tc))
				if tc.ConnectionState().DidResume {
					resumed++
				}
				_ = tc.Close()
			}
			b.ReportMetric(float64(resumed)/float64(b.N), "resumed/op")
		})
	}
}

func BenchmarkSplicedHandshake(b *testing.B) {
	addr, roots := benchFront(b, InterceptOff)
	for b.Loop() {
		tc := connectTLS(b, addr, "example.com:443", "example.com", roots)
		_ = tc.Close()
	}
}

func BenchmarkTunnelDecision(b *testing.B) {
	ca := testCAOnly(b)
	creds := map[string][]Credential{"api.example.com": {{Name: "KEY", Header: "X-Key", Value: "k"}}}
	for _, arm := range []struct {
		name string
		mode InterceptMode
	}{{"plain", InterceptOff}, {"inject", InterceptInject}, {"always", InterceptAlways}} {
		p := New(Policy{Allow: []Rule{{Host: "*", Intercept: arm.mode}}}, credSecrets{creds: creds}, ca, fixedDial(""), nil, nil)
		for _, host := range []string{"other.example.com", "api.example.com"} {
			b.Run(arm.name+"/"+host, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_, _ = p.tunnelDecision(host, 443, true)
				}
			})
		}
	}
}

func BenchmarkInjectAndScrubHeaders(b *testing.B) {
	base := newRequest(b, http.MethodPost, "/v19.0/me", "")
	for _, arm := range []struct {
		name, query, ctype, body string
		cred                     Credential
	}{
		{"no-credential", "a=1", "", "", Credential{}},
		{"header", "a=1", "", "", Credential{Name: "K", Header: "X-Key", Value: benchKey, Placeholder: "@K@"}},
		{"query", "a=1&access_token=%40K%40", "", "", Credential{Name: "K", Query: "access_token", Value: benchKey, Placeholder: "@K@"}},
		{"form-body", "", "application/x-www-form-urlencoded", "m=hi&access_token=%40K%40", Credential{Name: "K", Query: "access_token", Body: true, Value: benchKey, Placeholder: "@K@"}},
		{"json-body", "", "application/json", `{"m":"hi","access_token":"@K@"}`, Credential{Name: "K", Query: "access_token", Body: true, Value: benchKey, Placeholder: "@K@"}},
		{"json-body-escaped-unnamed", "", "application/json", `{"message":"` + strings.Repeat(`hello\nworld `, 150) + `"}`, Credential{Name: "K", Query: "access_token", Body: true, Value: benchKey, Placeholder: "@K@"}},
	} {
		creds := map[string][]Credential{}
		if arm.cred.Name != "" {
			creds["example.com"] = []Credential{arm.cred}
		}
		p := New(Policy{}, credSecrets{creds: creds}, nil, fixedDial(""), nil, nil)
		b.Run(arm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out := base.Clone(b.Context())
				out.URL.RawQuery = arm.query
				if arm.ctype != "" {
					out.Header.Set("Content-Type", arm.ctype)
					out.Body, out.ContentLength = io.NopCloser(strings.NewReader(arm.body)), int64(len(arm.body))
				}
				if _, sc := p.inject(Rule{}, out, "example.com", true); sc != nil {
					sc.header(http.Header{"Location": {"/next?access_token=" + benchKey}, "Content-Type": {"application/json"}, "Date": {"x"}})
				}
			}
		})
	}
}

func BenchmarkScrubBody(b *testing.B) {
	sc := newScrubber([]Credential{{Name: "K", Query: "access_token", Value: benchKey, Placeholder: "@K@"}})
	page := bytes.Repeat([]byte(`{"id":"1234567890","message":"hello world","created_time":"2026-10-04T00:00:00+0000"},`), 64<<10/88)
	page = append(page, `"paging":{"next":"https:\/\/graph.facebook.com\/v19.0\/me\/feed?access_token=`+benchKey+`&after=x"}`...)
	event := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello there\"}}]}\n\n")
	for _, arm := range []struct {
		name string
		src  func() io.Reader
	}{
		{"64KiB-page", func() io.Reader { return bytes.NewReader(page) }},
		{"sse-256-events", func() io.Reader {
			return chunked(slices.Repeat([]string{string(event)}, 256)...)
		}},
	} {
		b.Run(arm.name+"/plain", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = io.Copy(io.Discard, arm.src())
			}
		})
		b.Run(arm.name+"/scrubbed", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf := copyBufs.Get().(*[]byte)
				_, _ = io.Copy(io.Discard, newScrubReader(arm.src(), sc, *buf))
				copyBufs.Put(buf)
			}
		})
	}
}

func benchFront(b *testing.B, mode InterceptMode) (proxyAddr string, roots *x509.CertPool) {
	b.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	b.Cleanup(upstream.Close)
	rule := Rule{Host: "example.com", Intercept: mode}
	var ca *CA
	if mode == InterceptAlways {
		ca = testCAOnly(b)
		roots = rootsFor(b, ca)
	} else {
		roots = trustUpstream(upstream)
	}
	p := New(Policy{Allow: []Rule{rule}}, nil, ca, fixedDial(upstream.Listener.Addr().String()), nil, nil)
	if ca != nil {
		p.pools.Load().mitm.TLSClientConfig.RootCAs = trustUpstream(upstream)
	}
	return serveFront(b, p).Listener.Addr().String(), roots
}

func benchRoundTrip(b *testing.B, tc *tls.Conn, br *bufio.Reader) {
	b.Helper()
	req := newRequest(b, http.MethodGet, "/x", "")
	if err := req.Write(tc); err != nil {
		b.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		b.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("status %d, want 200", resp.StatusCode)
	}
}
