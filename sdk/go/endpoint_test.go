package sandbox

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sdk/go/silkd/silkdtest"
)

func TestEndpointURL(t *testing.T) {
	for _, tt := range []struct{ addr, scheme, want string }{
		{"localhost:7777", "http", "http://localhost:7777"},
		{"localhost:443", "https", "https://localhost:443"},
		{"https://example.com/", "http", "https://example.com"},
		{"http://example.com", "https", "http://example.com"},
		{"[::1]:7777", "https", "https://[::1]:7777"},
		{"https://[::1]", "http", "https://[::1]"},
	} {
		t.Run(tt.addr, func(t *testing.T) {
			u, err := endpointURL(tt.addr, tt.scheme)
			if err != nil || u.String() != tt.want {
				t.Fatalf("endpoint = %v, %v; want %s", u, err, tt.want)
			}
		})
	}
	for _, addr := range []string{"", "https://", "ftp://node", "https://user:pass@node", "https://node/path", "https://node?", "https://node#", "https://node#frag", "https://node:0", "https://node:65536", "https://node:bad", "https://node\r\nX: bad"} {
		t.Run(addr, func(t *testing.T) {
			if _, err := endpointURL(addr, "http"); err == nil {
				t.Fatalf("accepted %q", addr)
			}
		})
	}
}

func TestTLSControlAndRelay(t *testing.T) {
	agent := newAgentServer(t, silkdtest.ServeConn)
	secure := httptest.NewTLSServer(agent.Config.Handler)
	t.Cleanup(secure.Close)
	roots := x509.NewCertPool()
	roots.AddCert(secure.Certificate())
	for _, tt := range []struct {
		name string
		opts []ClientOption
	}{
		{"tls option", []ClientOption{WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})}},
		{"http transport", []ClientOption{WithHTTPClient(secure.Client())}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Connect(secure.URL, tt.opts...)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.roundTrip(t.Context(), http.MethodGet, secure.URL, "/healthz", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			for _, owner := range []string{secure.URL, strings.TrimPrefix(secure.URL, "https://")} {
				sb := c.Attach(owner, "sb_test", "tok")
				out, err := sb.Exec(t.Context(), "echo", "tls")
				if err != nil || out != "tls\n" {
					t.Fatalf("exec = %q, %v", out, err)
				}
				pc, err := sb.DialPort(t.Context(), 8080)
				if err != nil {
					t.Fatal(err)
				}
				defer pc.Close()
				if _, err = pc.Write([]byte("tail")); err != nil {
					t.Fatal(err)
				}
				if err = pc.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(pc)
				if err != nil || string(body) != "tail" {
					t.Fatalf("tail = %q, %v", body, err)
				}
			}
		})
	}
}

func TestTLSVerification(t *testing.T) {
	agent := newAgentServer(t, silkdtest.ServeConn)
	secure := httptest.NewTLSServer(agent.Config.Handler)
	t.Cleanup(secure.Close)
	roots := x509.NewCertPool()
	roots.AddCert(secure.Certificate())
	for _, tt := range []struct {
		name string
		cfg  *tls.Config
	}{
		{"untrusted CA", &tls.Config{MinVersion: tls.VersionTLS12}},
		{"wrong name", &tls.Config{RootCAs: roots, ServerName: "wrong.example", MinVersion: tls.VersionTLS12}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Connect(secure.URL, WithTLSConfig(tt.cfg))
			if err != nil {
				t.Fatal(err)
			}
			if resp, requestErr := c.roundTrip(t.Context(), http.MethodGet, secure.URL, "/healthz", nil, ""); requestErr == nil {
				_ = resp.Body.Close()
				t.Fatal("control request accepted invalid certificate")
			}
			if _, err := c.dialAgent(t.Context(), secure.URL, "sb_test", "tok"); err == nil {
				t.Fatal("relay accepted invalid certificate")
			}
		})
	}
}

func TestTLSHandshakeCancellation(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := l.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		cancel()
		_, _ = io.Copy(io.Discard, conn)
	}()
	c, err := Connect("https://" + l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.dialAgent(ctx, c.addr, "sb_test", "tok"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
	<-done
}
