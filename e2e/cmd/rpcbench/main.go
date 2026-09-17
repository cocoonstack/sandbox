// rpcbench measures the one-connection-per-RPC overhead on a live node and
// what the alternatives buy: mode A dials+upgrades per RPC by hand; mode C
// is the SDK's own path, one kept connection serving RPCs back to back; mode
// B keeps one hand-dialed connection ahead, hiding the handshake behind the
// previous call (H-4's decision data). A and C interleave sample by sample
// and swap which one leads, so node drift cannot land on one arm. Run by
// hand against a claimed sandbox:
//
//	rpcbench -addr <node> -token <api token> -template <ref> -n 200
//
// An https:// address drives both hand-dialed modes through the TLS edge the
// SDK already uses, so A pays a handshake per RPC the way a real client does.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/protocol/wire"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	n := flag.Int("n", 200, "RPCs per mode")
	caCert := flag.String("cacert", "", "PEM roots for an https address")
	flag.Parse()
	if err := run(*addr, *token, *template, *caCert, *n); err != nil {
		fmt.Fprintln(os.Stderr, "rpcbench:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, caCert string, n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	tlsCfg, err := edgeTLS(addr, caCert)
	if err != nil {
		return err
	}
	opts := []sandbox.ClientOption{sandbox.WithAPIToken(token)}
	if tlsCfg != nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = tlsCfg
		opts = append(opts, sandbox.WithHTTPClient(&http.Client{Transport: tr}))
	}
	client, err := sandbox.Connect(addr, opts...)
	if err != nil {
		return err
	}
	sb, err := client.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()

	dial := func() (net.Conn, error) { return dialAgent(ctx, sb.Owner(), sb.ID, sb.Token(), tlsCfg) }

	// warm the path (wake resolution, page cache) before either mode.
	for range 5 {
		conn, err := dial()
		if err != nil {
			return err
		}
		if err := statRPC(conn); err != nil {
			return err
		}
	}

	if _, err := sb.Stat(ctx, "/"); err != nil {
		return err
	}
	a := make([]time.Duration, 0, n)
	c := make([]time.Duration, 0, n)
	for i := range n {
		dialFirst := i%2 == 0
		if dialFirst {
			d, err := sampleDial(dial)
			if err != nil {
				return err
			}
			a = append(a, d)
		}
		d, err := sampleKept(ctx, sb)
		if err != nil {
			return err
		}
		c = append(c, d)
		if !dialFirst {
			if d, err = sampleDial(dial); err != nil {
				return err
			}
			a = append(a, d)
		}
	}
	report("A dial-per-RPC", a)
	report("C SDK keep-alive", c)

	spare := make(chan net.Conn, 1)
	errs := make(chan error, 1)
	go func() {
		for {
			conn, err := dial()
			if err != nil {
				errs <- err
				return
			}
			select {
			case spare <- conn:
			case <-ctx.Done():
				_ = conn.Close()
				return
			}
		}
	}()
	b := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		var conn net.Conn
		select {
		case conn = <-spare:
		case err := <-errs:
			return err
		}
		if err := statRPC(conn); err != nil {
			return err
		}
		b = append(b, time.Since(start))
	}
	cancel()
	report("B pre-dialed spare", b)
	return nil
}

func sampleDial(dial func() (net.Conn, error)) (time.Duration, error) {
	start := time.Now()
	conn, err := dial()
	if err != nil {
		return 0, err
	}
	if err := statRPC(conn); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// sampleKept times one RPC on the SDK's kept connection, probe and all.
func sampleKept(ctx context.Context, sb *sandbox.Sandbox) (time.Duration, error) {
	start := time.Now()
	if _, err := sb.Stat(ctx, "/"); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func statRPC(conn net.Conn) error {
	defer func() { _ = conn.Close() }()
	sc := silkd.NewConn(conn)
	if err := sc.Send(&wire.FsStat{Path: "/"}); err != nil {
		return err
	}
	resp, err := sc.Recv()
	if err != nil {
		return err
	}
	if e, ok := resp.(*wire.ErrorResp); ok {
		return e
	}
	return nil
}

// bufferedConn reads through the handshake reader so bytes coalesced behind
// the 101 are never lost.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// edgeTLS returns the client config for an https address, or nil for a plain one.
func edgeTLS(addr, caCert string) (*tls.Config, error) {
	if !strings.HasPrefix(addr, "https://") {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ClientSessionCache: tls.NewLRUClientSessionCache(0)}
	if caCert == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caCert) //nolint:gosec // operator-supplied roots for the bench edge
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parse %s: no certificates", caCert)
	}
	cfg.RootCAs = roots
	return cfg, nil
}

// dialAgent mirrors the SDK's hand-rolled upgrade (unexported there).
func dialAgent(ctx context.Context, addr, id, token string, tlsCfg *tls.Config) (net.Conn, error) {
	host := strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	scheme := "http"
	if tlsCfg != nil {
		if raw, err = handshake(ctx, raw, host, tlsCfg); err != nil {
			return nil, err
		}
		scheme = "https"
	}
	req, err := http.NewRequest(http.MethodGet, scheme+"://"+host+"/v1/sandboxes/"+id+"/agent", nil)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "silkd")
	req.Header.Set("Authorization", "Bearer "+token)
	if err = req.Write(raw); err != nil {
		_ = raw.Close()
		return nil, err
	}
	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = resp.Body.Close()
		_ = raw.Close()
		return nil, fmt.Errorf("upgrade: %s", resp.Status)
	}
	return bufferedConn{Conn: raw, r: br}, nil
}

// handshake wraps raw in TLS, naming the host so a cert without an IP SAN still verifies.
func handshake(ctx context.Context, raw net.Conn, host string, cfg *tls.Config) (net.Conn, error) {
	if cfg.ServerName == "" {
		name, _, err := net.SplitHostPort(host)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		cfg = cfg.Clone()
		cfg.ServerName = name
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tc, nil
}

func report(label string, samples []time.Duration) {
	slices.Sort(samples)
	pct := func(p float64) time.Duration { return samples[int(p*float64(len(samples)-1))] }
	fmt.Printf("%-22s n=%d p50=%.2fms p90=%.2fms p99=%.2fms\n",
		label, len(samples), ms(pct(0.50)), ms(pct(0.90)), ms(pct(0.99)))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
