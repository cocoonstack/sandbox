// rpcbench measures the one-connection-per-RPC overhead on a live node and
// what a pre-dialed spare connection would buy (H-4's decision data): mode A
// dials+upgrades per RPC like the SDK does today; mode B keeps one dialed
// connection ahead, hiding the handshake behind the previous call. Run by
// hand against a claimed sandbox:
//
//	rpcbench -addr <node> -token <api token> -template <ref> -n 200
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	"github.com/cocoonstack/sandbox/protocol/wire"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	n := flag.Int("n", 200, "RPCs per mode")
	flag.Parse()
	if err := run(*addr, *token, *template, *n); err != nil {
		fmt.Fprintln(os.Stderr, "rpcbench:", err)
		os.Exit(1)
	}
}

func run(addr, token, template string, n int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithNetwork(sandbox.NetNone))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()

	dial := func() (net.Conn, error) { return dialAgent(ctx, sb.Owner(), sb.ID, sb.Token()) }

	// Warm the path (wake resolution, page cache) before either mode.
	for range 5 {
		conn, err := dial()
		if err != nil {
			return err
		}
		if err := statRPC(conn); err != nil {
			return err
		}
	}

	a := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		conn, err := dial()
		if err != nil {
			return err
		}
		if err := statRPC(conn); err != nil {
			return err
		}
		a = append(a, time.Since(start))
	}
	report("A dial-per-RPC (today)", a)

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
	report("B pre-dialed spare   ", b)
	return nil
}

// statRPC: the protocol is one RPC per connection.
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

// dialAgent mirrors the SDK's hand-rolled upgrade (unexported there).
func dialAgent(ctx context.Context, addr, id, token string) (net.Conn, error) {
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/sandboxes/"+id+"/agent", nil)
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

// bufferedConn reads through the handshake reader so bytes coalesced behind
// the 101 are never lost.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

func report(label string, samples []time.Duration) {
	slices.Sort(samples)
	pct := func(p float64) time.Duration { return samples[int(p*float64(len(samples)-1))] }
	fmt.Printf("%s  n=%d p50=%.2fms p90=%.2fms p99=%.2fms\n",
		label, len(samples), ms(pct(0.50)), ms(pct(0.90)), ms(pct(0.99)))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
