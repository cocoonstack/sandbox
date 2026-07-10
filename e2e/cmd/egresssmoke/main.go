// egresssmoke is the none-lane guarded-egress acceptance: a NIC-less sandbox
// reaches an allowed origin through the host egress proxy with a host-injected
// credential it never holds, and a disallowed host is denied — over vsock, no NIC.
//
// The origin runs on the sandboxd host (the guest has no network); the proxy
// dials it host-side. Run on a node whose config declares a none-lane pool with
// an egress policy allowing 127.0.0.1 and a secret, e.g.:
//
//	"secrets":[{"name":"probe","header":"X-Egress-Token","value_env":"EGRESS_PROBE_TOKEN"}],
//	"pools":[{"template":"…","net":"none","size":"small","warm":1,
//	  "egress":{"allow":[{"host":"127.0.0.1","secret":"probe"}]}}]
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const proxy = "http://127.0.0.1:3128"

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "none-lane template with an egress policy")
	wantToken := flag.String("secret", "", "the value the origin should observe injected")
	flag.Parse()

	if err := run(*addr, *token, *template, *wantToken); err != nil {
		fmt.Fprintln(os.Stderr, "egresssmoke:", err)
		os.Exit(1)
	}
	fmt.Println("EGRESSSMOKE PASS")
}

func run(addr, token, template, wantToken string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	origin, port, err := startOrigin()
	if err != nil {
		return err
	}
	defer func() { _ = origin.Close() }()

	c, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	sb, err := c.New(ctx, template, sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.Small))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claimed none-lane sandbox %s\n", sb.ID)

	if out, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -m 3 http://127.0.0.1:%d/ || echo NO-NIC", port)); !strings.Contains(out, "NO-NIC") {
		return fmt.Errorf("guest reached the origin without the proxy: %q", out)
	}
	fmt.Println("  no-NIC confirmed: direct egress fails")

	seen, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -x %s http://127.0.0.1:%d/", proxy, port))
	if err != nil {
		return fmt.Errorf("proxied exec: %w", err)
	}
	if strings.TrimSpace(seen) != wantToken {
		return fmt.Errorf("origin saw injected token %q, want %q", strings.TrimSpace(seen), wantToken)
	}
	fmt.Printf("  allowed origin reached; injected credential observed host-side\n")

	if out, _ := sb.Exec(ctx, "sh", "-c", "env; cat /proc/1/environ 2>/dev/null | tr '\\0' '\\n'"); wantToken != "" && strings.Contains(out, wantToken) {
		return fmt.Errorf("secret value leaked into the guest")
	}
	fmt.Println("  secret absent from guest env")

	deny, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -o /dev/null -w '%%{http_code}' -x %s http://10.255.255.1/", proxy))
	if strings.TrimSpace(deny) != "403" {
		return fmt.Errorf("denied host returned %q, want 403", strings.TrimSpace(deny))
	}
	fmt.Println("  disallowed host denied (403)")
	return nil
}

// startOrigin serves one echo endpoint that returns the credential header the
// proxy injected, on a host-local port.
func startOrigin() (*http.Server, int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get("X-Egress-Token")) //nolint:gosec // test origin echoing the token we injected
	}), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, ln.Addr().(*net.TCPAddr).Port, nil
}
