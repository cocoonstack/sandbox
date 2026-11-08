// egresssmoke checks guarded egress: an allowed origin gets the host-injected credential, direct egress is blocked, and a disallowed host is denied.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template with an egress policy")
	wantToken := flag.String("secret", "", "the value the origin should observe injected")
	netShape := flag.String("net", "none", "claim lane: none|egress")
	reach := flag.String("reach", "127.0.0.1", "host the guest reaches the origin at")
	nicAddr := flag.String("nicaddr", "", "static CIDR to bring the egress-lane NIC up with (default route via -reach)")
	echo := flag.String("echo", "postman-echo.com", "public HTTP host echoing request headers at /get, for the injection check")
	flag.Parse()

	if err := run(*addr, *token, *template, *wantToken, *netShape, *reach, *nicAddr, *echo); err != nil {
		fmt.Fprintln(os.Stderr, "egresssmoke:", err)
		os.Exit(1)
	}
	fmt.Println("EGRESSSMOKE PASS")
}

func run(addr, token, template, wantToken, netShape, reach, nicAddr, echo string) error {
	if wantToken == "" {
		return errors.New("-secret is required for the injection check")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	origin, port, err := startOrigin()
	if err != nil {
		return err
	}
	defer func() { _ = origin.Close() }()
	target := fmt.Sprintf("http://%s:%d/", reach, port)

	_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithNetwork(sandbox.NetShape(netShape)), sandbox.WithSize(sandbox.Small))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claimed %s-lane sandbox %s\n", netShape, sb.ID)

	// The image leaves the NIC unconfigured; a static address gives the guest a route toward -reach, so a blocked direct egress is the lock.
	if nicAddr != "" {
		out, cfgErr := sb.Exec(ctx, "sh", "-c", fmt.Sprintf(
			"nic=$(ip -o link | awk -F': ' '$2 ~ /^(eth|en)/{sub(/@.*/,\"\",$2); print $2; exit}'); "+
				"ip addr add %s dev $nic && ip link set $nic up && ip route add default via %s", nicAddr, reach))
		if cfgErr != nil {
			return fmt.Errorf("configure NIC: %w (%s)", cfgErr, strings.TrimSpace(out))
		}
	}

	out, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -m 3 --noproxy '*' %s || echo BLOCKED", target))
	if !strings.Contains(out, "BLOCKED") {
		return fmt.Errorf("guest reached the origin without the proxy: %q", out)
	}
	fmt.Println("  direct egress blocked")

	// The SSRF guard refuses private and loopback destinations even when allow-listed, so injection needs a public echo.
	seen, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -x %s http://%s/get", harness.GuestProxy, echo))
	if err != nil {
		return fmt.Errorf("proxied exec: %w", err)
	}
	if !strings.Contains(seen, wantToken) {
		return fmt.Errorf("origin %s did not echo injected token %q: %q", echo, wantToken, strings.TrimSpace(seen))
	}
	fmt.Println("  allowed origin reached; injected credential observed host-side")

	implicit, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -m 10 http://%s/get", echo))
	if !strings.Contains(implicit, wantToken) {
		return fmt.Errorf("origin %s not reached without -x: silkd did not hand the proxy variables to the exec: %q", echo, strings.TrimSpace(implicit))
	}
	fmt.Println("  allowed origin reached with no -x: proxy variables present in the exec")

	env, err := sb.Exec(ctx, "sh", "-c", "env; cat /proc/1/environ 2>/dev/null | tr '\\0' '\\n'")
	if err != nil {
		return fmt.Errorf("leak probe: %w", err)
	}
	if strings.Contains(env, wantToken) {
		return errors.New("secret value leaked into the guest")
	}
	fmt.Println("  secret absent from guest env")

	deny, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -o /dev/null -w '%%{http_code}' -x %s http://10.255.255.1/", harness.GuestProxy))
	if strings.TrimSpace(deny) != "403" {
		return fmt.Errorf("denied host returned %q, want 403", strings.TrimSpace(deny))
	}
	fmt.Println("  disallowed host denied (403)")
	return nil
}

// startOrigin serves the host-local negative control, answering REACHED to a guest that dials it directly.
func startOrigin() (*http.Server, int, error) {
	ln, err := net.Listen("tcp", "0.0.0.0:0") //nolint:gosec // test origin the egress-lane guest reaches via the bridge
	if err != nil {
		return nil, 0, err
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "REACHED")
	}), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, ln.Addr().(*net.TCPAddr).Port, nil
}
