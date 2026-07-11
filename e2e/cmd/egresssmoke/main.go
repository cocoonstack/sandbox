// egresssmoke is the guarded-egress acceptance: a sandbox reaches an allowed
// origin through the host egress proxy with a host-injected credential it never
// holds, direct egress is blocked (no NIC on the none lane, an nft lock on the
// egress lane), and a disallowed host is denied.
//
// The origin runs on the sandboxd host and binds all interfaces so the guest
// reaches it at -reach (127.0.0.1 for the none lane, the bridge gateway for the
// egress lane); the pool's egress policy must allow that host. Example config:
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
	template := flag.String("template", "rt:24.04", "template with an egress policy")
	wantToken := flag.String("secret", "", "the value the origin should observe injected")
	netShape := flag.String("net", "none", "claim lane: none|egress")
	reach := flag.String("reach", "127.0.0.1", "host the guest reaches the origin at (policy must allow it)")
	nicAddr := flag.String("nicaddr", "", "static CIDR to bring the egress-lane NIC up with (default route via -reach)")
	guarded := flag.Bool("guarded", true, "pool has an egress policy; false asserts the unlocked NIC reaches directly (negative control)")
	flag.Parse()

	if err := run(*addr, *token, *template, *wantToken, *netShape, *reach, *nicAddr, *guarded); err != nil {
		fmt.Fprintln(os.Stderr, "egresssmoke:", err)
		os.Exit(1)
	}
	fmt.Println("EGRESSSMOKE PASS")
}

func run(addr, token, template, wantToken, netShape, reach, nicAddr string, guarded bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	origin, port, err := startOrigin()
	if err != nil {
		return err
	}
	defer func() { _ = origin.Close() }()
	target := fmt.Sprintf("http://%s:%d/", reach, port)

	c, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return err
	}
	sb, err := c.New(ctx, template, sandbox.WithNetwork(sandbox.NetShape(netShape)), sandbox.WithSize(sandbox.Small))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claimed %s-lane sandbox %s\n", netShape, sb.ID)

	// The image does not configure the NIC, so bring it up statically to give the
	// guest a real route toward -reach; a blocked direct egress is then the lock.
	if nicAddr != "" {
		out, cfgErr := sb.Exec(ctx, "sh", "-c", fmt.Sprintf(
			"nic=$(ip -o link | awk -F': ' '$2 ~ /^(eth|en)/{sub(/@.*/,\"\",$2); print $2; exit}'); "+
				"ip addr add %s dev $nic && ip link set $nic up && ip route add default via %s", nicAddr, reach))
		if cfgErr != nil {
			return fmt.Errorf("configure NIC: %w (%s)", cfgErr, strings.TrimSpace(out))
		}
	}

	out, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -m 3 %s || echo BLOCKED", target))
	if guarded && !strings.Contains(out, "BLOCKED") {
		return fmt.Errorf("guest reached the origin without the proxy: %q", out)
	}
	if !guarded {
		if !strings.Contains(out, "REACHED") {
			return fmt.Errorf("unlocked NIC did not reach the origin: %q", out)
		}
		fmt.Println("  unlocked NIC reaches the origin directly (negative control)")
		return nil
	}
	fmt.Println("  direct egress blocked")

	seen, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -x %s %s", proxy, target))
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
	ln, err := net.Listen("tcp", "0.0.0.0:0") //nolint:gosec // test origin the egress-lane guest reaches via the bridge
	if err != nil {
		return nil, 0, err
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Egress-Token"); v != "" {
			_, _ = io.WriteString(w, v) //nolint:gosec // test origin echoing the token we injected
			return
		}
		_, _ = io.WriteString(w, "REACHED")
	}), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, ln.Addr().(*net.TCPAddr).Port, nil
}
