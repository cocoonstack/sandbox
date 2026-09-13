// Command sockssmoke proves the SOCKS5 egress channel from inside a claimed
// sandbox: an allowed host tunnels over 127.0.0.1:1080, a GET-only host is
// reachable on the HTTP proxy but refused on SOCKS5, an unlisted host is
// refused, IMAPS rides the tunnel, and a pool without a policy leaves the
// host door unwired so the guest's dial is refused.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	socksProxy = "127.0.0.1:1080"
	httpProxy  = "http://127.0.0.1:3128"

	curlLoginDenied = "exit=67"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7780", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template of the policy-bearing small pool")
	echo := flag.String("echo", "postman-echo.com", "public HTTP host the policy allows without a method restriction")
	getOnly := flag.String("get-only", "example.com", "public HTTP host the policy allows for GET only")
	imap := flag.String("imap", "imap.163.com", "IMAPS host the policy allows, for the non-HTTP leg; empty skips it")
	flag.Parse()

	if err := run(*addr, *token, *template, *echo, *getOnly, *imap); err != nil {
		fmt.Fprintln(os.Stderr, "sockssmoke:", err)
		os.Exit(1)
	}
	fmt.Println("sockssmoke: PASS")
}

func run(addr, token, template, echo, getOnly, imap string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, sb, err := harness.Claim(ctx, addr, token, template, sandbox.WithSize(sandbox.Small))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claimed %s\n", sb.ID)

	if code := curl(ctx, sb, "--socks5-hostname", socksProxy, "http://"+echo+"/get"); code != "200" {
		return fmt.Errorf("allowed host over SOCKS5 returned %q, want 200", code)
	}
	fmt.Println("  allowed host tunnels over SOCKS5")

	if code := curl(ctx, sb, "-x", httpProxy, "http://"+getOnly+"/"); code != "200" {
		return fmt.Errorf("GET-only host over the HTTP proxy returned %q, want 200", code)
	}
	if code := curl(ctx, sb, "--socks5-hostname", socksProxy, "http://"+getOnly+"/"); code != "000" {
		return fmt.Errorf("GET-only host over SOCKS5 returned %q, want a refused tunnel", code)
	}
	fmt.Println("  GET-only host reachable on 3128, refused on 1080")

	if code := curl(ctx, sb, "--socks5-hostname", socksProxy, "http://10.255.255.1/"); code != "000" {
		return fmt.Errorf("unlisted host over SOCKS5 returned %q, want a refused tunnel", code)
	}
	fmt.Println("  unlisted host refused")

	if imap != "" {
		out, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf(
			"curl -sS -m 30 --socks5-hostname %s -u probe:probe imaps://%s/ 2>&1; echo exit=$?", socksProxy, imap))
		if !strings.Contains(out, curlLoginDenied) {
			return fmt.Errorf("IMAPS over SOCKS5 did not reach the login step: %q", strings.TrimSpace(out))
		}
		fmt.Println("  IMAPS handshake rides the tunnel; the server refused the probe login")
	}

	_, bare, err := harness.Claim(ctx, addr, token, template, sandbox.WithSize(sandbox.Medium))
	if err != nil {
		return fmt.Errorf("claim from the policy-less pool: %w", err)
	}
	defer func() { _ = bare.Close() }()
	if code := curl(ctx, bare, "--socks5-hostname", socksProxy, "http://"+echo+"/get"); code != "000" {
		return fmt.Errorf("policy-less pool served SOCKS5 with %q, want the host door unwired and the dial refused", code)
	}
	fmt.Println("  pool without a policy: host door unwired, guest dial refused")
	return nil
}

// curl reports the HTTP status the guest's curl saw; 000 is a connection the proxy refused.
func curl(ctx context.Context, sb *sandbox.Sandbox, args ...string) string {
	out, _ := sb.Exec(ctx, "sh", "-c", "curl -s -o /dev/null -m 30 -w '%{http_code}' "+strings.Join(args, " "))
	return strings.TrimSpace(out)
}
