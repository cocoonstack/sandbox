// interceptsmoke is the HTTPS-interception acceptance: a none-lane sandbox whose
// golden had the cluster root baked reaches an HTTPS echo through the proxy; the
// leaf the node's intermediate signed validates against that baked root, the TLS
// is terminated (so the guest sees our issuer, not the origin's), and the secret
// is injected into the HTTPS request — the guarantees plaintext already had.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const proxy = "http://127.0.0.1:3128"

func main() {
	addr := flag.String("addr", "127.0.0.1:7779", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template with an intercept policy")
	echo := flag.String("echo", "postman-echo.com", "HTTPS host that echoes request headers at /get")
	secret := flag.String("secret", "", "the value the origin should observe injected")
	issuer := flag.String("issuer-substr", "sandbox egress intermediate", "expected leaf issuer substring")
	flag.Parse()
	if err := run(*addr, *token, *template, *echo, *secret, *issuer); err != nil {
		fmt.Fprintln(os.Stderr, "interceptsmoke:", err)
		os.Exit(1)
	}
}

func run(addr, token, template, echo, secret, issuer string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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

	present, _ := sb.Exec(ctx, "sh", "-c", "test -f /usr/local/share/ca-certificates/sandbox-egress.crt && echo PRESENT || echo ABSENT")
	if strings.TrimSpace(present) != "PRESENT" {
		return fmt.Errorf("cluster root not baked into guest trust store: %q", strings.TrimSpace(present))
	}
	fmt.Println("  cluster root baked into guest trust store")

	verbose, err := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -sS -v -x %s https://%s/get 2>&1", proxy, echo))
	if err != nil {
		return fmt.Errorf("proxied HTTPS exec: %w (%s)", err, tail(verbose))
	}
	if !strings.Contains(verbose, secret) {
		return fmt.Errorf("injected credential %q not echoed by origin; interception/injection failed:\n%s", secret, tail(verbose))
	}
	fmt.Println("  HTTPS handshake succeeded and injected credential observed in the echo")

	gotIssuer := grepLine(verbose, "issuer:")
	switch {
	case gotIssuer == "":
		fmt.Println("  (curl -v printed no issuer line; the injected credential already proves interception)")
	case !strings.Contains(gotIssuer, issuer):
		return fmt.Errorf("guest saw leaf issuer %q, want this node's intermediate (%q)", strings.TrimSpace(gotIssuer), issuer)
	default:
		fmt.Printf("  guest's TLS leaf issued by this node's intermediate (%s)\n", strings.TrimSpace(gotIssuer))
	}

	blocked, _ := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("curl -s -m 3 https://%s/ >/dev/null 2>&1 && echo REACHED || echo BLOCKED", echo))
	if strings.TrimSpace(blocked) != "BLOCKED" {
		return fmt.Errorf("guest reached the origin without the proxy: %q", strings.TrimSpace(blocked))
	}
	fmt.Println("  direct egress blocked (only the proxy route works)")
	fmt.Println("INTERCEPTSMOKE PASS")
	return nil
}

func grepLine(out, needle string) string {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(strings.ToLower(line), needle) {
			return line
		}
	}
	return ""
}

func tail(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	return strings.Join(lines, "\n")
}
