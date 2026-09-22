// portsmoke proves sandboxd's guest-port relay on real hardware: it starts a
// listener inside a claimed microVM and drives
// GET /v1/sandboxes/{id}/ports/{port} end to end — the path an edge proxy
// takes to reach a sandbox that has no NIC. Run by scripts/port-e2e.sh.
package main

import (
	"bufio"
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

const (
	// guestPort is envd's; the relay is port-agnostic either way.
	guestPort = 49983
	// halfClosePort greets and shuts its write side, then keeps reading.
	halfClosePort = 49984
	// deadPort has no listener, so silkd's connect refusal must surface as 502.
	deadPort = 49985
	// listenerReadyWait bounds the wait for the in-guest listener to bind.
	listenerReadyWait = 20 * time.Second
	// guestServerPath is where the uploaded listener lands in the guest.
	guestServerPath = "/usr/local/bin/guestserver"
	// claimTTL outlasts the run; the node default would reap the sandbox mid-test.
	claimTTL = 30 * time.Minute
)

var guestServerMode = uint32(0o755)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "rt:24.04", "template ref")
	listener := flag.String("listener", "", "path to the guestserver binary uploaded into the sandbox")
	hold := flag.Duration("hold", 0, "after the steps pass, print the claim and keep it alive for this long")
	flag.Parse()

	if err := run(*addr, *token, *template, *listener, *hold); err != nil {
		fmt.Fprintln(os.Stderr, "portsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("PORTSMOKE PASS")
}

func run(addr, token, template, listener string, hold time.Duration) error {
	if listener == "" {
		return errors.New("-listener is required: the stock image ships no HTTP listener")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second+hold)
	defer cancel()

	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone), sandbox.WithTimeout(claimTTL))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("claimed %s on %s\n", sb.ID, sb.Owner())

	start := time.Now()
	if err := startGuestListener(ctx, sb, listener); err != nil {
		return err
	}
	fmt.Printf("  listener up in %.1fs\n", time.Since(start).Seconds())
	rt := &harness.PortRelay{Owner: sb.Owner(), ID: sb.ID, Token: sb.Token()}

	for _, step := range []struct {
		name string
		run  func(context.Context, *harness.PortRelay, *sandbox.Sandbox) error
	}{
		{"http round trip", stepHTTP},
		{"http2 round trip", stepHTTP2},
		{"half close", stepHalfClose},
		{"guest half close", stepGuestHalfClose},
		{"wrong token is 404", stepWrongToken},
		{"missing token is 401", stepNoToken},
		{"bad port is 400", stepBadPort},
		{"no upgrade is 426", stepNoUpgrade},
		{"no listener is 502", stepDeadPort},
		{"wake from hibernate", stepHibernate},
	} {
		t0 := time.Now()
		if err := step.run(ctx, rt, sb); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Printf("  ok  %-24s %5.1fs\n", step.name, time.Since(t0).Seconds())
	}
	if hold > 0 {
		// the listener is live now, for the operator's envd-proxy smoke to drive
		fmt.Printf("SANDBOX %s %s %s %d\n", sb.ID, sb.Token(), sb.Owner(), guestPort)
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
	}
	return nil
}

// startGuestListener ships an envd stand-in in: the stock image has no python, netcat or HTTP server.
func startGuestListener(ctx context.Context, sb *sandbox.Sandbox, listener string) error {
	bin, err := os.ReadFile(listener) //nolint:gosec // the path is an operator-supplied flag on a test harness
	if err != nil {
		return fmt.Errorf("read listener: %w", err)
	}
	if err := sb.WriteFile(ctx, guestServerPath, bin, &guestServerMode); err != nil {
		return fmt.Errorf("upload listener: %w", err)
	}
	argv := []string{
		guestServerPath,
		"-addr", fmt.Sprintf("127.0.0.1:%d", guestPort),
		"-half-close-addr", fmt.Sprintf("127.0.0.1:%d", halfClosePort),
	}
	if _, err := sb.Spawn(ctx, sandbox.Cmd{Argv: argv}); err != nil {
		return fmt.Errorf("spawn listener: %w", err)
	}
	deadline := time.Now().Add(listenerReadyWait)
	for time.Now().Before(deadline) {
		out, execErr := sb.Exec(ctx, "sh", "-c", fmt.Sprintf("ss -ltn 'sport = :%d' | grep -c LISTEN", guestPort))
		if execErr == nil && strings.TrimSpace(out) != "0" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("listener never bound 127.0.0.1:%d", guestPort)
}

func stepHTTP(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	body, err := relayGet(ctx, rt, guestPort, "/echo?a=1")
	if err != nil {
		return err
	}
	return wantReport(body, "proto=1", "path=/echo?a=1")
}

// stepHTTP2 proves the relay is protocol-blind; ConnectRPC streaming needs HTTP/2.
func stepHTTP2(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{
		Protocols:   &protocols,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return rt.Dial(ctx, guestPort) },
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://guest/echo", strings.NewReader("{}"))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return wantReport(string(out), "proto=2", "method=POST")
}

// wantReport asserts guestserver reported each expected line.
func wantReport(body string, want ...string) error {
	for _, line := range want {
		if !strings.Contains(body, line) {
			return fmt.Errorf("guest report missing %q:\n%s", line, body)
		}
	}
	return nil
}

// stepHalfClose proves a client shutdown reaches the guest without ending its answer.
func stepHalfClose(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	conn, err := rt.Dial(ctx, guestPort)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, writeErr := io.WriteString(conn, "GET /half HTTP/1.1\r\nHost: guest\r\n\r\n"); writeErr != nil {
		return writeErr
	}
	cw, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		return errors.New("relay conn does not support CloseWrite")
	}
	if closeErr := cw.CloseWrite(); closeErr != nil {
		return fmt.Errorf("close write: %w", closeErr)
	}
	out, err := io.ReadAll(conn)
	if err != nil {
		return err
	}
	return wantReport(string(out), "200 OK", "path=/half")
}

// stepGuestHalfClose proves the guest can shut its write side and still receive.
func stepGuestHalfClose(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	conn, err := rt.Dial(ctx, halfClosePort)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	greeting := make([]byte, len("greeting"))
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if n, err := conn.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("after the guest shut its write side: read %d, %v, want EOF", n, err)
	}
	if _, err := io.WriteString(conn, "after-done"); err != nil {
		return fmt.Errorf("write after the guest half close: %w", err)
	}
	cw, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		return errors.New("relay conn does not support CloseWrite")
	}
	if err := cw.CloseWrite(); err != nil {
		return err
	}
	deadline := time.Now().Add(listenerReadyWait)
	for time.Now().Before(deadline) {
		seen, err := relayGet(ctx, rt, guestPort, "/half-close-seen")
		if err != nil {
			return err
		}
		if seen == "after-done" {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("the guest never saw what the client sent after its own half close")
}

func stepWrongToken(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	return relayWantStatus(ctx, rt, rt.URL(guestPort), "wrong-token", true, http.StatusNotFound)
}

func stepNoToken(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	return relayWantStatus(ctx, rt, rt.URL(guestPort), "", true, http.StatusUnauthorized)
}

func stepBadPort(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	for _, port := range []string{"0", "65536", "+22", "http"} {
		url := fmt.Sprintf("http://%s/v1/sandboxes/%s/ports/%s", rt.Owner, rt.ID, port)
		if err := relayWantStatus(ctx, rt, url, rt.Token, true, http.StatusBadRequest); err != nil {
			return fmt.Errorf("port %q: %w", port, err)
		}
	}
	return nil
}

func stepNoUpgrade(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	return relayWantStatus(ctx, rt, rt.URL(guestPort), rt.Token, false, http.StatusUpgradeRequired)
}

func stepDeadPort(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	return relayWantStatus(ctx, rt, rt.URL(deadPort), rt.Token, true, http.StatusBadGateway)
}

// stepHibernate proves the relay wakes a hibernated sandbox; the listener survives in the snapshot.
func stepHibernate(ctx context.Context, rt *harness.PortRelay, sb *sandbox.Sandbox) error {
	if err := sb.Hibernate(ctx); err != nil {
		return fmt.Errorf("hibernate: %w", err)
	}
	body, err := relayGet(ctx, rt, guestPort, "/after-wake")
	if err != nil {
		return err
	}
	return wantReport(body, "path=/after-wake")
}

func relayGet(ctx context.Context, r *harness.PortRelay, port uint16, path string) (string, error) {
	conn, err := r.Dial(ctx, port)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	if _, writeErr := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: guest\r\nConnection: close\r\n\r\n", path); writeErr != nil {
		return "", writeErr
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("guest answered %s", resp.Status)
	}
	out, err := io.ReadAll(resp.Body)
	return string(out), err
}

// relayWantStatus asserts a refusal is answered as HTTP, never by opening a relay.
func relayWantStatus(ctx context.Context, r *harness.PortRelay, url, token string, upgrade bool, want int) error {
	req, err := r.Request(ctx, url, token, upgrade)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("status %d, want %d", resp.StatusCode, want)
	}
	return nil
}
