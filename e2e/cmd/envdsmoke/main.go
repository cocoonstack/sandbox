// envdsmoke proves the e2b flavor on real hardware: it claims a sandbox from an
// envd-carrying pool and drives the real envd through sandboxd's guest-port
// relay, the path an edge proxy takes. The guest has no NIC, so the relay is
// the only way in and nothing here can be reached by accident.
//
// It asserts what the e2b SDK depends on — the daemon is up, its version is the
// one the compat API must report, files and the ConnectRPC surface answer — and
// reports which HTTP version envd actually serves, because the proxy in front
// of it must match rather than assume.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	// envdPort is envd's fixed listener; the SDK derives its host from it.
	envdPort = 49983
	// readyWait bounds the wait for envd to answer after the claim.
	readyWait = 60 * time.Second
	// claimTTL outlasts the run so a lease expiry cannot look like a relay fault.
	claimTTL = 30 * time.Minute
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "", "e2b flavor template ref")
	wantVersion := flag.String("envd-version", "", "version envd must report; empty only prints it")
	hold := flag.Duration("hold", 0, "after the steps pass, print the claim and keep it alive for this long")
	flag.Parse()

	if err := run(*addr, *token, *template, *wantVersion, *hold); err != nil {
		fmt.Fprintln(os.Stderr, "envdsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("ENVDSMOKE PASS")
}

func run(addr, token, template, wantVersion string, hold time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second+hold)
	defer cancel()

	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone), sandbox.WithTimeout(claimTTL))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("claimed %s on %s\n", sb.ID, sb.Owner())

	if err := reportGuest(ctx, sb, wantVersion); err != nil {
		return err
	}
	rt := &relay{owner: sb.Owner(), id: sb.ID, token: sb.Token()}
	if err := waitReady(ctx, rt); err != nil {
		return err
	}

	for _, step := range []struct {
		name string
		run  func(context.Context, *relay) error
	}{
		{"GET /health", stepHealth},
		{"GET /files", stepDownload},
		{"POST /files", stepUpload},
		{"connect unary over http/1.1", stepConnectH1},
		{"envd serves http/1.1 only", stepProtocol},
	} {
		t0 := time.Now()
		if err := step.run(ctx, rt); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Printf("  ok  %-30s %5.1fms\n", step.name, float64(time.Since(t0).Microseconds())/1000)
	}
	if hold > 0 {
		fmt.Printf("SANDBOX %s %s %s %d\n", sb.ID, sb.Token(), sb.Owner(), envdPort)
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
	}
	return nil
}

// reportGuest asks the guest what it is running, over silkd — the native data
// plane, which must keep working alongside envd.
func reportGuest(ctx context.Context, sb *sandbox.Sandbox, wantVersion string) error {
	version, err := sb.Exec(ctx, "/usr/local/bin/envd", "-version")
	if err != nil {
		return fmt.Errorf("envd -version: %w", err)
	}
	version = strings.TrimSpace(version)
	state, _ := sb.Exec(ctx, "systemctl", "is-active", "envd.service")
	fmt.Printf("  envd %s (%s), silkd relay answering\n", version, strings.TrimSpace(state))
	if strings.TrimSpace(state) != "active" {
		out, _ := sb.Exec(ctx, "sh", "-c", "systemctl status envd.service --no-pager 2>&1 | tail -20")
		return fmt.Errorf("envd.service is %q:\n%s", strings.TrimSpace(state), out)
	}
	if wantVersion != "" && version != wantVersion {
		return fmt.Errorf("envd reports %q, want %q — the compat API's envdVersion would be a lie", version, wantVersion)
	}
	return nil
}

func waitReady(ctx context.Context, rt *relay) error {
	deadline := time.Now().Add(readyWait)
	var last error
	for time.Now().Before(deadline) {
		resp, err := rt.do(ctx, http.MethodGet, "/health", nil, nil)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("envd never answered /health: %w", last)
}

func stepHealth(ctx context.Context, rt *relay) error {
	resp, err := rt.do(ctx, http.MethodGet, "/health", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %s, want 204", resp.Status)
	}
	return nil
}

// stepDownload reads a file through envd's HTTP surface, the path the SDK's
// files.read takes.
func stepDownload(ctx context.Context, rt *relay) error {
	resp, err := rt.do(ctx, http.MethodGet, "/files?path=/etc/envd-version&username=root", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s: %s", resp.Status, body)
	}
	if strings.TrimSpace(string(body)) == "" {
		return fmt.Errorf("empty body for /etc/envd-version")
	}
	return nil
}

// stepUpload writes a file through envd and reads it back through silkd's own
// view, so the two daemons are proven to share one filesystem.
func stepUpload(ctx context.Context, rt *relay) error {
	const boundary = "envdsmoke"
	body := fmt.Sprintf("--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"probe.txt\"\r\n"+
		"Content-Type: text/plain\r\n\r\nenvd-wrote-this\r\n--%s--\r\n", boundary, boundary)
	headers := http.Header{"Content-Type": []string{"multipart/form-data; boundary=" + boundary}}
	resp, err := rt.do(ctx, http.MethodPost, "/files?path=/tmp/probe.txt&username=root", headers, strings.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("status %s: %s", resp.Status, out)
	}
	return nil
}

// stepConnectH1 drives one ConnectRPC unary call the way connect-web does over
// HTTP/1.1: the SDK's filesystem and process services are all Connect.
func stepConnectH1(ctx context.Context, rt *relay) error {
	headers := http.Header{
		"Content-Type":             []string{"application/json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{"root"},
	}
	payload := `{"path":"/etc/envd-version"}`
	resp, err := rt.do(ctx, http.MethodPost, "/filesystem.Filesystem/Stat", headers, strings.NewReader(payload))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s: %s", resp.Status, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return fmt.Errorf("decode %q: %w", body, err)
	}
	if _, ok := out["entry"]; !ok {
		return fmt.Errorf("stat reply carries no entry: %s", body)
	}
	return nil
}

// stepProtocol records what envd actually serves in the clear. envd 0.8.0 sets
// no h2c handler, so an edge that forwards HTTP/2 upstream would break every
// request; this fails the moment that stops being true, which is when the
// proxy's upstream should be revisited.
func stepProtocol(ctx context.Context, rt *relay) error {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{
		Protocols:   &protocols,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return rt.dial(ctx) },
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://guest/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil // h2c refused, as expected
	}
	defer func() { _ = resp.Body.Close() }()
	return fmt.Errorf("envd answered h2c with %s: the proxy may now forward HTTP/2 upstream", resp.Status)
}

// relay drives the node's guest-port endpoint the way an edge proxy does.
type relay struct {
	owner string
	id    string
	token string
}

// do sends one request over a fresh relay connection.
func (r *relay) do(ctx context.Context, method, path string, headers http.Header, body io.Reader) (*http.Response, error) {
	conn, err := r.dial(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://guest"+path, body)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	maps.Copy(req.Header, headers)
	req.Close = true
	if writeErr := req.Write(conn); writeErr != nil {
		_ = conn.Close()
		return nil, writeErr
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	resp.Body = &connBody{ReadCloser: resp.Body, conn: conn}
	return resp, nil
}

// dial upgrades to the raw relay and returns the connection.
func (r *relay) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.owner)
	if err != nil {
		return nil, err
	}
	url := "http://" + r.owner + "/v1/sandboxes/" + r.id + "/ports/" + strconv.Itoa(envdPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Upgrade", "tcp")
	req.Header.Set("Connection", "Upgrade")
	if writeErr := req.Write(conn); writeErr != nil {
		_ = conn.Close()
		return nil, writeErr
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, fmt.Errorf("upgrade: %s", resp.Status)
	}
	return conn, nil
}

// connBody closes the relay connection with the response body.
type connBody struct {
	io.ReadCloser
	conn net.Conn
}

func (b *connBody) Close() error {
	_ = b.ReadCloser.Close()
	return b.conn.Close()
}
