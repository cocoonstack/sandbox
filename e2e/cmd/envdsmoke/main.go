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
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
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
	// envdPort is envd's fixed listener; the SDK derives its host from it.
	envdPort  = 49983
	readyWait = 60 * time.Second
	// claimTTL outlasts the run so a lease expiry cannot look like a relay fault.
	claimTTL = 30 * time.Minute

	uploadPath  = "/tmp/envdsmoke-probe.txt"
	uploadProbe = "envd-wrote-this"

	processProbe = "envd-process-ok"

	contentType = "Content-Type"
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
	rt := &harness.PortRelay{Owner: sb.Owner(), ID: sb.ID, Token: sb.Token()}
	if err := waitReady(ctx, rt); err != nil {
		return err
	}

	for _, step := range []struct {
		name string
		run  func(context.Context, *harness.PortRelay, *sandbox.Sandbox) error
	}{
		{"GET /health", stepHealth},
		{"GET /files", stepDownload},
		{"POST /files", stepUpload},
		{"connect unary over http/1.1", stepConnectH1},
		{"process start under -no-cgroups", stepProcessStart},
		{"envd serves http/1.1 only", stepProtocol},
	} {
		t0 := time.Now()
		if err := step.run(ctx, rt, sb); err != nil {
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

// reportGuest asks the guest what it runs over silkd, the native data plane that must keep working beside envd.
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

func waitReady(ctx context.Context, rt *harness.PortRelay) error {
	deadline := time.Now().Add(readyWait)
	var last error
	for time.Now().Before(deadline) {
		resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/health", nil, nil)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("envd never answered /health: %w", last)
}

func stepHealth(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/health", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %s, want 204", resp.Status)
	}
	return nil
}

// stepDownload takes the path the SDK's files.read takes.
func stepDownload(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	resp, err := rt.Do(ctx, envdPort, http.MethodGet, "/files?path=/etc/envd-version&username=root", nil, nil)
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

// stepUpload writes through envd and reads back through silkd, so the two daemons share one filesystem.
func stepUpload(ctx context.Context, rt *harness.PortRelay, sb *sandbox.Sandbox) error {
	const boundary = "envdsmoke"
	body := fmt.Sprintf("--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"probe.txt\"\r\n"+
		"Content-Type: text/plain\r\n\r\n%s\r\n--%s--\r\n", boundary, uploadProbe, boundary)
	headers := http.Header{contentType: []string{"multipart/form-data; boundary=" + boundary}}
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/files?path="+uploadPath+"&username=root", headers, strings.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("status %s: %s", resp.Status, out)
	}
	seen, err := sb.ReadFile(ctx, uploadPath)
	if err != nil {
		return fmt.Errorf("silkd read back: %w", err)
	}
	if strings.TrimSpace(string(seen)) != uploadProbe {
		return fmt.Errorf("silkd sees %q at %s, envd wrote %q", seen, uploadPath, uploadProbe)
	}
	return nil
}

// stepConnectH1 drives one unary call the way connect-web does; the SDK's services are all Connect.
func stepConnectH1(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	headers := http.Header{
		contentType:                []string{"application/json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{"root"},
	}
	payload := `{"path":"/etc/envd-version"}`
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/filesystem.Filesystem/Stat", headers, strings.NewReader(payload))
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

// stepProcessStart spawns through envd's process service, which the image runs with -no-cgroups, and reads its Connect server stream back.
func stepProcessStart(ctx context.Context, rt *harness.PortRelay, _ *sandbox.Sandbox) error {
	headers := http.Header{
		contentType:                []string{"application/connect+json"},
		"Connect-Protocol-Version": []string{"1"},
		"X-User":                   []string{"root"},
	}
	req := fmt.Sprintf(`{"process":{"cmd":"/bin/echo","args":[%q]}}`, processProbe)
	resp, err := rt.Do(ctx, envdPort, http.MethodPost, "/process.Process/Start", headers, strings.NewReader(envelope(req)))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %s: %s", resp.Status, body)
	}
	events, err := readEnvelopes(resp.Body)
	if err != nil {
		return err
	}
	joined := strings.Join(events, "\n")
	if !strings.Contains(joined, `"start"`) {
		return fmt.Errorf("no start event in the stream:\n%s", joined)
	}
	if !strings.Contains(joined, base64.StdEncoding.EncodeToString([]byte(processProbe+"\n"))) {
		return fmt.Errorf("the command's stdout never arrived:\n%s", joined)
	}
	return nil
}

// envelope frames one Connect streaming message: a flag byte, then a big-endian length.
func envelope(payload string) string {
	head := make([]byte, 5)
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload))) //nolint:gosec // a probe payload is tens of bytes
	return string(head) + payload
}

// readEnvelopes unframes a Connect server stream, whose trailing envelope carries the RPC's error; the HTTP status is 200 either way.
func readEnvelopes(r io.Reader) ([]string, error) {
	var out []string
	head := make([]byte, 5)
	for {
		if _, err := io.ReadFull(r, head); err != nil {
			return out, fmt.Errorf("stream ended before its end-of-stream envelope: %w", err)
		}
		body := make([]byte, binary.BigEndian.Uint32(head[1:]))
		if _, err := io.ReadFull(r, body); err != nil {
			return out, err
		}
		if head[0]&0x02 == 0 {
			out = append(out, string(body))
			continue
		}
		var end struct {
			Error json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(body, &end); err != nil {
			return out, fmt.Errorf("parse end-of-stream envelope %q: %w", body, err)
		}
		if len(end.Error) > 0 {
			return out, fmt.Errorf("rpc failed: %s", end.Error)
		}
		return out, nil
	}
}

// stepProtocol fails the day envd gains h2c, which is when a proxy may forward HTTP/2 upstream.
func stepProtocol(ctx context.Context, rt *harness.PortRelay, sb *sandbox.Sandbox) error {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{
		Protocols:   &protocols,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return rt.Dial(ctx, envdPort) },
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://guest/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		return fmt.Errorf("envd answered h2c with %s", resp.Status)
	}
	// an error here must mean h2c specifically, not a guest that stopped answering at all
	return stepHealth(ctx, rt, sb)
}
