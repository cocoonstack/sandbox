// androidsmoke proves the android flavor on a live node: an egress-lane
// xlarge claim boots the redroid guest, silkd answers over vsock, ps shows
// the Android init tree, the VNC port speaks RFB through the relay (and a
// preview URL serves the guest's HTTP port when the node has preview
// configured), and a checkpoint branch of the booted guest boots too.
// Sessions are deliberately not exercised: the guest has no bash.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	vncPort   = 5900
	rfbBanner = "RFB "
	// The VNC app starts well after Android's init: give it its own window
	// past the claim's readiness probe.
	vncWait = 3 * time.Minute
	// Android userspace lives under /system/bin, which silkd's base PATH
	// does not carry.
	shellPath = "PATH=/system/bin:/system/xbin:$PATH"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/android:15", "android template ref")
	flag.Parse()

	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "androidsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("ANDROIDSMOKE PASS")
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var copts []sandbox.ClientOption
	if token != "" {
		copts = append(copts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, copts...)
	if err != nil {
		return err
	}

	start := time.Now()
	sb, err := client.New(ctx, template,
		sandbox.WithNetwork(sandbox.NetEgress), sandbox.WithSize(sandbox.XLarge),
		sandbox.WithTimeout(30*time.Minute))
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claim: android xlarge up in %.1fs (silkd probed)\n", time.Since(start).Seconds())

	if treeErr := initTree(ctx, sb); treeErr != nil {
		return treeErr
	}
	if rfbErr := rfbHandshake(ctx, sb); rfbErr != nil {
		return rfbErr
	}
	previewVNC(ctx, sb)

	ckpt, err := sb.Checkpoint(ctx, "android-booted")
	if err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	defer func() { _ = ckpt.Delete(ctx) }()
	branch, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	defer func() { _ = branch.Close() }()
	if err := initTree(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	if err := rfbHandshake(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	fmt.Println("  checkpoint: branch of the booted guest answers ps + RFB")
	return nil
}

// initTree asserts the Android init tree is up over the relay.
func initTree(ctx context.Context, sb *sandbox.Sandbox) error {
	out, err := sb.Exec(ctx, "/system/bin/sh", "-c", shellPath+"; ps -A")
	if err != nil {
		return fmt.Errorf("exec ps: %w", err)
	}
	for _, proc := range []string{"zygote", "system_server"} {
		if !strings.Contains(out, proc) {
			return fmt.Errorf("ps misses %s:\n%s", proc, tail(out))
		}
	}
	fmt.Println("  exec: ps -A shows the Android init tree (zygote, system_server)")
	return nil
}

// rfbHandshake dials the VNC port through the relay until the server
// answers its RFB greeting.
func rfbHandshake(ctx context.Context, sb *sandbox.Sandbox) error {
	deadline := time.Now().Add(vncWait)
	for {
		banner, err := dialBanner(ctx, sb)
		if err == nil && strings.HasPrefix(banner, rfbBanner) {
			fmt.Printf("  vnc: port %d speaks %s\n", vncPort, strings.TrimSpace(banner))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("vnc port %d: banner %q, err %v", vncPort, banner, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func dialBanner(ctx context.Context, sb *sandbox.Sandbox) (string, error) {
	pc, err := sb.DialPort(ctx, vncPort)
	if err != nil {
		return "", err
	}
	defer func() { _ = pc.Close() }()
	buf := make([]byte, 12)
	if _, err := io.ReadFull(pc, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// previewVNC mints a preview URL for the first guest port that answers
// HTTP (droidVNC-NG's web server, when enabled) and fetches it through the
// preview server. Best-effort: a node without preview configured, or a
// lineage without the HTTP server, reports and moves on — RFB through the
// relay is the hard acceptance.
func previewVNC(ctx context.Context, sb *sandbox.Sandbox) {
	port, ok := httpPort(ctx, sb)
	if !ok {
		fmt.Println("  preview: no guest HTTP port found (VNC web UI off) — skipped")
		return
	}
	url, err := sb.PreviewURL(ctx, port, 10*time.Minute)
	if err != nil {
		fmt.Printf("  preview: mint failed (%v) — skipped\n", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Printf("  preview: %v — skipped\n", err)
		return
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // the preview URL was minted by our own node
	if err != nil {
		fmt.Printf("  preview: fetch failed (%v) — skipped\n", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	fmt.Printf("  preview: %s → HTTP %d, %d bytes (guest port %d)\n", url, resp.StatusCode, len(body), port)
}

func httpPort(ctx context.Context, sb *sandbox.Sandbox) (uint16, bool) {
	for _, port := range []uint16{5800, 5801, 8080, 80} {
		pc, err := sb.DialPort(ctx, port)
		if err != nil {
			continue
		}
		if _, err := pc.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err == nil {
			buf := make([]byte, 5)
			if _, err := io.ReadFull(pc, buf); err == nil && strings.HasPrefix(string(buf), "HTTP/") {
				_ = pc.Close()
				return port, true
			}
		}
		_ = pc.Close()
	}
	return 0, false
}

func tail(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > 12 {
		lines = lines[:12]
	}
	return strings.Join(lines, "\n")
}
