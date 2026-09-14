// desktopsmoke is the desktop-flavor acceptance: claim (2xlarge) → the OSWorld
// guest server over the relay (screenshot, AT-SPI tree, a PyAutoGUI action
// echoed by the cursor) → on the none lane a checkpoint/branch of the warmed
// desktop, on the egress lane a fetch through the session's proxy environment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	serverPort = 5000
	// the GNOME session and osworld-server start after silkd; a cold first launch on a loaded node takes tens of seconds.
	serverWait = 3 * time.Minute
	clickX     = 300
	clickY     = 300
)

var pngMagic = []byte("\x89PNG\r\n\x1a\n")

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/desktop:24.04", "desktop template ref")
	lane := flag.String("net", "none", "lane: none, or egress for a guarded bridge pool with a policy")
	probe := flag.String("probe", "http://postman-echo.com/get", "URL the egress policy allows, fetched from the session's environment on the egress lane")
	flag.Parse()

	if err := run(*addr, *token, *template, sandbox.NetShape(*lane), *probe); err != nil {
		fmt.Fprintln(os.Stderr, "desktopsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("DESKTOPSMOKE PASS")
}

func run(addr, token, template string, lane sandbox.NetShape, probe string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	start := time.Now()
	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(lane), sandbox.WithSize(sandbox.XXLarge),
		sandbox.WithTimeout(30*time.Minute))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claim: desktop 2xlarge up in %.1fs (silkd probed)\n", time.Since(start).Seconds())

	if err = waitScreenshot(ctx, sb); err != nil {
		return err
	}
	if err = accessibilityTree(ctx, sb); err != nil {
		return err
	}
	if err = clickAndReadCursor(ctx, sb); err != nil {
		return err
	}
	if lane == sandbox.NetEgress {
		return proxyReachesSession(ctx, sb, probe)
	}

	ckpt, err := sb.Checkpoint(ctx, "desktop-warmed")
	if err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	defer func() { _ = ckpt.Delete(ctx) }()
	branch, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	defer func() { _ = branch.Close() }()
	if err := waitScreenshot(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	fmt.Println("  checkpoint: branch of the warmed desktop answers /screenshot without relaunch")
	return nil
}

func waitScreenshot(ctx context.Context, sb *sandbox.Sandbox) error {
	deadline := time.Now().Add(serverWait)
	for {
		body, err := harness.HTTPOverPort(ctx, sb, serverPort, "GET", "/screenshot", nil)
		if err == nil {
			if bytes.HasPrefix(body, pngMagic) {
				fmt.Printf("  server: /screenshot → PNG, %d bytes\n", len(body))
				return nil
			}
			err = fmt.Errorf("not a PNG: %q", body[:min(len(body), 16)])
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("osworld-server never answered: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func accessibilityTree(ctx context.Context, sb *sandbox.Sandbox) error {
	body, err := harness.HTTPOverPort(ctx, sb, serverPort, "GET", "/accessibility", nil)
	if err != nil {
		return fmt.Errorf("accessibility: %w", err)
	}
	var reply struct {
		AT string `json:"AT"`
	}
	if err = json.Unmarshal(body, &reply); err != nil {
		return fmt.Errorf("accessibility: %w", err)
	}
	if !strings.Contains(reply.AT, "<desktop-frame") || !strings.Contains(reply.AT, "<application") {
		return fmt.Errorf("accessibility: no AT-SPI desktop in %d bytes", len(reply.AT))
	}
	fmt.Printf("  server: /accessibility → AT-SPI tree, %d bytes\n", len(reply.AT))
	return nil
}

// clickAndReadCursor proves /execute reaches the X display, not just the shell.
func clickAndReadCursor(ctx context.Context, sb *sandbox.Sandbox) error {
	action := fmt.Sprintf("import pyautogui; pyautogui.moveTo(%d, %d); pyautogui.click()", clickX, clickY)
	if _, err := execute(ctx, sb, []string{"python", "-c", action}, 15); err != nil {
		return err
	}
	body, err := harness.HTTPOverPort(ctx, sb, serverPort, "GET", "/cursor_position", nil)
	if err != nil {
		return fmt.Errorf("cursor_position: %w", err)
	}
	var pos [2]int
	if err = json.Unmarshal(body, &pos); err != nil {
		return fmt.Errorf("cursor_position: %w", err)
	}
	if pos != [2]int{clickX, clickY} {
		return fmt.Errorf("cursor_position: got %v, want [%d %d]", pos, clickX, clickY)
	}
	fmt.Printf("  server: /execute pyautogui click → /cursor_position %v\n", pos)
	return nil
}

// proxyReachesSession proves the session's environment carries the relay and the relay reaches an allowed origin.
func proxyReachesSession(ctx context.Context, sb *sandbox.Sandbox, probe string) error {
	script := `test -n "$http_proxy" && curl -sS -m 20 -o /dev/null -w '%{http_code}' ` + probe
	out, err := execute(ctx, sb, []string{"sh", "-c", script}, 30)
	if err != nil {
		return fmt.Errorf("proxy leg: %w", err)
	}
	if strings.TrimSpace(out) != "200" {
		return fmt.Errorf("proxy leg: fetch through the session's proxy returned %q, want 200", strings.TrimSpace(out))
	}
	fmt.Println("  proxy: the session's environment carries the relay and an allowed origin answers 200")
	return nil
}

// execute runs one command through osworld-server and returns its output; a non-zero exit is an error.
func execute(ctx context.Context, sb *sandbox.Sandbox, command []string, timeout int) (string, error) {
	req, _ := json.Marshal(map[string]any{"command": command, "shell": false, "timeout": timeout})
	body, err := harness.HTTPOverPort(ctx, sb, serverPort, "POST", "/execute", req)
	if err != nil {
		return "", fmt.Errorf("execute: %w", err)
	}
	var reply struct {
		Status     string `json:"status"`
		Output     string `json:"output"`
		ReturnCode int    `json:"returncode"`
		Error      string `json:"error"`
	}
	if err = json.Unmarshal(body, &reply); err != nil {
		return "", fmt.Errorf("execute: %w", err)
	}
	if reply.Status != "success" || reply.ReturnCode != 0 {
		return "", fmt.Errorf("execute: %s rc=%d: %s", reply.Status, reply.ReturnCode, strings.TrimSpace(reply.Error))
	}
	return reply.Output, nil
}
