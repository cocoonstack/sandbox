// desktopsmoke is the desktop-flavor acceptance: claim (none/2xlarge) → the
// OSWorld guest server over the relay (screenshot, AT-SPI tree, a PyAutoGUI
// action echoed by the cursor) → checkpoint/branch of the warmed desktop.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	serverPort = 5000
	// The GNOME session and osworld-server start after silkd; a cold first
	// launch on a loaded node can take tens of seconds.
	serverWait = 3 * time.Minute
	clickX     = 300
	clickY     = 300
)

var pngMagic = []byte("\x89PNG\r\n\x1a\n")

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/desktop:24.04", "desktop template ref")
	flag.Parse()

	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "desktopsmoke:", err)
		os.Exit(1)
	}
	fmt.Println("DESKTOPSMOKE PASS")
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	start := time.Now()
	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.XXLarge),
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
		body, err := serverRequest(ctx, sb, "GET", "/screenshot", nil)
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
	body, err := serverRequest(ctx, sb, "GET", "/accessibility", nil)
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
	req, _ := json.Marshal(map[string]any{"command": []string{"python", "-c", action}, "shell": false, "timeout": 15})
	body, err := serverRequest(ctx, sb, "POST", "/execute", req)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	var exec struct {
		Status     string `json:"status"`
		ReturnCode int    `json:"returncode"`
		Error      string `json:"error"`
	}
	if err = json.Unmarshal(body, &exec); err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	if exec.Status != "success" || exec.ReturnCode != 0 {
		return fmt.Errorf("execute: %s rc=%d: %s", exec.Status, exec.ReturnCode, strings.TrimSpace(exec.Error))
	}
	body, err = serverRequest(ctx, sb, "GET", "/cursor_position", nil)
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

// serverRequest hand-rolls one HTTP request to osworld-server over the port relay.
func serverRequest(ctx context.Context, sb *sandbox.Sandbox, method, path string, body []byte) ([]byte, error) {
	pc, err := sb.DialPort(ctx, serverPort)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pc.Close() }()
	if _, err = fmt.Fprintf(pc, "%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", method, path, len(body), body); err != nil {
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(pc), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(out)))
	}
	return out, nil
}
