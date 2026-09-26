// browsersmoke is the browser-flavor acceptance: claim the browser flavor → CDP
// /json/version over the relay → open a target → checkpoint/branch of the
// warmed browser.
package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/e2e/internal/harness"
	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	cdpPort = 9222
	// Chromium starts after silkd, so the claim returns before CDP is up; a cold first launch takes tens of seconds.
	cdpWait = 3 * time.Minute
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "sandboxd address")
	token := flag.String("token", "", "node api token")
	template := flag.String("template", "ghcr.io/cocoonstack/sandbox/browser:24.04", "browser template ref")
	flag.Parse()

	if err := run(*addr, *token, *template); err != nil {
		fmt.Fprintln(os.Stderr, "browsersmoke:", err)
		os.Exit(1)
	}
	fmt.Println("BROWSERSMOKE PASS")
}

func run(addr, token, template string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	start := time.Now()
	_, sb, err := harness.Claim(ctx, addr, token, template,
		sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.Large),
		sandbox.WithTimeout(20*time.Minute))
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close() }()
	fmt.Printf("  claim: browser large up in %.1fs (silkd probed)\n", time.Since(start).Seconds())

	if err = waitCDP(ctx, sb); err != nil {
		return err
	}
	if err = openTarget(ctx, sb); err != nil {
		return err
	}

	ckpt, err := sb.Checkpoint(ctx, "browser-warmed")
	if err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	defer func() { _ = ckpt.Delete(ctx) }()
	branch, err := ckpt.New(ctx)
	if err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	defer func() { _ = branch.Close() }()
	if err := waitCDP(ctx, branch); err != nil {
		return fmt.Errorf("branch: %w", err)
	}
	fmt.Println("  checkpoint: branch of the warmed browser answers /json/version without relaunch")
	return nil
}

func waitCDP(ctx context.Context, sb *sandbox.Sandbox) error {
	deadline := time.Now().Add(cdpWait)
	for {
		ver, err := cdpJSON(ctx, sb, "GET", "/json/version")
		if err == nil {
			if b, _ := ver["Browser"].(string); strings.Contains(b, "Chrome") {
				fmt.Printf("  cdp: /json/version → %s (protocol %v)\n", b, ver["Protocol-Version"])
				return nil
			}
			err = fmt.Errorf("unexpected browser %v", ver["Browser"])
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cdp never answered: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// openTarget proves CDP accepts writes, not just liveness.
func openTarget(ctx context.Context, sb *sandbox.Sandbox) error {
	tgt, err := cdpJSON(ctx, sb, "PUT", "/json/new?about:blank")
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	ws, _ := tgt["webSocketDebuggerUrl"].(string)
	if ws == "" {
		return fmt.Errorf("open target: no webSocketDebuggerUrl in %v", tgt)
	}
	fmt.Println("  cdp: PUT /json/new opened a target (webSocketDebuggerUrl present)")
	return nil
}

func cdpJSON(ctx context.Context, sb *sandbox.Sandbox, method, path string) (map[string]any, error) {
	body, err := harness.HTTPOverPort(ctx, sb, cdpPort, method, path, nil)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	return out, nil
}
