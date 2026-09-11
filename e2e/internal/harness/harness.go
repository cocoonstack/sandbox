// Package harness carries the claim prologue shared by the e2e tools.
package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

// Claim connects to a node and claims one sandbox; the caller owns Close.
func Claim(ctx context.Context, addr, token, template string, opts ...sandbox.Option) (*sandbox.Client, *sandbox.Sandbox, error) {
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return nil, nil, err
	}
	sb, err := client.New(ctx, template, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("claim: %w", err)
	}
	return client, sb, nil
}

// HTTPOverPort sends one HTTP/1.1 request over the port relay and returns a 200 body; the Host stays "localhost" because Chrome's DevTools allowlist accepts it but not proxied vhosts.
func HTTPOverPort(ctx context.Context, sb *sandbox.Sandbox, port uint16, method, path string, body []byte) ([]byte, error) {
	pc, err := sb.DialPort(ctx, port)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pc.Close() }()
	head := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", method, path)
	if body != nil {
		head += fmt.Sprintf("Content-Type: application/json\r\nContent-Length: %d\r\n", len(body))
	}
	if _, err = fmt.Fprintf(pc, "%s\r\n%s", head, body); err != nil {
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
