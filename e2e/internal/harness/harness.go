// Package harness carries helpers shared by the e2e tools.
package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const GuestProxy = "http://127.0.0.1:3128"

type RelayConn interface {
	net.Conn
	CloseWrite() error
}

// PortRelay drives the guest-port endpoint the way an edge proxy does, not via the SDK's DialPort.
type PortRelay struct {
	Owner string
	ID    string
	Token string
}

// URL is the endpoint a given guest port is reached through.
func (p PortRelay) URL(port uint16) string {
	return "http://" + p.Owner + "/v1/sandboxes/" + p.ID + "/ports/" + strconv.FormatUint(uint64(port), 10)
}

// Dial upgrades to the raw relay and hands back the connection.
func (p PortRelay) Dial(ctx context.Context, port uint16) (RelayConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.Owner)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := Request(ctx, p.URL(port), p.Token, true)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if writeErr := req.Write(conn); writeErr != nil {
		_ = conn.Close()
		return nil, writeErr
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, fmt.Errorf("upgrade: %s", resp.Status)
	}
	if got := resp.Header.Get("Upgrade"); got != "tcp" {
		_ = conn.Close()
		return nil, fmt.Errorf("upgrade header %q, want tcp", got)
	}
	return &portRelayConn{Conn: conn, reader: reader}, nil
}

type portRelayConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *portRelayConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *portRelayConn) CloseWrite() error {
	return c.Conn.(*net.TCPConn).CloseWrite()
}

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

func Poll(ctx context.Context, budget, every time.Duration, try func() error) error {
	deadline := time.Now().Add(budget)
	for {
		err := try()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

// HTTPOverPort sends one HTTP/1.1 request over the port relay and returns a 200 body; the Host stays "localhost" because Chrome's DevTools allowlist accepts it but not proxied vhosts.
func HTTPOverPort(ctx context.Context, sb *sandbox.Sandbox, port uint16, method, path string) ([]byte, error) {
	pc, err := sb.DialPort(ctx, port)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pc.Close() }()
	if _, err = fmt.Fprintf(pc, "%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", method, path); err != nil {
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

// Request builds one call to the endpoint, optionally asking for the upgrade.
func Request(ctx context.Context, url, token string, upgrade bool) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if upgrade {
		req.Header.Set("Upgrade", "tcp")
		req.Header.Set("Connection", "Upgrade")
	}
	return req, nil
}
