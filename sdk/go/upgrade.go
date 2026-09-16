package sandbox

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
)

func (c *Client) dialAgent(ctx context.Context, addr, id, token string) (net.Conn, error) {
	u, err := endpointURL(addr, c.scheme)
	if err != nil {
		return nil, err
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == httpsScheme {
			port = "443"
		}
	}
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", net.JoinHostPort(u.Hostname(), port))
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	conn := raw
	if u.Scheme == httpsScheme {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.tlsConfig != nil {
			cfg = c.tlsConfig.Clone()
		}
		if cfg.ServerName == "" {
			cfg.ServerName = u.Hostname()
		}
		cfg.NextProtos = []string{"http/1.1"}
		secured := tls.Client(raw, cfg)
		if err = secured.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("agent TLS handshake: %w", err)
		}
		conn = secured
	}

	if strings.ContainsAny(id, "\r\n\x00") || strings.ContainsAny(token, "\r\n\x00") {
		_ = raw.Close()
		return nil, fmt.Errorf("agent upgrade: id or token contains a control character")
	}
	req := "GET /v1/sandboxes/" + id + "/agent HTTP/1.1\r\nHost: " + u.Host +
		"\r\nConnection: Upgrade\r\nUpgrade: silkd\r\nAuthorization: Bearer " + token + "\r\n\r\n"
	if _, err = conn.Write([]byte(req)); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = raw.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("read upgrade response: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		err := apiError("agent upgrade", resp)
		_ = resp.Body.Close()
		_ = raw.Close()
		return nil, err
	}
	return &upgradedConn{Conn: conn, r: br}, nil
}

type upgradedConn struct {
	net.Conn
	r *bufio.Reader
}

func (u *upgradedConn) Read(p []byte) (int, error) { return u.r.Read(p) }
