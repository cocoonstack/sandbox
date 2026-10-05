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

// upgradedConn is a relay connection; over plain HTTP the upgrade request leaves with the first frame and the 101 is read on the first Read.
type upgradedConn struct {
	net.Conn
	tcp   net.Conn // under any TLS, for the parked-connection probe
	r     *bufio.Reader
	req   []byte // unsent upgrade request; writes arrive serialized by silkd.Conn
	await bool
}

func (u *upgradedConn) Read(p []byte) (int, error) {
	if u.await {
		if err := u.handshake(); err != nil {
			return 0, err
		}
	}
	return u.r.Read(p)
}

func (u *upgradedConn) Write(p []byte) (int, error) {
	if u.req == nil {
		return u.Conn.Write(p)
	}
	u.req = append(u.req, p...)
	n, err := u.Conn.Write(u.req)
	n = max(n-len(u.req)+len(p), 0)
	u.req = nil
	return n, err
}

func (u *upgradedConn) handshake() error {
	u.await = false
	resp, err := http.ReadResponse(u.r, nil)
	if err != nil {
		_ = u.tcp.Close()
		return fmt.Errorf("read upgrade response: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		err = apiError("agent upgrade", resp)
		_ = resp.Body.Close()
		_ = u.tcp.Close()
		return err
	}
	return nil
}

func (c *Client) dialAgent(ctx context.Context, addr, id, token string) (*upgradedConn, error) {
	if strings.ContainsAny(id, "\r\n\x00") || strings.ContainsAny(token, "\r\n\x00") {
		return nil, fmt.Errorf("agent upgrade: id or token contains a control character")
	}
	scheme, authority, target := agentEndpoint(addr, c.scheme)
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	conn := raw
	if scheme == httpsScheme {
		cfg := c.tlsConfig.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName, _, _ = net.SplitHostPort(target)
		}
		cfg.NextProtos = []string{"http/1.1"}
		secured := tls.Client(raw, cfg)
		if err = secured.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("agent TLS handshake: %w", err)
		}
		conn = secured
	}

	req := "GET /v1/sandboxes/" + id + "/agent HTTP/1.1\r\nHost: " + authority +
		"\r\nConnection: Upgrade\r\nUpgrade: silkd\r\nAuthorization: Bearer " + token + "\r\n\r\n"
	u := &upgradedConn{Conn: conn, tcp: raw, r: bufio.NewReader(conn), req: []byte(req), await: true}
	if scheme != httpsScheme {
		return u, nil
	}
	// a TLS edge may be a proxy that drops bytes sent ahead of the 101
	if _, err = u.Write(nil); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}
	if err = u.handshake(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return u, nil
}

func agentEndpoint(addr, scheme string) (string, string, string) {
	authority := addr
	if s, a, ok := strings.Cut(addr, "://"); ok {
		scheme, authority = strings.ToLower(s), a
	}
	target := authority
	if _, _, err := net.SplitHostPort(authority); err != nil {
		port := "80"
		if scheme == httpsScheme {
			port = "443"
		}
		host := strings.TrimSuffix(strings.TrimPrefix(authority, "["), "]")
		target = net.JoinHostPort(host, port)
	}
	return scheme, authority, target
}
