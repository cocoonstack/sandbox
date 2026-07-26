package sandbox

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// dialAgent opens the data-plane connection to the owner node: a raw TCP dial
// plus a hand-rolled HTTP Upgrade, so the result is a real net.Conn with no
// Transport pooling or proxying underneath a long-lived byte stream.
func (c *Client) dialAgent(ctx context.Context, addr, id, token string) (net.Conn, error) {
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}

	// id/token interpolate into the raw request; CR/LF would inject headers.
	if strings.ContainsAny(id, "\r\n\x00") || strings.ContainsAny(token, "\r\n\x00") {
		_ = raw.Close()
		return nil, fmt.Errorf("agent upgrade: id or token contains a control character")
	}
	req := "GET /v1/sandboxes/" + id + "/agent HTTP/1.1\r\nHost: " + addr +
		"\r\nConnection: Upgrade\r\nUpgrade: silkd\r\nAuthorization: Bearer " + token + "\r\n\r\n"
	if _, err = raw.Write([]byte(req)); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}

	br := bufio.NewReader(raw)
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
	_ = raw.SetDeadline(time.Time{})
	return &upgradedConn{Conn: raw, r: br}, nil
}

// upgradedConn keeps reading through the handshake reader forever, so frame
// bytes the server coalesced behind the 101 are never lost.
type upgradedConn struct {
	net.Conn
	r *bufio.Reader
}

func (u *upgradedConn) Read(p []byte) (int, error) { return u.r.Read(p) }
