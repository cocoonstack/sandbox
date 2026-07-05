package sandbox

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// dialAgent opens the data-plane connection: a raw TCP dial plus a
// hand-rolled HTTP Upgrade, so the result is a real net.Conn with no
// Transport pooling or proxying underneath a long-lived byte stream.
func (c *Client) dialAgent(ctx context.Context, id, token string) (net.Conn, error) {
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}

	req, err := http.NewRequest(http.MethodGet, c.url("/v1/sandboxes/"+id+"/agent"), nil)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "silkd")
	req.Header.Set("Authorization", "Bearer "+token)
	if err = req.Write(raw); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("write upgrade request: %w", err)
	}

	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, req)
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
