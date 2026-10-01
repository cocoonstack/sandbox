package egress

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

// Router picks each connection's path: Route names it and Dial opens a connection over it.
type Router interface {
	Route() string
	Dial(ctx context.Context, route, network, addr string) (net.Conn, error)
}

// UpstreamAllow is the set of proxies a claim may route through: exact host names and IP prefixes.
type UpstreamAllow struct {
	hosts    []string
	prefixes []netip.Prefix
}

// ParseUpstreamAllow reads allow entries, each a host name, an IP or a CIDR prefix.
func ParseUpstreamAllow(entries []string) (UpstreamAllow, error) {
	var a UpstreamAllow
	for _, entry := range entries {
		if p, err := netip.ParsePrefix(entry); err == nil {
			a.prefixes = append(a.prefixes, p.Masked())
			continue
		}
		if ip, err := netip.ParseAddr(entry); err == nil {
			a.prefixes = append(a.prefixes, netip.PrefixFrom(ip.Unmap(), ip.Unmap().BitLen()))
			continue
		}
		if !validHostName(entry) {
			return UpstreamAllow{}, fmt.Errorf("upstream allow %q is not a host name, an IP or a prefix", entry)
		}
		a.hosts = append(a.hosts, strings.ToLower(entry))
	}
	return a, nil
}

// Allows reports whether u's host is on the list; an IP host matches only a prefix, a name only a name.
func (a UpstreamAllow) Allows(u *url.URL) bool {
	host := u.Hostname()
	if ip, err := netip.ParseAddr(host); err == nil {
		ip = ip.Unmap()
		return slices.ContainsFunc(a.prefixes, func(p netip.Prefix) bool { return p.Contains(ip) })
	}
	return slices.Contains(a.hosts, strings.ToLower(host))
}

// ParseUpstream reads an upstream proxy URL, http:// (CONNECT) or socks5://, with optional user:pass and a required port.
func ParseUpstream(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("upstream is not a URL")
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "socks5":
		return nil, errors.New("upstream scheme must be http or socks5")
	case u.Hostname() == "" || u.Port() == "":
		return nil, errors.New("upstream needs a host and a port")
	case u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "":
		return nil, errors.New("upstream takes no path or query")
	}
	return u, nil
}

// UpstreamHost names an upstream for audit: its host and port, never its credentials.
func UpstreamHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

// DialUpstream opens a tunnel to target through the upstream proxy u; dial reaches the proxy itself.
func DialUpstream(ctx context.Context, u *url.URL, target string, dial DialFunc) (net.Conn, error) {
	conn, err := dial(ctx, "tcp", u.Host)
	if err != nil {
		return nil, fmt.Errorf("dial upstream %s: %w", u.Host, err)
	}
	if u.Scheme == "socks5" {
		err = socksTunnel(ctx, conn, u, target)
	} else {
		err = connectTunnel(ctx, conn, u, target)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// socksTunnel handshakes on conn itself, so the tunnel keeps the TCP conn's CloseWrite.
func socksTunnel(ctx context.Context, conn net.Conn, u *url.URL, target string) error {
	var auth *proxy.Auth
	if u.User != nil {
		pass, _ := u.User.Password()
		auth = &proxy.Auth{User: u.User.Username(), Password: pass}
	}
	d, err := proxy.SOCKS5("tcp", u.Host, auth, nil)
	if err != nil {
		return err
	}
	hs, ok := d.(interface {
		DialWithConn(ctx context.Context, c net.Conn, network, address string) (net.Addr, error)
	})
	if !ok {
		return errors.New("socks5 dialer cannot handshake on an open conn")
	}
	if _, err := hs.DialWithConn(ctx, conn, "tcp", target); err != nil {
		return fmt.Errorf("upstream %s: %w", u.Host, err)
	}
	return nil
}

func connectTunnel(ctx context.Context, conn net.Conn, u *url.URL, target string) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: target}, Host: target, Header: http.Header{}}
	if u.User != nil {
		pass, _ := u.User.Password()
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+pass)))
	}
	if err := req.Write(conn); err != nil {
		return fmt.Errorf("upstream %s: %w", u.Host, err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return fmt.Errorf("upstream %s: %w", u.Host, err)
	}
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("upstream %s refused the tunnel: %s", u.Host, resp.Status)
	case br.Buffered() > 0:
		return fmt.Errorf("upstream %s sent bytes before the tunnel opened", u.Host)
	case !stop():
		return ctx.Err()
	}
	return conn.SetDeadline(time.Time{})
}

func validHostName(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	return !strings.ContainsFunc(s, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '.'
	})
}
