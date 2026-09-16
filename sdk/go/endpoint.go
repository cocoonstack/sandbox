package sandbox

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const httpsScheme = "https"

func (c *Client) configureTLS() error {
	tr, ok := cmp.Or(c.hc.Transport, http.DefaultTransport).(*http.Transport)
	switch {
	case !ok && c.tlsConfig != nil:
		return fmt.Errorf("tls configuration requires an http.Transport")
	case !ok:
		c.tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	case c.tlsConfig == nil && c.hc.Transport != nil:
		c.tlsConfig = cmp.Or(tr.TLSClientConfig.Clone(), &tls.Config{MinVersion: tls.VersionTLS12})
	default:
		c.tlsConfig = cmp.Or(c.tlsConfig, &tls.Config{MinVersion: tls.VersionTLS12})
		hc := *c.hc
		tr = tr.Clone()
		tr.TLSClientConfig = c.tlsConfig
		if c.hc.Transport == nil {
			tr.MaxIdleConnsPerHost = idleConnsPerHost
		}
		hc.Transport = tr
		c.hc = &hc
	}
	if c.tlsConfig.ClientSessionCache == nil {
		c.tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(0)
	}
	return nil
}

// WithTLSConfig sets certificate verification for HTTPS requests and agent relays.
func WithTLSConfig(cfg *tls.Config) ClientOption {
	return func(c *Client) { c.tlsConfig = cfg.Clone() }
}

func endpointURL(addr, scheme string) (*url.URL, error) {
	if !strings.Contains(addr, "://") {
		addr = scheme + "://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("parse sandboxd endpoint: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != httpsScheme) || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || strings.ContainsAny(addr, "?#") {
		return nil, fmt.Errorf("sandboxd endpoint must be an http or https origin")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("sandboxd endpoint port must be between 1 and 65535")
		}
	}
	u.Path = ""
	return u, nil
}
