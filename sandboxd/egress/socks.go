package egress

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"time"
)

const (
	methodSOCKS = "SOCKS5"

	socksVersion   = 0x05
	socksNoAuth    = 0x00
	socksNoMethod  = 0xff
	socksConnect   = 0x01
	socksAtypIPv4  = 0x01
	socksAtypName  = 0x03
	socksAtypIPv6  = 0x04
	socksGranted   = 0x00
	socksDenied    = 0x02
	socksUnreached = 0x04
	socksBadCmd    = 0x07
	socksBadAtyp   = 0x08

	socksTimeout = 30 * time.Second
)

var (
	errSocksVersion = errors.New("socks: unsupported version")
	errSocksRefused = errors.New("socks: request refused")
)

// ServeStream serves SOCKS5 on ln until it closes; a tunnel takes the same decision as a CONNECT.
func (p *Proxy) ServeStream(ctx context.Context, ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go p.serveSocks(ctx, conn)
	}
}

func (p *Proxy) serveSocks(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	if !p.track(conn) {
		return
	}
	defer p.untrack(conn)
	_ = conn.SetDeadline(time.Now().Add(socksTimeout))
	host, port, err := socksHandshake(conn)
	if err != nil {
		return
	}
	decision, intercept := p.tunnelDecision(host)
	if intercept {
		decision = DecisionDeny
	}
	p.record(Event{Method: methodSOCKS, Host: host, Decision: decision})
	if decision == DecisionDeny {
		_ = socksReply(conn, socksDenied)
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, socksTimeout)
	defer cancel()
	upstream, err := p.dial(dialCtx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		_ = socksReply(conn, socksUnreached)
		return
	}
	defer func() { _ = upstream.Close() }()
	if socksReply(conn, socksGranted) != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	splice(conn, upstream)
}

// socksHandshake negotiates no-auth and parses one CONNECT request; a refusal is answered before it returns.
func socksHandshake(conn net.Conn) (host, port string, err error) {
	greeting, err := readN(conn, 2)
	if err != nil {
		return "", "", err
	}
	if greeting[0] != socksVersion {
		return "", "", errSocksVersion
	}
	methods, err := readN(conn, int(greeting[1]))
	if err != nil {
		return "", "", err
	}
	if !slices.Contains(methods, byte(socksNoAuth)) {
		_, _ = conn.Write([]byte{socksVersion, socksNoMethod})
		return "", "", errSocksRefused
	}
	if _, err = conn.Write([]byte{socksVersion, socksNoAuth}); err != nil {
		return "", "", err
	}
	req, err := readN(conn, 4)
	if err != nil {
		return "", "", err
	}
	if req[0] != socksVersion {
		return "", "", errSocksVersion
	}
	addrLen := 0
	switch req[3] {
	case socksAtypIPv4:
		addrLen = 4
	case socksAtypIPv6:
		addrLen = 16
	case socksAtypName:
		var n []byte
		if n, err = readN(conn, 1); err != nil {
			return "", "", err
		}
		addrLen = int(n[0])
	default:
		_ = socksReply(conn, socksBadAtyp)
		return "", "", errSocksRefused
	}
	addr, err := readN(conn, addrLen)
	if err != nil {
		return "", "", err
	}
	portBytes, err := readN(conn, 2)
	if err != nil {
		return "", "", err
	}
	if req[1] != socksConnect {
		_ = socksReply(conn, socksBadCmd)
		return "", "", errSocksRefused
	}
	host = string(addr)
	if ip, ok := netip.AddrFromSlice(addr); ok && req[3] != socksAtypName {
		host = ip.String()
	}
	return host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes))), nil
}

// socksReply answers with an all-zero IPv4 bind address, which every client ignores for CONNECT.
func socksReply(w io.Writer, code byte) error {
	_, err := w.Write([]byte{socksVersion, code, 0, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
	return err
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
