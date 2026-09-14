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

	socksTimeout       = 30 * time.Second
	socksAcceptBackoff = 50 * time.Millisecond
)

// ServeSOCKS serves SOCKS5 on ln until it closes; a tunnel takes the decision of an un-intercepted CONNECT.
func (p *Proxy) ServeSOCKS(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			time.Sleep(socksAcceptBackoff)
			continue
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
	host, port, ok := socksHandshake(conn)
	if !ok {
		return
	}
	decision, intercept := p.tunnelDecision(host, port)
	if intercept || port == 0 {
		decision = DecisionDeny
	}
	p.record(Event{Method: methodSOCKS, Host: host, Port: port, Decision: decision})
	if decision == DecisionDeny {
		_ = socksReply(conn, socksDenied)
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, socksTimeout)
	defer cancel()
	upstream, err := p.dial(dialCtx, "tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
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

// socksHandshake negotiates no-auth and parses one CONNECT request; a refusal is answered before it returns false.
func socksHandshake(conn net.Conn) (host string, port uint16, ok bool) {
	greeting, err := readN(conn, 2)
	if err != nil || greeting[0] != socksVersion {
		return "", 0, false
	}
	methods, err := readN(conn, int(greeting[1]))
	if err != nil {
		return "", 0, false
	}
	if !slices.Contains(methods, byte(socksNoAuth)) {
		_, _ = conn.Write([]byte{socksVersion, socksNoMethod})
		return "", 0, false
	}
	if _, err = conn.Write([]byte{socksVersion, socksNoAuth}); err != nil {
		return "", 0, false
	}
	req, err := readN(conn, 4)
	if err != nil || req[0] != socksVersion {
		return "", 0, false
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
			return "", 0, false
		}
		addrLen = int(n[0])
	default:
		_ = socksReply(conn, socksBadAtyp)
		return "", 0, false
	}
	target, err := readN(conn, addrLen+2)
	if err != nil {
		return "", 0, false
	}
	if req[1] != socksConnect {
		_ = socksReply(conn, socksBadCmd)
		return "", 0, false
	}
	addr := target[:addrLen]
	host = string(addr)
	if req[3] != socksAtypName {
		ip, _ := netip.AddrFromSlice(addr)
		host = ip.String()
	}
	return host, binary.BigEndian.Uint16(target[addrLen:]), true
}

// socksReply answers with an all-zero bind address: CONNECT clients ignore it, and the node's own address stays out of the guest.
func socksReply(w io.Writer, code byte) error {
	_, err := w.Write([]byte{socksVersion, code, 0, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
	return err
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
