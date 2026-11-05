package outbound

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/time/rate"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
)

const (
	DNSAllow       = "allow"
	DNSFiltered    = "filtered"
	DNSRateLimited = "rate_limited"
	DNSFailed      = "failed"

	resolvConf      = "/etc/resolv.conf"
	dnsIdle         = 30 * time.Second
	dnsExchange     = 2 * time.Second
	dnsQueryRate    = 50
	dnsQueryBurst   = 200
	dnsRefusalRate  = 1
	dnsRefusalBurst = 10
	dnsNamesMax     = 4096
	dnsMsgMax       = 65535
	dnsHeaderLen    = 12
	dnsQueryMax     = 4096
	dnsInflight     = 32
)

var (
	errNoNameserver = errors.New("no nameserver")
	errMalformed    = errors.New("malformed dns query")
	errAnswerID     = errors.New("dns answer for another query")

	udpBufs = sync.Pool{New: func() any { return new([dnsMsgMax]byte) }}
)

// DNSEvent is one guest DNS query and what the resolver did with it.
type DNSEvent struct {
	Name     string
	Type     string
	Answers  int
	Decision string
}

type resolvedName struct {
	name    string
	expires time.Time
}

type resolver struct {
	h        *Host
	record   func(DNSEvent)
	queries  *rate.Limiter
	refusals *rate.Limiter
	inflight chan struct{}

	mu     sync.Mutex
	names  map[netip.Addr]resolvedName
	conns  map[net.Conn]struct{}
	closed bool
}

func newResolver(h *Host, record func(DNSEvent)) *resolver {
	return &resolver{
		h:        h,
		record:   record,
		queries:  rate.NewLimiter(dnsQueryRate, dnsQueryBurst),
		refusals: rate.NewLimiter(dnsRefusalRate, dnsRefusalBurst),
		inflight: make(chan struct{}, dnsInflight),
		names:    map[netip.Addr]resolvedName{},
		conns:    map[net.Conn]struct{}{},
	}
}

func (r *resolver) serve(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go r.serveConn(ctx, conn)
	}
}

func (r *resolver) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for conn := range r.conns {
		_ = conn.Close()
	}
}

func (r *resolver) serveConn(ctx context.Context, conn net.Conn) {
	if !r.track(conn) {
		_ = conn.Close()
		return
	}
	defer r.untrack(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(dnsIdle))
		query, err := readQuery(conn)
		if errors.Is(err, errMalformed) || errors.Is(err, io.ErrUnexpectedEOF) {
			r.h.dnsMalformed.Add(1)
			return
		}
		if err != nil {
			return
		}
		answer, pooled, ok := r.answer(ctx, query)
		if !ok {
			r.h.dnsMalformed.Add(1)
			return
		}
		err = writeDNSMsg(conn, answer)
		if pooled != nil {
			udpBufs.Put(pooled)
		}
		if err != nil {
			return
		}
	}
}

func (r *resolver) track(conn net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.conns[conn] = struct{}{}
	return true
}

func (r *resolver) untrack(conn net.Conn) {
	r.mu.Lock()
	delete(r.conns, conn)
	r.mu.Unlock()
	_ = conn.Close()
}

// answer returns the reply for query; a non-nil pooled buffer backs it and goes back to udpBufs once the reply is written.
func (r *resolver) answer(ctx context.Context, query []byte) ([]byte, *[dnsMsgMax]byte, bool) {
	hdr, q, ok := parseQuery(query)
	if !ok {
		return nil, nil, false
	}
	ev := DNSEvent{Name: strings.TrimSuffix(q.Name.String(), "."), Type: strings.TrimPrefix(q.Type.String(), "Type")}
	if !r.queries.Allow() {
		ev.Decision = DNSRateLimited
		if r.refusals.Allow() {
			r.record(ev)
		}
		return failure(hdr, q, dnsmessage.RCodeRefused)
	}
	r.inflight <- struct{}{}
	resp, pooled, err := r.exchange(ctx, query)
	<-r.inflight
	var drop bool
	if err == nil {
		ev.Answers, drop, err = r.scan(resp, hdr.ID, ev.Name)
	}
	if err != nil {
		if pooled != nil {
			udpBufs.Put(pooled)
		}
		ev.Decision = DNSFailed
		r.record(ev)
		return failure(hdr, q, dnsmessage.RCodeServerFailure)
	}
	if !drop {
		ev.Decision = DNSAllow
		r.record(ev)
		return resp, pooled, true
	}
	ev.Decision = DNSFiltered
	r.record(ev)
	out, err := r.strip(resp)
	if pooled != nil {
		udpBufs.Put(pooled)
	}
	if err != nil {
		return failure(hdr, q, dnsmessage.RCodeServerFailure)
	}
	return out, nil, true
}

// scan walks resp without allocating: it counts the answers kept, reports whether an address record must go, and remembers the kept answer addresses.
func (r *resolver) scan(resp []byte, id uint16, name string) (int, bool, error) {
	var p dnsmessage.Parser
	hdr, err := p.Start(resp)
	if err != nil {
		return 0, false, err
	}
	if hdr.ID != id {
		return 0, false, errAnswerID
	}
	if err = p.SkipAllQuestions(); err != nil {
		return 0, false, err
	}
	self, allow := *r.h.self.Load(), r.h.o.View().internalAllow
	now := time.Now()
	answers, drop := 0, false
	for section := range 3 {
		for {
			rh, err := resourceHeader(&p, section)
			if errors.Is(err, dnsmessage.ErrSectionDone) {
				break
			}
			if err != nil {
				return 0, false, err
			}
			ip, err := resourceAddr(&p, section, rh.Type)
			if err != nil {
				return 0, false, err
			}
			switch {
			case ip.IsValid() && refusedAddr(self, allow, ip):
				drop = true
			case section == 0:
				answers++
				if ip.IsValid() {
					r.remember(ip, name, now.Add(time.Duration(rh.TTL)*time.Second))
				}
			}
		}
	}
	return answers, drop, nil
}

// strip rebuilds resp without the address records the dial guard would refuse, in every section.
func (r *resolver) strip(resp []byte) ([]byte, error) {
	var msg dnsmessage.Message
	if err := msg.Unpack(resp); err != nil {
		return nil, err
	}
	self, allow := *r.h.self.Load(), r.h.o.View().internalAllow
	refused := func(rr dnsmessage.Resource) bool {
		ip := recordAddr(rr)
		return ip.IsValid() && refusedAddr(self, allow, ip)
	}
	msg.Answers = slices.DeleteFunc(msg.Answers, refused)
	msg.Authorities = slices.DeleteFunc(msg.Authorities, refused)
	msg.Additionals = slices.DeleteFunc(msg.Additionals, refused)
	return msg.Pack()
}

func (r *resolver) remember(ip netip.Addr, name string, expires time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.names[ip]; !ok && len(r.names) >= dnsNamesMax {
		now := time.Now()
		for addr, rn := range r.names {
			if now.After(rn.expires) {
				delete(r.names, addr)
			}
		}
		if len(r.names) >= dnsNamesMax {
			for addr := range r.names {
				delete(r.names, addr)
				break
			}
		}
	}
	r.names[ip] = resolvedName{name: name, expires: expires}
}

func (r *resolver) name(ip netip.Addr) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rn, ok := r.names[canonicalAddr(ip)]
	if !ok || time.Now().After(rn.expires) {
		return "", false
	}
	return rn.name, true
}

func (r *resolver) exchange(ctx context.Context, query []byte) ([]byte, *[dnsMsgMax]byte, error) {
	var errs error
	for _, server := range r.h.o.Nameservers() {
		resp, pooled, err := exchangeWith(ctx, server, query)
		if err == nil {
			return resp, pooled, nil
		}
		errs = errors.Join(errs, err)
	}
	return nil, nil, cmp.Or(errs, errNoNameserver)
}

// exchangeWith asks server over UDP into a pooled buffer, and again over TCP when the reply is truncated.
func exchangeWith(ctx context.Context, server string, query []byte) ([]byte, *[dnsMsgMax]byte, error) {
	buf := udpBufs.Get().(*[dnsMsgMax]byte)
	n, err := exchangeUDP(ctx, server, query, buf[:])
	if err == nil && (n < dnsHeaderLen || buf[2]&0x02 == 0) {
		return buf[:n], buf, nil
	}
	udpBufs.Put(buf)
	if err != nil {
		return nil, nil, err
	}
	resp, err := exchangeTCP(ctx, server, query)
	return resp, nil, err
}

func exchangeUDP(ctx context.Context, server string, query, buf []byte) (int, error) {
	conn, err := dialUpstream(ctx, "udp", server)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.Write(query); err != nil {
		return 0, err
	}
	return conn.Read(buf)
}

func exchangeTCP(ctx context.Context, server string, query []byte) ([]byte, error) {
	conn, err := dialUpstream(ctx, "tcp", server)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if err = writeDNSMsg(conn, query); err != nil {
		return nil, err
	}
	return readDNSMsg(conn)
}

func dialUpstream(ctx context.Context, network, server string) (net.Conn, error) {
	d := net.Dialer{Timeout: dnsExchange}
	conn, err := d.DialContext(ctx, network, server)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(dnsExchange))
	return conn, nil
}

func resourceHeader(p *dnsmessage.Parser, section int) (dnsmessage.ResourceHeader, error) {
	switch section {
	case 0:
		return p.AnswerHeader()
	case 1:
		return p.AuthorityHeader()
	default:
		return p.AdditionalHeader()
	}
}

// resourceAddr reads an A or AAAA body as its address and skips any other body.
func resourceAddr(p *dnsmessage.Parser, section int, typ dnsmessage.Type) (netip.Addr, error) {
	switch typ {
	case dnsmessage.TypeA:
		body, err := p.AResource()
		return canonicalAddr(netip.AddrFrom4(body.A)), err
	case dnsmessage.TypeAAAA:
		body, err := p.AAAAResource()
		return canonicalAddr(netip.AddrFrom16(body.AAAA)), err
	}
	switch section {
	case 0:
		return netip.Addr{}, p.SkipAnswer()
	case 1:
		return netip.Addr{}, p.SkipAuthority()
	default:
		return netip.Addr{}, p.SkipAdditional()
	}
}

func recordAddr(rr dnsmessage.Resource) netip.Addr {
	switch body := rr.Body.(type) {
	case *dnsmessage.AResource:
		return canonicalAddr(netip.AddrFrom4(body.A))
	case *dnsmessage.AAAAResource:
		return canonicalAddr(netip.AddrFrom16(body.AAAA))
	}
	return netip.Addr{}
}

func refusedAddr(self []netip.Addr, allow []egress.InternalAllow, ip netip.Addr) bool {
	return internalAddr(self, ip) && !slices.ContainsFunc(allow, func(a egress.InternalAllow) bool { return a.Prefix.Contains(ip) })
}

// parseQuery admits a standard query with one question that the full parser accepts, compression pointers and record bodies included.
func parseQuery(query []byte) (dnsmessage.Header, dnsmessage.Question, bool) {
	var msg dnsmessage.Message
	if msg.Unpack(query) != nil || msg.Response || msg.OpCode != 0 || len(msg.Questions) != 1 {
		return dnsmessage.Header{}, dnsmessage.Question{}, false
	}
	return msg.Header, msg.Questions[0], true
}

// readQuery reads one framed guest query, refusing a length outside a DNS header and dnsQueryMax before it allocates.
func readQuery(r io.Reader) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(size[:])
	if n < dnsHeaderLen || n > dnsQueryMax {
		return nil, errMalformed
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func failure(hdr dnsmessage.Header, q dnsmessage.Question, rcode dnsmessage.RCode) ([]byte, *[dnsMsgMax]byte, bool) {
	out, err := (&dnsmessage.Message{
		ID: hdr.ID, Response: true, OpCode: hdr.OpCode, RecursionDesired: hdr.RecursionDesired, RCode: rcode,
		Questions: []dnsmessage.Question{q},
	}).Pack()
	return out, nil, err == nil
}

func readDNSMsg(r io.Reader) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	msg := make([]byte, binary.BigEndian.Uint16(size[:]))
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func writeDNSMsg(w io.Writer, msg []byte) error {
	if len(msg) > dnsMsgMax {
		return fmt.Errorf("dns message of %d bytes", len(msg))
	}
	bufs := net.Buffers{binary.BigEndian.AppendUint16(nil, uint16(len(msg))), msg} //nolint:gosec // capped above
	_, err := bufs.WriteTo(w)
	return err
}

// nameservers reads path's nameserver lines as host:53 addresses, the loopback default when it names none.
func nameservers(path string) []string {
	f, err := os.Open(path) //nolint:gosec // the node's resolver config
	if err != nil {
		return []string{"127.0.0.1:53"}
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "nameserver" {
			if ip, err := netip.ParseAddr(fields[1]); err == nil {
				out = append(out, netip.AddrPortFrom(ip, 53).String())
			}
		}
	}
	if len(out) == 0 {
		return []string{"127.0.0.1:53"}
	}
	return out
}
