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
	resolvConfTTL   = 5 * time.Second
	dnsIdle         = 30 * time.Second
	dnsExchange     = 2 * time.Second
	dnsAcceptRetry  = 50 * time.Millisecond
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
	errMalformed    = errors.New("malformed dns message")
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

type nameserverList struct {
	servers []string
	read    time.Time
}

type resolver struct {
	h        *Host
	ctx      context.Context
	cancel   context.CancelFunc
	record   func(DNSEvent)
	queries  *rate.Limiter
	refusals *rate.Limiter
	inflight chan struct{}

	mu    sync.Mutex
	names map[netip.Addr]resolvedName
}

func newResolver(ctx context.Context, h *Host, record func(DNSEvent)) *resolver {
	ctx, cancel := context.WithCancel(ctx)
	return &resolver{
		h:        h,
		ctx:      ctx,
		cancel:   cancel,
		record:   record,
		queries:  rate.NewLimiter(dnsQueryRate, dnsQueryBurst),
		refusals: rate.NewLimiter(dnsRefusalRate, dnsRefusalBurst),
		inflight: make(chan struct{}, dnsInflight),
		names:    map[netip.Addr]resolvedName{},
	}
}

func (r *resolver) serve(ln net.Listener, datagram bool) {
	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			time.Sleep(dnsAcceptRetry)
			continue
		}
		go r.serveConn(conn, datagram)
	}
}

func (r *resolver) close() {
	r.cancel()
}

func (r *resolver) serveConn(conn net.Conn, datagram bool) {
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(r.ctx, func() { _ = conn.Close() })
	defer stop()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(dnsIdle))
		query, err := readDNSMsg(conn, dnsQueryMax)
		if errors.Is(err, errMalformed) || errors.Is(err, io.ErrUnexpectedEOF) {
			r.h.dnsMalformed.Add(1)
			return
		}
		if err != nil {
			return
		}
		r.inflight <- struct{}{}
		buf := udpBufs.Get().(*[dnsMsgMax]byte)
		answer, ok := r.answer(query, buf[:], datagram)
		if ok {
			_ = conn.SetWriteDeadline(time.Now().Add(dnsIdle))
			err = writeDNSMsg(conn, answer)
		}
		udpBufs.Put(buf)
		<-r.inflight
		if !ok {
			r.h.dnsMalformed.Add(1)
			return
		}
		if err != nil {
			return
		}
	}
}

// answer returns the reply to query; an unfiltered upstream reply is returned in buf as received.
func (r *resolver) answer(query, buf []byte, datagram bool) ([]byte, bool) {
	hdr, q, ok := parseQuery(query)
	if !ok {
		return nil, false
	}
	if !r.queries.Allow() {
		if r.refusals.Allow() {
			r.record(dnsEvent(q, DNSRateLimited))
		}
		return failure(hdr, q, dnsmessage.RCodeRefused), true
	}
	ev := dnsEvent(q, DNSAllow)
	resp, err := r.exchange(query, buf, datagram)
	var drop bool
	if err == nil {
		ev.Answers, drop, err = r.scan(resp, hdr.ID, ev.Name)
	}
	switch {
	case err != nil:
		ev.Decision = DNSFailed
		r.record(ev)
		return failure(hdr, q, dnsmessage.RCodeServerFailure), true
	case !drop:
		r.record(ev)
		return resp, true
	}
	ev.Decision = DNSFiltered
	r.record(ev)
	out, err := r.strip(resp)
	if err != nil {
		return failure(hdr, q, dnsmessage.RCodeServerFailure), true
	}
	return out, true
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
		for addr := range r.names {
			delete(r.names, addr)
			break
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

func (r *resolver) exchange(query, buf []byte, datagram bool) ([]byte, error) {
	var errs error
	for _, server := range r.h.o.Nameservers() {
		resp, err := exchangeWith(r.ctx, server, query, buf, datagram)
		if err == nil {
			return resp, nil
		}
		errs = errors.Join(errs, err)
	}
	return nil, cmp.Or(errs, errNoNameserver)
}

func (h *Host) nameservers() []string {
	if l := h.ns.Load(); l != nil && time.Since(l.read) < resolvConfTTL {
		return l.servers
	}
	l := &nameserverList{servers: readNameservers(resolvConf), read: time.Now()}
	h.ns.Store(l)
	return l.servers
}

// exchangeWith asks server over UDP into buf; a truncated reply goes back as is to a datagram client and is asked again over TCP for a stream one.
func exchangeWith(ctx context.Context, server string, query, buf []byte, datagram bool) ([]byte, error) {
	n, err := exchangeUDP(ctx, server, query, buf)
	if err != nil {
		return nil, err
	}
	if datagram || n < dnsHeaderLen || buf[2]&0x02 == 0 {
		return buf[:n], nil
	}
	return exchangeTCP(ctx, server, query)
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
	return readDNSMsg(conn, dnsMsgMax)
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

func dnsEvent(q dnsmessage.Question, decision string) DNSEvent {
	return DNSEvent{Name: strings.TrimSuffix(q.Name.String(), "."), Type: strings.TrimPrefix(q.Type.String(), "Type"), Decision: decision}
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

func failure(hdr dnsmessage.Header, q dnsmessage.Question, rcode dnsmessage.RCode) []byte {
	out, _ := (&dnsmessage.Message{
		ID: hdr.ID, Response: true, OpCode: hdr.OpCode, RecursionDesired: hdr.RecursionDesired, RCode: rcode,
		Questions: []dnsmessage.Question{q},
	}).Pack()
	return out
}

// readDNSMsg reads one length-framed message, refusing a length outside a DNS header and limit before it allocates.
func readDNSMsg(r io.Reader, limit int) ([]byte, error) {
	var size [2]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n < dnsHeaderLen || n > limit {
		return nil, errMalformed
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
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

// readNameservers reads path's nameserver lines as host:53 addresses, the loopback default when it names none.
func readNameservers(path string) []string {
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
