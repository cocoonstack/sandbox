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
		answer, ok := r.answer(ctx, query)
		if !ok {
			r.h.dnsMalformed.Add(1)
			return
		}
		if writeDNSMsg(conn, answer) != nil {
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

func (r *resolver) answer(ctx context.Context, query []byte) ([]byte, bool) {
	hdr, q, ok := parseQuery(query)
	if !ok {
		return nil, false
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
	resp, err := r.exchange(ctx, query)
	<-r.inflight
	var msg dnsmessage.Message
	if err == nil {
		err = msg.Unpack(resp)
	}
	if err != nil || msg.ID != hdr.ID {
		ev.Decision = DNSFailed
		r.record(ev)
		return failure(hdr, q, dnsmessage.RCodeServerFailure)
	}
	ev.Decision = DNSAllow
	if !r.filter(&msg, ev.Name) {
		ev.Answers = len(msg.Answers)
		r.record(ev)
		return resp, true
	}
	ev.Decision = DNSFiltered
	ev.Answers = len(msg.Answers)
	r.record(ev)
	out, err := msg.Pack()
	if err != nil {
		return failure(hdr, q, dnsmessage.RCodeServerFailure)
	}
	return out, true
}

// filter drops address records the dial guard would refuse from every section, and remembers the answer section's kept addresses.
func (r *resolver) filter(msg *dnsmessage.Message, name string) bool {
	self := *r.h.self.Load()
	allow := r.h.o.View().internalAllow
	refused := func(rr dnsmessage.Resource) (netip.Addr, bool) {
		var ip netip.Addr
		switch body := rr.Body.(type) {
		case *dnsmessage.AResource:
			ip = netip.AddrFrom4(body.A)
		case *dnsmessage.AAAAResource:
			ip = netip.AddrFrom16(body.AAAA)
		default:
			return ip, false
		}
		ip = canonicalAddr(ip)
		return ip, internalAddr(self, ip) && !slices.ContainsFunc(allow, func(a egress.InternalAllow) bool { return a.Prefix.Contains(ip) })
	}
	now := time.Now()
	dropped := false
	msg.Answers = slices.DeleteFunc(msg.Answers, func(rr dnsmessage.Resource) bool {
		ip, drop := refused(rr)
		if !drop && ip.IsValid() {
			r.remember(ip, name, now.Add(time.Duration(rr.Header.TTL)*time.Second))
		}
		dropped = dropped || drop
		return drop
	})
	for _, section := range []*[]dnsmessage.Resource{&msg.Authorities, &msg.Additionals} {
		*section = slices.DeleteFunc(*section, func(rr dnsmessage.Resource) bool {
			_, drop := refused(rr)
			dropped = dropped || drop
			return drop
		})
	}
	return dropped
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

func (r *resolver) exchange(ctx context.Context, query []byte) ([]byte, error) {
	var errs error
	for _, server := range r.h.o.Nameservers() {
		resp, err := exchangeWith(ctx, server, query)
		if err == nil {
			return resp, nil
		}
		errs = errors.Join(errs, err)
	}
	return nil, cmp.Or(errs, errNoNameserver)
}

func exchangeWith(ctx context.Context, server string, query []byte) ([]byte, error) {
	resp, err := exchangeOver(ctx, "udp", server, query)
	if err == nil && len(resp) > 2 && resp[2]&0x02 != 0 {
		resp, err = exchangeOver(ctx, "tcp", server, query)
	}
	return resp, err
}

func exchangeOver(ctx context.Context, network, server string, query []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, dnsExchange)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, server)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if network == "tcp" {
		if err = writeDNSMsg(conn, query); err != nil {
			return nil, err
		}
		return readDNSMsg(conn)
	}
	if _, err = conn.Write(query); err != nil {
		return nil, err
	}
	buf := make([]byte, dnsMsgMax)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// parseQuery admits a standard query with one question whose every section parses.
func parseQuery(query []byte) (dnsmessage.Header, dnsmessage.Question, bool) {
	var p dnsmessage.Parser
	hdr, err := p.Start(query)
	if err != nil || hdr.Response || hdr.OpCode != 0 {
		return dnsmessage.Header{}, dnsmessage.Question{}, false
	}
	q, err := p.Question()
	if err != nil {
		return dnsmessage.Header{}, dnsmessage.Question{}, false
	}
	if _, err = p.Question(); !errors.Is(err, dnsmessage.ErrSectionDone) {
		return dnsmessage.Header{}, dnsmessage.Question{}, false
	}
	if p.SkipAllAnswers() != nil || p.SkipAllAuthorities() != nil || p.SkipAllAdditionals() != nil {
		return dnsmessage.Header{}, dnsmessage.Question{}, false
	}
	return hdr, q, true
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

func failure(hdr dnsmessage.Header, q dnsmessage.Question, rcode dnsmessage.RCode) ([]byte, bool) {
	out, err := (&dnsmessage.Message{
		ID: hdr.ID, Response: true, OpCode: hdr.OpCode, RecursionDesired: hdr.RecursionDesired, RCode: rcode,
		Questions: []dnsmessage.Question{q},
	}).Pack()
	return out, err == nil
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
	_, err := w.Write(binary.BigEndian.AppendUint16(make([]byte, 0, 2+len(msg)), uint16(len(msg)))) //nolint:gosec // capped above
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
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
