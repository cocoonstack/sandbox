package outbound

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/time/rate"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/outbound/outboundtest"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const bigAnswers = 40

var dnsPolicy = &egress.Policy{DNS: true, Allow: []egress.Rule{{Host: "example.com"}}}

func TestDNSDoorBindsOnlyWhenOptedIn(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy *egress.Policy
		want   bool
	}{
		{"opted in", dnsPolicy, true},
		{"not opted in", &egress.Policy{Allow: []egress.Rule{{Host: "example.com"}}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHost(t, &fakeEngine{}, poolConfig(testKey, tt.policy), nil)
			sb := outboundtest.VsockSandbox(t, "sb_dns", testKey)
			if err := h.ArmProxy(t.Context(), sb); err != nil {
				t.Fatalf("arm proxy: %v", err)
			}
			paths := []string{engine.DNSSocketPath(sb.VsockSocket), engine.DNSDatagramSocketPath(sb.VsockSocket)}
			for _, path := range paths {
				conn, err := net.Dial("unix", path)
				if (err == nil) != tt.want {
					t.Fatalf("%s bound = %v, want %v", filepath.Base(path), err == nil, tt.want)
				}
				if conn != nil {
					_ = conn.Close()
				}
			}
			h.Disarm(sb.ID, true)
			for _, path := range paths {
				if _, err := net.Dial("unix", path); err == nil {
					t.Errorf("%s still accepts after disarm", filepath.Base(path))
				}
			}
		})
	}
}

func TestDNSDoorDropsInternalAnswersAndRemembersTheRest(t *testing.T) {
	upstream := newFakeNameserver(t)
	cfg := poolConfig(testKey, dnsPolicy)
	cfg.EgressInternalAllow = []string{"10.9.0.0/16:5432"}
	events := &dnsEvents{}
	h := newHost(t, &fakeEngine{}, cfg, func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
		o.RecordDNS = events.record
	})
	h.self.Store(&[]netip.Addr{netip.MustParseAddr("93.184.216.99")})
	sb := armDNS(t, h)

	for _, tt := range []struct {
		name     string
		want     []string
		decision string
	}{
		{"mixed.test.", []string{"93.184.216.34"}, DNSFiltered},
		{"inside.test.", nil, DNSFiltered},
		{"node.test.", nil, DNSFiltered},
		{"mapped.test.", nil, DNSFiltered},
		{"allowed.test.", []string{"10.9.1.1"}, DNSAllow},
		{"public.test.", []string{"93.184.216.35", "2606:2800:220:1::1"}, DNSAllow},
	} {
		msg := queryDoor(t, sb, tt.name, dnsmessage.TypeA)
		if got := addresses(msg); !slices.Equal(got, tt.want) {
			t.Errorf("%s answers %v, want %v", tt.name, got, tt.want)
		}
		if msg.RCode != dnsmessage.RCodeSuccess {
			t.Errorf("%s rcode %v, want success", tt.name, msg.RCode)
		}
		ev := events.last(t)
		if ev.Name != strings.TrimSuffix(tt.name, ".") || ev.Type != "A" || ev.Answers != len(tt.want) || ev.Decision != tt.decision {
			t.Errorf("%s event %+v, want %d answers and %s", tt.name, ev, len(tt.want), tt.decision)
		}
	}
	if name, ok := h.ResolvedName(sb.ID, netip.MustParseAddr("93.184.216.34")); !ok || name != "mixed.test" {
		t.Errorf("name of 93.184.216.34 = %q, %v, want mixed.test", name, ok)
	}
	if _, ok := h.ResolvedName(sb.ID, netip.MustParseAddr("10.0.0.5")); ok {
		t.Error("a dropped internal address has a name")
	}
}

func TestDNSDoorPassesAnUnfilteredAnswerByteForByte(t *testing.T) {
	upstream := newFakeNameserver(t)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
	})
	conn := dialDNS(t, armDNS(t, h))
	query := packQuery(t, dnsmessage.Header{ID: 0x5a5a, RecursionDesired: true}, "raw.test.")
	if err := writeDNSMsg(conn, query); err != nil {
		t.Fatal(err)
	}
	got, err := readDNSMsg(conn, dnsMsgMax)
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	if want := fakeAnswer(query, true); !bytes.Equal(got, want) {
		t.Errorf("answer differs from the upstream's bytes:\n got %x\nwant %x", got, want)
	}
}

func TestDNSDoorDropsAnInternalGlueRecord(t *testing.T) {
	upstream := newFakeNameserver(t)
	events := &dnsEvents{}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
		o.RecordDNS = events.record
	})
	msg := queryDoor(t, armDNS(t, h), "glue.test.", dnsmessage.TypeA)
	if got := addresses(msg); !slices.Equal(got, []string{"93.184.216.37"}) {
		t.Errorf("answers %v, want the public address", got)
	}
	if len(msg.Additionals) != 0 || len(msg.Authorities) != 1 {
		t.Errorf("%d additional and %d authority records, want the glue dropped and the NS kept", len(msg.Additionals), len(msg.Authorities))
	}
	if ev := events.last(t); ev.Decision != DNSFiltered || ev.Answers != 1 {
		t.Errorf("event %+v, want filtered with one answer", ev)
	}
}

func TestDNSDoorDisarmEndsAnOpenConnection(t *testing.T) {
	upstream := newFakeNameserver(t)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
	})
	sb := armDNS(t, h)
	conn := dialDNS(t, sb)
	if got := addresses(exchangeDoor(t, conn, "public.test.", dnsmessage.TypeA)); len(got) != 2 {
		t.Fatalf("armed door answers %v, want two addresses", got)
	}
	h.Disarm(sb.ID, false)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	query, err := (&dnsmessage.Message{
		ID:        1,
		Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName("public.test."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}},
	}).Pack()
	if err != nil {
		t.Fatal(err)
	}
	_ = writeDNSMsg(conn, query)
	if answer, err := readDNSMsg(conn, dnsMsgMax); err == nil {
		t.Fatalf("a disarmed door answered %d bytes on a connection opened while armed", len(answer))
	}
}

func TestDNSDoorsHandATruncatedAnswerToEachTransport(t *testing.T) {
	upstream := newFakeNameserver(t)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
	})
	sb := armDNS(t, h)
	gram, err := net.Dial("unix", engine.DNSDatagramSocketPath(sb.VsockSocket))
	if err != nil {
		t.Fatalf("dial datagram door: %v", err)
	}
	t.Cleanup(func() { _ = gram.Close() })
	if msg := exchangeDoor(t, gram, "big.test.", dnsmessage.TypeA); !msg.Truncated || len(msg.Answers) != 0 {
		t.Errorf("datagram door answered truncated=%v with %d answers, want the upstream's truncated reply", msg.Truncated, len(msg.Answers))
	}
	if msg := queryDoor(t, sb, "big.test.", dnsmessage.TypeA); msg.Truncated || len(addresses(msg)) != bigAnswers {
		t.Errorf("stream door answered truncated=%v with %d addresses, want %d over TCP", msg.Truncated, len(addresses(msg)), bigAnswers)
	}
}

func TestADNSDialBeforeTheClaimForcesARebind(t *testing.T) {
	upstream := newFakeNameserver(t)
	events := &dnsEvents{}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
		o.RecordDNS = events.record
	})
	sb := outboundtest.VsockSandbox(t, "sb_dns", testKey)
	sb.VMName = "sbx-dns-1"
	h.Prebind(t.Context(), sb)
	prebound := h.prebound[sb.VMName].ln
	early, err := net.Dial("unix", engine.DNSDatagramSocketPath(sb.VsockSocket))
	if err != nil {
		t.Fatalf("dial the pre-bound door: %v", err)
	}
	t.Cleanup(func() { _ = early.Close() })
	if err = writeDNSMsg(early, packQuery(t, dnsmessage.Header{ID: 1}, "public.test.")); err != nil {
		t.Fatal(err)
	}
	if err = h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	if h.doors[sb.ID].ln != prebound {
		t.Error("the egress door no guest dialed was rebound too")
	}
	_ = early.SetDeadline(time.Now().Add(2 * time.Second))
	if answer, err := readDNSMsg(early, dnsMsgMax); err == nil {
		t.Fatalf("a query queued before the claim got %d bytes under the claim", len(answer))
	}
	if got := addresses(queryDoor(t, sb, "public.test.", dnsmessage.TypeA)); len(got) != 2 {
		t.Errorf("the rebound door answers %v, want two addresses", got)
	}
	if n := events.count(DNSAllow); n != 1 {
		t.Errorf("%d allow events, want only the query made after the claim", n)
	}
}

func TestDNSDoorMovesPastANameserverThatAnswersServfail(t *testing.T) {
	upstream := newFakeNameserver(t)
	failing := newServfailNameserver(t)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{failing, upstream.udp} }
	})
	if got := addresses(queryDoor(t, armDNS(t, h), "public.test.", dnsmessage.TypeA)); len(got) != 2 {
		t.Errorf("answers %v, want the second nameserver's two addresses", got)
	}
	allFailing := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{failing, failing} }
	})
	if msg := queryDoor(t, armDNS(t, allFailing), "public.test.", dnsmessage.TypeA); msg.RCode != dnsmessage.RCodeServerFailure {
		t.Errorf("rcode %v with every nameserver failing, want their SERVFAIL", msg.RCode)
	}
}

func TestDNSDoorAnswersServfailWhenNoNameserverAnswers(t *testing.T) {
	dead, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := dead.LocalAddr().String()
	_ = dead.Close()
	events := &dnsEvents{}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{addr} }
		o.RecordDNS = events.record
	})
	msg := queryDoor(t, armDNS(t, h), "public.test.", dnsmessage.TypeA)
	if msg.RCode != dnsmessage.RCodeServerFailure || events.last(t).Decision != DNSFailed {
		t.Errorf("rcode %v decision %q, want SERVFAIL and failed", msg.RCode, events.last(t).Decision)
	}
}

func TestDNSDoorRefusesAQueryFloodPastItsBucket(t *testing.T) {
	upstream := newFakeNameserver(t)
	events := &dnsEvents{}
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
		o.RecordDNS = events.record
	})
	sb := armDNS(t, h)
	h.mu.Lock()
	res := h.doors[sb.ID].resolver
	h.mu.Unlock()
	res.queries.SetLimit(0)
	res.refusals.SetLimit(0)
	conn := dialDNS(t, sb)
	refused := 0
	for range dnsQueryBurst + 100 {
		if exchangeDoor(t, conn, "public.test.", dnsmessage.TypeA).RCode == dnsmessage.RCodeRefused {
			refused++
		}
	}
	if refused != 100 {
		t.Errorf("%d of %d queries refused with a bucket of %d and no refill, want 100", refused, dnsQueryBurst+100, dnsQueryBurst)
	}
	if logged := events.count(DNSRateLimited); logged != dnsRefusalBurst {
		t.Errorf("%d rate_limited events for %d refusals, want %d", logged, refused, dnsRefusalBurst)
	}
}

func TestDNSDoorDropsAndCountsMalformedQueries(t *testing.T) {
	upstream := newFakeNameserver(t)
	h := newHost(t, &fakeEngine{}, poolConfig(testKey, dnsPolicy), func(o *Options) {
		o.Nameservers = func() []string { return []string{upstream.udp} }
	})
	sb := armDNS(t, h)
	for _, tt := range []struct {
		name   string
		stream []byte
	}{
		{"shorter than a header", []byte{0, 5, 1, 2, 3, 4, 5}},
		{"past the query cap", []byte{0xff, 0xff}},
		{"truncated body", append([]byte{0, 40}, make([]byte, 10)...)},
		{"truncated length", []byte{0}},
		{"compression pointer loop", frame(compressionLoop())},
		{"two questions", frame(packQuery(t, dnsmessage.Header{RecursionDesired: true}, "a.test.", "b.test."))},
		{"a response", frame(packQuery(t, dnsmessage.Header{Response: true}, "a.test."))},
		{"an update", frame(packQuery(t, dnsmessage.Header{OpCode: 5}, "a.test."))},
		{"a truncated additional record", frame(badAdditional(packQuery(t, dnsmessage.Header{}, "a.test.")))},
		{"an additional record named by a pointer to itself", frame(selfPointerAdditional(packQuery(t, dnsmessage.Header{}, "a.test.")))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := h.DNSMalformed()
			conn := dialDNS(t, sb)
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(tt.stream); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = conn.(*net.UnixConn).CloseWrite()
			if answer, err := readDNSMsg(conn, dnsMsgMax); err == nil {
				t.Fatalf("door answered %d bytes to a malformed query", len(answer))
			}
			if got := h.DNSMalformed() - before; got != 1 {
				t.Errorf("malformed count rose by %d, want 1", got)
			}
		})
	}
	if got := addresses(queryDoor(t, sb, "public.test.", dnsmessage.TypeA)); len(got) != 2 {
		t.Errorf("door after the malformed queries answers %v, want two addresses", got)
	}
}

func FuzzDNSDoor(f *testing.F) {
	upstream := newFakeNameserver(f)
	v := NewView(poolConfig(testKey, dnsPolicy), nil)
	var forwarded atomic.Int64
	h := New(Options{View: func() *View { return v }, Nameservers: func() []string {
		forwarded.Add(1)
		return []string{upstream.udp}
	}})
	valid := packQuery(f, dnsmessage.Header{RecursionDesired: true}, "public.test.")
	for _, seed := range [][]byte{
		frame(valid),
		append(frame(valid), frame(valid)...),
		{0, 5, 1, 2, 3, 4, 5},
		{0xff, 0xff},
		append([]byte{0, 40}, make([]byte, 10)...),
		frame(compressionLoop()),
		frame(packQuery(f, dnsmessage.Header{OpCode: 5}, "a.test.")),
		frame(selfPointerAdditional(packQuery(f, dnsmessage.Header{}, "a.test."))),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, stream []byte) {
		r := newResolver(t.Context(), h, func(DNSEvent) {})
		r.queries.SetLimit(rate.Inf)
		conn := &fuzzConn{in: bytes.NewReader(stream)}
		before, sent := h.DNSMalformed(), forwarded.Load()
		r.serveConn(conn, len(stream)%2 == 0)
		if conn.maxRead > dnsQueryMax {
			t.Fatalf("read a %d-byte message, past the %d-byte cap", conn.maxRead, dnsQueryMax)
		}
		if len(stream) < 2 {
			return
		}
		n := int(stream[0])<<8 | int(stream[1])
		if n < dnsHeaderLen || n > dnsQueryMax || len(stream) < 2+n {
			if h.DNSMalformed() == before {
				t.Fatalf("a %d-byte first frame of a %d-byte stream was not counted as malformed", n, len(stream))
			}
			return
		}
		if !standardQuery(stream[2 : 2+n]) {
			if h.DNSMalformed() == before || forwarded.Load() != sent {
				t.Fatalf("a first query the parser refuses was forwarded or not counted: %x", stream[2:2+n])
			}
		}
	})
}

func TestReadNameserversReadsResolvConf(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "resolv.conf")
	if err := os.WriteFile(conf, []byte("# comment\nnameserver 10.0.0.2\nsearch example\nnameserver fe80::1%eth0\nnameserver bogus\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, want := readNameservers(conf), []string{"10.0.0.2:53", "[fe80::1%eth0]:53"}; !slices.Equal(got, want) {
		t.Errorf("nameservers = %v, want %v", got, want)
	}
	if got := readNameservers(filepath.Join(dir, "missing")); !slices.Equal(got, []string{"127.0.0.1:53"}) {
		t.Errorf("nameservers of a missing file = %v, want the loopback default", got)
	}
}

type fakeNameserver struct {
	udp string
}

func newFakeNameserver(t testing.TB) *fakeNameserver {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", pc.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close(); _ = ln.Close() })
	go func() {
		buf := make([]byte, dnsMsgMax)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteTo(fakeAnswer(buf[:n], true), from)
		}
	}()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if query, err := readDNSMsg(conn, dnsMsgMax); err == nil {
					_ = writeDNSMsg(conn, fakeAnswer(query, false))
				}
			}()
		}
	}()
	return &fakeNameserver{udp: pc.LocalAddr().String()}
}

func newServfailNameserver(t testing.TB) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, dnsMsgMax)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var q dnsmessage.Message
			if q.Unpack(buf[:n]) != nil {
				continue
			}
			out, _ := (&dnsmessage.Message{ID: q.ID, Response: true, RCode: dnsmessage.RCodeServerFailure, Questions: q.Questions}).Pack()
			_, _ = pc.WriteTo(out, from)
		}
	}()
	return pc.LocalAddr().String()
}

func fakeAnswer(query []byte, udp bool) []byte {
	var q dnsmessage.Message
	if q.Unpack(query) != nil {
		return nil
	}
	name := q.Questions[0].Name
	resp := dnsmessage.Message{
		ID: q.ID, Response: true, RecursionAvailable: true,
		Questions:   q.Questions,
		Authorities: []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: name, Class: dnsmessage.ClassINET, TTL: 60}, Body: &dnsmessage.NSResource{NS: dnsmessage.MustNewName("ns.test.")}}},
	}
	addA := func(ip string) {
		a := netip.MustParseAddr(ip)
		hdr := dnsmessage.ResourceHeader{Name: name, Class: dnsmessage.ClassINET, TTL: 60}
		if a.Is4() {
			resp.Answers = append(resp.Answers, dnsmessage.Resource{Header: hdr, Body: &dnsmessage.AResource{A: a.As4()}})
		} else {
			resp.Answers = append(resp.Answers, dnsmessage.Resource{Header: hdr, Body: &dnsmessage.AAAAResource{AAAA: a.As16()}})
		}
	}
	switch name.String() {
	case "mixed.test.":
		addA("93.184.216.34")
		addA("10.0.0.5")
	case "inside.test.":
		addA("169.254.169.254")
	case "node.test.":
		addA("93.184.216.99")
	case "mapped.test.":
		addA("::ffff:127.0.0.1")
	case "allowed.test.":
		addA("10.9.1.1")
	case "public.test.":
		addA("93.184.216.35")
		addA("2606:2800:220:1::1")
	case "raw.test.":
		return rawAnswer(q)
	case "glue.test.":
		addA("93.184.216.37")
		resp.Additionals = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName("ns.test."), Class: dnsmessage.ClassINET, TTL: 60}, Body: &dnsmessage.AResource{A: [4]byte{10, 0, 0, 9}}}}
	case "big.test.":
		if udp {
			resp.Truncated = true
			break
		}
		for i := range bigAnswers {
			addA(netip.AddrFrom4([4]byte{93, 184, 217, byte(i + 1)}).String())
		}
	}
	out, _ := resp.Pack()
	return out
}

func armDNS(t *testing.T, h *Host) *types.Sandbox {
	t.Helper()
	sb := outboundtest.VsockSandbox(t, "sb_dns", testKey)
	if err := h.ArmProxy(t.Context(), sb); err != nil {
		t.Fatalf("arm proxy: %v", err)
	}
	t.Cleanup(func() { h.Disarm(sb.ID, true) })
	return sb
}

func dialDNS(t *testing.T, sb *types.Sandbox) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", engine.DNSSocketPath(sb.VsockSocket))
	if err != nil {
		t.Fatalf("dial dns door: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func queryDoor(t *testing.T, sb *types.Sandbox, name string, qtype dnsmessage.Type) dnsmessage.Message {
	t.Helper()
	return exchangeDoor(t, dialDNS(t, sb), name, qtype)
}

func exchangeDoor(t *testing.T, conn net.Conn, name string, qtype dnsmessage.Type) dnsmessage.Message {
	t.Helper()
	query, err := (&dnsmessage.Message{
		ID: 0x5a5a, RecursionDesired: true,
		Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName(name), Type: qtype, Class: dnsmessage.ClassINET}},
	}).Pack()
	if err != nil {
		t.Fatal(err)
	}
	if err = writeDNSMsg(conn, query); err != nil {
		t.Fatalf("write query: %v", err)
	}
	raw, err := readDNSMsg(conn, dnsMsgMax)
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	var msg dnsmessage.Message
	if err = msg.Unpack(raw); err != nil {
		t.Fatalf("unpack answer: %v", err)
	}
	if msg.ID != 0x5a5a {
		t.Fatalf("answer id %#x, want the query's", msg.ID)
	}
	return msg
}

func addresses(msg dnsmessage.Message) []string {
	var out []string
	for _, rr := range msg.Answers {
		switch body := rr.Body.(type) {
		case *dnsmessage.AResource:
			out = append(out, netip.AddrFrom4(body.A).String())
		case *dnsmessage.AAAAResource:
			out = append(out, netip.AddrFrom16(body.AAAA).String())
		}
	}
	return out
}

type dnsEvents struct {
	mu  sync.Mutex
	evs []DNSEvent
}

func (e *dnsEvents) record(_ context.Context, _, _ string, ev DNSEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evs = append(e.evs, ev)
}

func (e *dnsEvents) last(t *testing.T) DNSEvent {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.evs) == 0 {
		t.Fatal("no dns event recorded")
	}
	return e.evs[len(e.evs)-1]
}

func (e *dnsEvents) count(decision string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, ev := range e.evs {
		if ev.Decision == decision {
			n++
		}
	}
	return n
}

// rawAnswer repeats the question name uncompressed and carries an EDNS record, so a repack changes its bytes.
func rawAnswer(q dnsmessage.Message) []byte {
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: q.ID, Response: true, RecursionAvailable: true})
	_ = b.StartQuestions()
	_ = b.Question(q.Questions[0])
	_ = b.StartAnswers()
	_ = b.AResource(dnsmessage.ResourceHeader{Name: q.Questions[0].Name, Class: dnsmessage.ClassINET, TTL: 60}, dnsmessage.AResource{A: [4]byte{93, 184, 216, 36}})
	_ = b.StartAdditionals()
	var opt dnsmessage.ResourceHeader
	_ = opt.SetEDNS0(1232, dnsmessage.RCodeSuccess, true)
	_ = b.OPTResource(opt, dnsmessage.OPTResource{})
	out, _ := b.Finish()
	return out
}

func frame(msg []byte) []byte {
	return append([]byte{byte(len(msg) >> 8), byte(len(msg))}, msg...)
}

func packQuery(t testing.TB, hdr dnsmessage.Header, names ...string) []byte {
	t.Helper()
	msg := dnsmessage.Message{Header: hdr}
	for _, name := range names {
		msg.Questions = append(msg.Questions, dnsmessage.Question{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET})
	}
	out, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// compressionLoop is a one-question query whose name is a pointer to itself.
func compressionLoop() []byte {
	return []byte{0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 12, 0, 1, 0, 1}
}

// badAdditional claims one additional record and carries half a name pointer.
func badAdditional(query []byte) []byte {
	query[11] = 1
	return append(query, 0xc0)
}

func standardQuery(query []byte) bool {
	var msg dnsmessage.Message
	return msg.Unpack(query) == nil && !msg.Response && msg.OpCode == 0 && len(msg.Questions) == 1
}

func selfPointerAdditional(query []byte) []byte {
	query[11] = 1
	off := len(query)
	return append(query, 0xc0|byte(off>>8), byte(off), 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 1, 2, 3, 4)
}

type fuzzConn struct {
	in      *bytes.Reader
	maxRead int
}

func (c *fuzzConn) Read(p []byte) (int, error) {
	c.maxRead = max(c.maxRead, len(p))
	return c.in.Read(p)
}

func (c *fuzzConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fuzzConn) Close() error                     { return nil }
func (c *fuzzConn) LocalAddr() net.Addr              { return nil }
func (c *fuzzConn) RemoteAddr() net.Addr             { return nil }
func (c *fuzzConn) SetDeadline(time.Time) error      { return nil }
func (c *fuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fuzzConn) SetWriteDeadline(time.Time) error { return nil }
