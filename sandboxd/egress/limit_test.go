package egress

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	testRate = 512 << 10
	bulkDown = 8 << 20
)

func TestALimitCapsOneDirectionOfATunnel(t *testing.T) {
	for _, tt := range []struct {
		name string
		open func(t *testing.T, target string, edits ...proxyEdit) (*Proxy, io.Reader, net.Conn)
	}{
		{"connect", openConnect},
		{"socks", openSocks},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := bulkServer(t, bulkDown)
			p, r, conn := tt.open(t, srv.addr)
			p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate})
			go writeAll(conn, 2*testRate)
			start := time.Now()
			n, _ := io.Copy(io.Discard, r)
			down := time.Since(start)
			got := <-srv.uploads
			if n != bulkDown || got.n != 2*testRate {
				t.Fatalf("moved %d down and %d up, want %d and %d", n, got.n, bulkDown, 2*testRate)
			}
			if got.took < 700*time.Millisecond {
				t.Errorf("upload of two seconds' worth took %v under a limit with a one-second burst", got.took)
			}
			if down > 5*time.Second {
				t.Errorf("download took %v with only tx limited", down)
			}
		})
	}
}

func TestChangingTheLimitMovesAnOpenTunnel(t *testing.T) {
	addr, s := sinkServer(t)
	p, _, conn := openConnect(t, addr)
	if _, err := conn.Write(make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitBytes(t, s, 1<<20)

	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate / 2})
	start := time.Now()
	go writeUntilClosed(conn)
	waitBytes(t, s, 1<<20+limitBurst+testRate/2)
	if took := time.Since(start); took < 700*time.Millisecond {
		t.Errorf("a burst and a second's worth past it took %v after the limit dropped to %d/s", took, testRate/2)
	}

	p.SetLimit(types.EgressLimit{})
	waitBytes(t, s, s.received.Load()+4<<20)
}

func TestChangingOneDirectionKeepsTheOtherBucket(t *testing.T) {
	p := New(Policy{}, nil, nil, fixedDial(""), nil, nil)
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate, RxBytesPerSecond: testRate})
	first := p.limits.Load()
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate})
	second := p.limits.Load()
	if second.tx != first.tx || second.rx != nil {
		t.Errorf("dropping rx swapped the unchanged tx bucket or kept rx: tx kept %v, rx %v", second.tx == first.tx, second.rx)
	}
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate})
	if p.limits.Load() != second {
		t.Error("an unchanged cap replaced the buckets")
	}
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: 2 * testRate})
	if third := p.limits.Load(); third.tx == second.tx {
		t.Error("a changed tx rate kept the old bucket")
	}
}

func TestCloseReleasesAReadWaitingForTokens(t *testing.T) {
	addr, _ := sinkServer(t)
	rows := make(chan int64, 4)
	p, _, conn := openConnect(t, addr, func(p *Proxy) {
		p.OnTransfer(func(_ Event, sent, _ int64) { rows <- sent })
	})
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: 64})
	go writeUntilClosed(conn)
	waitDebt(t, p.limits.Load().tx)

	start := time.Now()
	p.Close()
	select {
	case <-rows:
		if took := time.Since(start); took > time.Second {
			t.Errorf("the tunnel ended %v after Close", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read waiting for tokens outlived Close")
	}
}

func TestReportEveryWritesRowsThatAddUpToTheTunnel(t *testing.T) {
	t.Run("limited upload", func(t *testing.T) {
		srv := bulkServer(t, 0)
		rows := make(chan int64, 64)
		p, r, conn := openConnect(t, srv.addr, reportRows(rows))
		p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate})
		writeAll(conn, 2*testRate)
		_, _ = io.Copy(io.Discard, r)
		<-srv.uploads
		_ = conn.Close()
		sent, count := sumRows(t, rows, 2*testRate)
		if sent != 2*testRate || count < 4 {
			t.Errorf("%d rows carried %d bytes, want several rows adding up to %d", count, sent, 2*testRate)
		}
	})
	t.Run("short tunnel", func(t *testing.T) {
		srv := bulkServer(t, 0)
		rows := make(chan int64, 64)
		_, r, conn := openConnect(t, srv.addr, reportRows(rows))
		writeAll(conn, 4)
		_, _ = io.Copy(io.Discard, r)
		<-srv.uploads
		_ = conn.Close()
		if sent, count := sumRows(t, rows, 4); sent != 4 || count != 1 {
			t.Errorf("%d rows carried %d bytes, want one row of 4", count, sent)
		}
	})
}

func TestInterimRowsOfARoutedRequestNameItsUpstream(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(origin.Close)
	router := &switchRouter{route: "http://up1.example:3128", target: origin.Listener.Addr().String()}
	type row struct {
		upstream string
		sent     int64
	}
	rows := make(chan row, 64)
	p := New(Policy{Allow: []Rule{{Host: "*"}}}, nil, nil, router, nil, nil)
	t.Cleanup(p.Close)
	p.OnTransfer(func(ev Event, sent, _ int64) { rows <- row{ev.Upstream, sent} })
	p.ReportEvery(100 * time.Millisecond)
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, origin.URL+"/", bytes.NewReader(make([]byte, 2*testRate)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := proxyClient(t, serveFront(t, p).URL).Do(req)
	if err != nil {
		t.Fatalf("proxied PUT: %v", err)
	}
	_ = resp.Body.Close()
	var sent int64
	var count int
	for sent < 2*testRate {
		select {
		case r := <-rows:
			sent += r.sent
			count++
			if r.upstream != "up1.example:3128" {
				t.Errorf("row %d of %d bytes names upstream %q", count, r.sent, r.upstream)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("rows carried %d of %d bytes", sent, 2*testRate)
		}
	}
	if count < 4 {
		t.Errorf("an upload of about a second wrote %d rows, want interim ones", count)
	}
}

func TestALimitCapsAForwardedRequestEachWay(t *testing.T) {
	var uploaded atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			n, _ := io.Copy(io.Discard, r.Body)
			uploaded.Store(n)
			return
		}
		_, _ = w.Write(make([]byte, 2*testRate))
	}))
	t.Cleanup(upstream.Close)
	p := New(Policy{Allow: []Rule{{Host: "bulk.internal"}}}, nil, nil, fixedDial(upstream.Listener.Addr().String()), nil, nil)
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: testRate, RxBytesPerSecond: testRate})
	front := serveFront(t, p).URL

	start := time.Now()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://bulk.internal/", bytes.NewReader(make([]byte, 2*testRate)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := proxyClient(t, front).Do(req)
	if err != nil {
		t.Fatalf("proxied PUT: %v", err)
	}
	_ = resp.Body.Close()
	if took := time.Since(start); uploaded.Load() != 2*testRate || took < 700*time.Millisecond {
		t.Errorf("uploaded %d bytes in %v, want %d at no more than %d/s past the burst", uploaded.Load(), took, 2*testRate, testRate)
	}

	start = time.Now()
	resp = proxiedGet(t, front, "http://bulk.internal/")
	n, _ := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if took := time.Since(start); n != 2*testRate || took < 700*time.Millisecond {
		t.Errorf("downloaded %d bytes in %v, want %d at no more than %d/s past the burst", n, took, 2*testRate, testRate)
	}
}

func TestEveryReadSliceFitsTheSmallestBucket(t *testing.T) {
	smallest := newBucket(1).Burst()
	for _, perSecond := range []int64{1, 1 << 10, 64 << 10, 1 << 20, 125_000_000, 1 << 40} {
		if got := readSlice(newBucket(perSecond)); got > smallest || got > newBucket(perSecond).Burst() {
			t.Errorf("rate %d: a %d-byte slice outgrows the smallest bucket %d", perSecond, got, smallest)
		}
	}
	if got := readSlice(newBucket(125_000_000)); got != limitBurst {
		t.Errorf("a 1 Gbit/s cap reads %d-byte slices, want %d", got, limitBurst)
	}
}

func TestEarlyConnectBytesWaitForTxTokens(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	arrived := make(chan time.Time, 1)
	go func() {
		c, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = c.Close() }()
		if _, readErr := c.Read(make([]byte, 16)); readErr == nil {
			arrived <- time.Now()
		}
	}()
	p := New(Policy{Allow: []Rule{{Host: "bulk.internal"}}}, nil, nil, fixedDial(ln.Addr().String()), nil, nil)
	t.Cleanup(p.Close)
	p.SetLimit(types.EgressLimit{TxBytesPerSecond: limitBurst})
	tx := p.limits.Load().tx
	tx.ReserveN(time.Now(), limitBurst)
	tx.ReserveN(time.Now(), limitBurst)

	start := time.Now()
	conn := dialProxy(t, serveFront(t, p).Listener.Addr().String(), "CONNECT bulk.internal:443 HTTP/1.1\r\nHost: bulk.internal:443\r\n\r\nearly")
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case at := <-arrived:
		if took := at.Sub(start); took < 700*time.Millisecond {
			t.Errorf("early bytes reached the upstream after %v with a second of tx debt", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("early bytes never reached the upstream")
	}
}

type proxyEdit func(*Proxy)

type upload struct {
	n    int64
	took time.Duration
}

type bulk struct {
	addr    string
	uploads chan upload
}

type sink struct {
	received atomic.Int64
	reads    chan struct{}
}

func openConnect(t *testing.T, target string, edits ...proxyEdit) (*Proxy, io.Reader, net.Conn) {
	t.Helper()
	p := New(Policy{Allow: []Rule{{Host: "bulk.internal"}}}, nil, nil, fixedDial(target), nil, nil)
	for _, edit := range edits {
		edit(p)
	}
	t.Cleanup(p.Close)
	conn := dialConnect(t, serveFront(t, p).Listener.Addr().String(), "bulk.internal:443")
	t.Cleanup(func() { _ = conn.Close() })
	br := bufio.NewReader(conn)
	if status := readStatus(t, br); status != "HTTP/1.1 200 Connection Established" {
		t.Fatalf("CONNECT status = %q", status)
	}
	return p, br, conn
}

func openSocks(t *testing.T, target string, _ ...proxyEdit) (*Proxy, io.Reader, net.Conn) {
	t.Helper()
	p, addr := socksProxy(t, Policy{Allow: []Rule{{Host: "bulk.internal"}}}, nil, fixedDial(target), nil, nil)
	conn, reply := socksExchange(t, addr, socksGreeting, socksNameRequest("bulk.internal", 443))
	if reply[1] != socksGranted {
		t.Fatalf("reply code = %#x, want granted", reply[1])
	}
	return p, conn, conn
}

func reportRows(rows chan<- int64) proxyEdit {
	return func(p *Proxy) {
		p.OnTransfer(func(_ Event, sent, _ int64) { rows <- sent })
		p.ReportEvery(100 * time.Millisecond)
	}
}

func sumRows(t *testing.T, rows <-chan int64, want int64) (sent int64, count int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for sent < want {
		select {
		case n := <-rows:
			sent += n
			count++
		case <-deadline:
			t.Fatalf("rows carried %d of %d bytes", sent, want)
		}
	}
	select {
	case n := <-rows:
		sent += n
		count++
	case <-time.After(300 * time.Millisecond):
	}
	return sent, count
}

func writeAll(conn net.Conn, n int) {
	_, _ = conn.Write(make([]byte, n))
	_ = conn.(interface{ CloseWrite() error }).CloseWrite()
}

func writeUntilClosed(conn net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		if _, err := conn.Write(buf); err != nil {
			return
		}
	}
}

func bulkServer(t *testing.T, down int) *bulk {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen bulk: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	b := &bulk{addr: ln.Addr().String(), uploads: make(chan upload, 1)}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _ = conn.Write(make([]byte, down))
			_ = conn.(*net.TCPConn).CloseWrite()
		})
		start := time.Now()
		n, _ := io.Copy(io.Discard, conn)
		b.uploads <- upload{n: n, took: time.Since(start)}
		wg.Wait()
	}()
	return b
}

func sinkServer(t *testing.T) (string, *sink) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen sink: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	s := &sink{reads: make(chan struct{}, 1)}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64<<10)
		for {
			n, err := conn.Read(buf)
			s.received.Add(int64(n))
			select {
			case s.reads <- struct{}{}:
			default:
			}
			if err != nil {
				return
			}
		}
	}()
	return ln.Addr().String(), s
}

func waitBytes(t *testing.T, s *sink, want int64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for s.received.Load() < want {
		select {
		case <-s.reads:
		case <-deadline:
			t.Fatalf("sink got %d bytes, want %d", s.received.Load(), want)
		}
	}
}

func waitDebt(t *testing.T, lim *rate.Limiter) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for lim.Tokens() >= 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the bucket kept %v tokens", lim.Tokens())
		}
		runtime.Gosched()
	}
}
