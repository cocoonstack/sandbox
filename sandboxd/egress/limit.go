package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/cocoonstack/sandbox/sandboxd/types"
	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

const (
	// limitBurst is the least bucket depth, so one read slice always fits.
	limitBurst = 64 << 10
	// limitSlices cuts a second of a limited direction into reads, so one wait stays near a tenth of a second.
	limitSlices  = 10
	minLimitRead = 1 << 10

	// minWindow keeps a clamped receive window above what a long round trip needs at a low cap.
	minWindow       = 128 << 10
	maxScaledWindow = 65535 << 14
	clampEvery      = 100 * time.Millisecond
)

// SetLimit caps the claim's bytes per second each way and moves every open tunnel onto the new cap at once.
func (p *Proxy) SetLimit(l types.EgressLimit) {
	cur := p.limits.Load()
	if cur == nil && l == (types.EgressLimit{}) || cur != nil && cur.set == l {
		return
	}
	var next *limiters
	if l != (types.EgressLimit{}) {
		next = newLimiters(l, cur)
	}
	if old := p.limits.Swap(next); old != nil {
		old.cancel()
	}
	p.kick()
}

// ReportEvery adds a row every d for each allowed tunnel or request that lasts longer; call it after OnTransfer, before serving.
func (p *Proxy) ReportEvery(d time.Duration) {
	p.every = d
	go func() {
		tick := time.NewTicker(d)
		defer tick.Stop()
		for {
			select {
			case <-p.stop:
				return
			case now := <-tick.C:
				p.reportInterim(now)
			}
		}
	}()
}

// limiter is the current bucket of one direction and the cap it belongs to; nil when that direction is open.
func (p *Proxy) limiter(rx bool) (*rate.Limiter, *limiters) {
	l := p.limits.Load()
	if l == nil {
		return nil, nil
	}
	if rx {
		return l.rx, l
	}
	return l.tx, l
}

// begin registers an allowed tunnel or request; a tunnel names its guest and upstream conns so kick reaches them.
func (p *Proxy) begin(ev Event, conns ...net.Conn) *transfer {
	t := &transfer{ev: ev, start: time.Now(), conns: conns}
	if len(conns) > 0 || p.every > 0 {
		p.connMu.Lock()
		p.transfers[t] = struct{}{}
		p.connMu.Unlock()
	}
	return t
}

func (p *Proxy) settle(t *transfer, ev Event) {
	p.connMu.Lock()
	t.ev = ev
	p.connMu.Unlock()
}

// finish writes the bytes no interim row carried.
func (p *Proxy) finish(t *transfer, report bool) {
	p.connMu.Lock()
	delete(p.transfers, t)
	interim := t.reportedSent > 0 || t.reportedReceived > 0
	ev, sent, received := t.take()
	p.connMu.Unlock()
	if report && (sent > 0 || received > 0 || !interim) {
		p.reportTransfer(ev, sent, received)
	}
}

func (p *Proxy) reportInterim(now time.Time) {
	type row struct {
		ev             Event
		sent, received int64
	}
	var rows []row
	p.connMu.Lock()
	for t := range p.transfers {
		if now.Sub(t.start) < p.every {
			continue
		}
		if ev, sent, received := t.take(); sent > 0 || received > 0 {
			rows = append(rows, row{ev, sent, received})
		}
	}
	p.connMu.Unlock()
	for _, r := range rows {
		p.reportTransfer(r.ev, r.sent, r.received)
	}
	// A spliced tunnel counts its bytes only when its copy returns.
	p.kick()
}

// kick sets a past read deadline on every open tunnel, so each copy returns, counts and resumes under the current limit.
func (p *Proxy) kick() {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	for t := range p.transfers {
		for _, c := range t.conns {
			_ = c.SetReadDeadline(time.Unix(1, 0))
		}
	}
}

// splice copies a tunnel both ways until either side ends, half-closing the peer so EOF propagates.
func (p *Proxy) splice(t *transfer) {
	guest, upstream := t.conns[0], t.conns[1]
	ra := spliceable(guest)
	done := make(chan struct{})
	go func() {
		p.stream(ra, upstream, true, &t.received)
		utils.CloseWrite(guest)
		close(done)
	}()
	p.stream(upstream, ra, false, &t.sent)
	utils.CloseWrite(upstream)
	<-done
}

// stream copies src into dst until src ends: in the kernel while its direction is open, through a meter while it is limited.
func (p *Proxy) stream(dst io.Writer, src net.Conn, rx bool, n *atomic.Int64) {
	for {
		var err error
		if lim, _ := p.limiter(rx); lim != nil {
			buf := copyBufs.Get().(*[]byte)
			m := &meter{r: src, p: p, rx: rx, n: n}
			if rx {
				m.win = tcpOf(src)
			}
			// Hide ReaderFrom so the copy reads through the meter into the pooled buffer.
			_, err = io.CopyBuffer(struct{ io.Writer }{dst}, m, *buf)
			copyBufs.Put(buf)
		} else {
			var moved int64
			if rx {
				moved, err = utils.CopyIn(dst, src, p.noSplice)
			} else {
				moved, err = io.Copy(dst, src)
			}
			n.Add(moved)
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return
		}
		_ = src.SetReadDeadline(time.Time{})
	}
}

// limiters is one cap's buckets; ctx ends when a newer cap replaces it, so a wait moves to the new buckets.
type limiters struct {
	set    types.EgressLimit
	tx, rx *rate.Limiter
	ctx    context.Context
	cancel context.CancelFunc
}

func newLimiters(l types.EgressLimit, cur *limiters) *limiters {
	var prev types.EgressLimit
	var tx, rx *rate.Limiter
	if cur != nil {
		prev, tx, rx = cur.set, cur.tx, cur.rx
	}
	if l.TxBytesPerSecond != prev.TxBytesPerSecond {
		tx = newBucket(l.TxBytesPerSecond)
	}
	if l.RxBytesPerSecond != prev.RxBytesPerSecond {
		rx = newBucket(l.RxBytesPerSecond)
	}
	out := &limiters{set: l, tx: tx, rx: rx}
	out.ctx, out.cancel = context.WithCancel(context.Background())
	return out
}

// transfer is one allowed tunnel or request in flight; the reported counts are guarded by Proxy.connMu.
type transfer struct {
	ev    Event
	start time.Time
	conns []net.Conn

	sent, received                 atomic.Int64
	reportedSent, reportedReceived int64

	body meteredBody
	resp meter
}

// take returns the bytes moved since the last row and marks them reported; the caller holds Proxy.connMu.
func (t *transfer) take() (ev Event, sent, received int64) {
	s, r := t.sent.Load(), t.received.Load()
	sent, received = s-t.reportedSent, r-t.reportedReceived
	t.reportedSent, t.reportedReceived = s, r
	return t.ev, sent, received
}

// meter counts what it reads into n and, while its direction is limited, reads one slice at a time and waits for its tokens.
type meter struct {
	r       io.Reader
	p       *Proxy
	rx      bool
	n       *atomic.Int64
	win     *net.TCPConn
	clamped bool
	applied int
	at      time.Time
}

func (m *meter) Read(b []byte) (int, error) {
	lim, l := m.p.limiter(m.rx)
	if lim != nil {
		b = b[:min(len(b), readSlice(lim))]
	}
	n, err := m.r.Read(b)
	m.n.Add(int64(n))
	m.clamp(lim, err != nil && !errors.Is(err, os.ErrDeadlineExceeded))
	for lim != nil && n > 0 && lim.WaitN(l.ctx, n) != nil {
		lim, l = m.p.limiter(m.rx)
	}
	return n, err
}

// clamp sets the window again every clampEvery, because the kernel raises it while it tunes the receive buffer.
func (m *meter) clamp(lim *rate.Limiter, done bool) {
	if m.win == nil {
		return
	}
	if lim == nil || done {
		if m.clamped {
			clampWindow(m.win, maxScaledWindow)
			m.clamped = false
		}
		return
	}
	w := max(minWindow, min(int(lim.Limit())/2, maxScaledWindow))
	if now := time.Now(); !m.clamped || w != m.applied || now.Sub(m.at) >= clampEvery {
		clampWindow(m.win, w)
		m.clamped, m.applied, m.at = true, w, now
	}
}

type meteredBody struct {
	meter
	io.Closer
}

func newBucket(perSecond int64) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), max(int(min(perSecond, 1<<30)), limitBurst))
}

func readSlice(lim *rate.Limiter) int {
	return max(minLimitRead, min(int(lim.Limit())/limitSlices, limitBurst))
}

func tcpOf(c net.Conn) *net.TCPConn {
	for {
		switch v := c.(type) {
		case *net.TCPConn:
			return v
		case *upstreamConn:
			c = v.Conn
		case interface{ NetConn() net.Conn }:
			c = v.NetConn()
		default:
			return nil
		}
	}
}
