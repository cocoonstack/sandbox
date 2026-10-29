package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/protocol/wire"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
)

const (
	// infoWait bounds how long a lone info frame waits for the call a client pipelines behind it.
	infoWait = 100 * time.Millisecond
	routeBuf = 64 << 10
)

var (
	readOnlyOps = map[string]bool{
		wire.FsStat{}.Op(): true,
		wire.FsList{}.Op(): true,
		wire.FsRead{}.Op(): true,
		wire.FsPull{}.Op(): true,
	}
	infoOp = wire.Info{}.Op()

	errReaderGone  = errors.New("reader connection lost")
	errUnreachable = errors.New("guest agent unreachable")
)

type router struct {
	s     *Server
	ctx   context.Context
	id    string
	token string

	client net.Conn
	br     *bufio.Reader

	reader     net.Conn
	source     string
	pumped     chan struct{}
	superseded atomic.Bool

	pending []byte
	replay  []byte
}

func (r *router) run() (net.Conn, func(), bool) {
	defer r.closeReader()
	for {
		head, partial, err := r.readHead()
		if err != nil {
			return nil, nil, false
		}
		op := wire.RequestOp(head)
		switch {
		case wire.IsContinuation(head):
			if r.reader == nil {
				err = r.pass(head, partial, io.Discard)
			} else {
				err = r.pass(head, partial, r.reader)
			}
			if err != nil {
				return nil, nil, false
			}
		case op == infoOp && r.reader == nil:
			r.pending = append(r.pending, head...)
			followed, waitErr := r.infoFollowed()
			if waitErr != nil {
				return nil, nil, false
			}
			if !followed {
				return r.handover(nil)
			}
		case readOnlyOps[op] || op == infoOp:
			switch err = r.toReader(head, partial); {
			case err == nil:
			case errors.Is(err, errReaderGone):
				return nil, nil, false
			case errors.Is(err, pool.ErrUnknownSandbox), errors.Is(err, pool.ErrLeaseLost):
				r.fail(err)
				return nil, nil, false
			default:
				log.WithFunc("server.router.run").Warnf(r.ctx, "no reader for %s, waking it: %v", r.id, err)
				return r.handover(head)
			}
		default:
			return r.handover(head)
		}
	}
}

func (r *router) toReader(head []byte, partial bool) error {
	if r.reader != nil && r.s.mgr.SleepingSource(r.id, r.token) != r.source {
		r.closeReader()
	}
	if r.reader == nil {
		if err := r.dialReader(); err != nil {
			return err
		}
	}
	if r.s.mgr.AuditEnabled() {
		r.audit(head, partial)
	}
	if err := r.pass(head, partial, r.reader); err != nil {
		return errReaderGone
	}
	return nil
}

func (r *router) dialReader() error {
	conn, source, err := r.s.mgr.DialReader(r.ctx, r.id, r.token)
	if err != nil {
		return err
	}
	r.reader, r.source = conn, source
	r.startPump()
	if len(r.pending) == 0 {
		return nil
	}
	if r.s.mgr.AuditEnabled() {
		r.auditPending()
	}
	if _, err = conn.Write(r.pending); err != nil {
		return errReaderGone
	}
	r.pending = nil
	return nil
}

func (r *router) handover(head []byte) (net.Conn, func(), bool) {
	r.closeReader()
	guest, done, err := r.s.openGuest(r.ctx, r.id, r.token)
	if err != nil {
		r.fail(err)
		return nil, nil, false
	}
	r.replay = slices.Concat(r.pending, head)
	return guest, done, true
}

func (r *router) readHead() (head []byte, partial bool, err error) {
	head, err = r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return head, true, nil
	}
	return head, false, err
}

func (r *router) pass(head []byte, partial bool, w io.Writer) error {
	if _, err := w.Write(head); err != nil {
		return err
	}
	for partial {
		next, err := r.br.ReadSlice('\n')
		partial = errors.Is(err, bufio.ErrBufferFull)
		if err != nil && !partial {
			return err
		}
		if _, err = w.Write(next); err != nil {
			return err
		}
	}
	return nil
}

func (r *router) infoFollowed() (bool, error) {
	if r.br.Buffered() > 0 {
		return true, nil
	}
	_ = r.client.SetReadDeadline(time.Now().Add(infoWait))
	_, err := r.br.Peek(1)
	_ = r.client.SetReadDeadline(time.Time{})
	if timeout, ok := errors.AsType[net.Error](err); ok && timeout.Timeout() {
		return false, nil
	}
	return err == nil, err
}

func (r *router) startPump() {
	r.superseded.Store(false)
	r.pumped = make(chan struct{})
	reader := r.reader
	go func() {
		defer close(r.pumped)
		_, _ = io.Copy(r.client, reader)
		if !r.superseded.Load() {
			_ = r.client.Close()
		}
	}()
}

func (r *router) closeReader() {
	if r.reader == nil {
		return
	}
	r.superseded.Store(true)
	_ = r.reader.Close()
	<-r.pumped
	r.reader = nil
}

// fail answers with an error frame, the only reply left once the 101 has gone out.
func (r *router) fail(err error) {
	kind, msg := wire.KindInternal, "sandbox lookup failed"
	switch _, mapped, ok := lookupPoolErr(err); {
	case errors.Is(err, errUnreachable):
		msg = errUnreachable.Error()
		log.WithFunc("server.fail").Errorf(r.ctx, err, "dial silkd for %s", r.id)
	case ok:
		msg = mapped
		if errors.Is(err, pool.ErrUnknownSandbox) {
			kind = wire.KindNotFound
		}
	default:
		log.WithFunc("server.fail").Errorf(r.ctx, err, "agent socket for %s", r.id)
	}
	frame, encodeErr := wire.EncodeResponse(&wire.ErrorResp{Kind: kind, Message: msg})
	if encodeErr != nil {
		return
	}
	_, _ = r.client.Write(append(frame, '\n'))
}

func (r *router) auditPending() {
	for line := range bytes.SplitSeq(bytes.TrimSuffix(r.pending, newline), newline) {
		r.s.mgr.Audit(r.ctx, r.id, line)
	}
}

func (r *router) audit(head []byte, partial bool) {
	if partial {
		r.s.mgr.Audit(r.ctx, r.id, head)
		return
	}
	r.s.mgr.Audit(r.ctx, r.id, bytes.TrimSuffix(head, newline))
}

// serveSleeping answers the 101 before reading any frame, since a client's first frame can wait on it.
func (s *Server) serveSleeping(ctx context.Context, w http.ResponseWriter, id, token string) {
	client, clientBuf, ok := hijack(ctx, w)
	if !ok {
		return
	}
	release, ok := s.trackRelay(client, io.NopCloser(nil))
	if !ok {
		return
	}
	defer release()
	_ = client.SetDeadline(time.Time{})
	keepAlive(client)
	if _, err := client.Write(switchingProtocols); err != nil {
		return
	}
	rt := &router{s: s, ctx: ctx, id: id, token: token, client: client, br: bufio.NewReaderSize(clientReader(client, clientBuf), routeBuf)}
	guest, done, ok := rt.run()
	if !ok {
		return
	}
	defer done()
	defer func() { _ = guest.Close() }()
	s.retargetRelay(client, guest)
	copyBoth(client, s.audited(ctx, id, io.MultiReader(bytes.NewReader(rt.replay), rt.br)), guest, silkdEnds, s.noSplice)
}

func (s *Server) openGuest(ctx context.Context, id, token string) (net.Conn, func(), error) {
	sock, fenced, done, err := s.mgr.WakeAgentSocket(ctx, id, token)
	if err != nil {
		return nil, nil, err
	}
	guest, err := s.dialer.DialSilkd(ctx, sock)
	if err != nil {
		done()
		return nil, nil, fmt.Errorf("%w: %w", errUnreachable, err)
	}
	return guest, fenceGuest(guest, fenced, done), nil
}
