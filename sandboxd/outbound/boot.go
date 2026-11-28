package outbound

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"time"
	"uuid"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	bootNoticeTimeout = 2 * time.Second
	bootNoticeGap     = time.Second
	bootNoticeMax     = 64
	bootAck           = "\n"
)

func (h *Host) armBoot(ctx context.Context, sb *types.Sandbox, ln *net.UnixListener, bind unixBinder) {
	if sb.VsockSocket == "" {
		closeBoot(ln)
		return
	}
	if ln != nil && listenBound(ln) != nil {
		closeBoot(ln)
		ln = nil
	}
	if ln == nil {
		var err error
		if ln, err = bind(engine.DoorPath(sb.VsockSocket, engine.BootPort)); err != nil {
			h.dropReservation(h.boots, sb.ID)
			log.WithFunc("outbound.armBoot").Warnf(ctx, "bind the boot door of %s, a reboot inside its guest goes unseen: %v", sb.ID, err)
			return
		}
	}
	if !h.publishReserved(h.boots, sb.ID, ln) {
		closeBoot(ln)
		return
	}
	go h.serveBoot(context.WithoutCancel(ctx), sb, ln)
}

func (h *Host) takeBootLocked(sb *types.Sandbox) (ln, displaced *net.UnixListener) {
	ln, displaced = h.preboots[sb.VMName], h.boots[sb.ID]
	delete(h.preboots, sb.VMName)
	if sb.VsockSocket == "" {
		delete(h.boots, sb.ID)
	} else {
		h.boots[sb.ID] = nil
	}
	return ln, displaced
}

func (h *Host) serveBoot(ctx context.Context, sb *types.Sandbox, ln *net.UnixListener) {
	acceptEach(ln, func(err error) {
		log.WithFunc("outbound.serveBoot").Warnf(ctx, "boot door of %s: %v", sb.ID, err)
	}, func(conn net.Conn) {
		if h.takeBootNotice(ctx, sb, conn) {
			time.Sleep(bootNoticeGap)
		}
	})
}

func (h *Host) takeBootNotice(ctx context.Context, sb *types.Sandbox, conn net.Conn) bool {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(bootNoticeTimeout))
	line, err := bufio.NewReaderSize(conn, bootNoticeMax).ReadSlice('\n')
	var bootID uuid.UUID
	if err == nil {
		bootID, err = uuid.Parse(strings.TrimSuffix(string(line), "\n"))
	}
	if err != nil {
		log.WithFunc("outbound.takeBootNotice").Debugf(ctx, "drop a malformed boot notice of %s: %v", sb.ID, err)
		return false
	}
	h.o.Rebooted(ctx, sb, bootID.String())
	_, _ = io.WriteString(conn, bootAck)
	return true
}

func closeBoot(ln *net.UnixListener) {
	if ln != nil {
		_ = ln.Close()
	}
}
