package outbound

import (
	"bufio"
	"context"
	"errors"
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
	bootNoticeMax     = 64
	bootAck           = "\n"
)

func (h *Host) armBoot(ctx context.Context, sb *types.Sandbox) {
	h.mu.Lock()
	ln, displaced := h.preboots[sb.VMName], h.boots[sb.ID]
	delete(h.preboots, sb.VMName)
	delete(h.boots, sb.ID)
	h.mu.Unlock()
	closeBoot(displaced)
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
		if ln, err = listenUnix(engine.BootSocketPath(sb.VsockSocket)); err != nil {
			log.WithFunc("outbound.armBoot").Warnf(ctx, "bind the boot door of %s, a reboot inside its guest goes unseen: %v", sb.ID, err)
			return
		}
	}
	h.mu.Lock()
	h.boots[sb.ID] = ln
	h.mu.Unlock()
	go h.serveBoot(context.WithoutCancel(ctx), sb, ln)
}

func (h *Host) serveBoot(ctx context.Context, sb *types.Sandbox, ln *net.UnixListener) {
	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.WithFunc("outbound.serveBoot").Warnf(ctx, "boot door of %s: %v", sb.ID, err)
			time.Sleep(acceptBackoff)
			continue
		}
		h.takeBootNotice(ctx, sb, conn)
	}
}

func (h *Host) takeBootNotice(ctx context.Context, sb *types.Sandbox, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(bootNoticeTimeout))
	line, err := bufio.NewReaderSize(conn, bootNoticeMax).ReadSlice('\n')
	var bootID uuid.UUID
	if err == nil {
		bootID, err = uuid.Parse(strings.TrimSuffix(string(line), "\n"))
	}
	if err != nil {
		log.WithFunc("outbound.takeBootNotice").Debugf(ctx, "drop a malformed boot notice of %s: %v", sb.ID, err)
		return
	}
	h.o.Rebooted(ctx, sb, bootID.String())
	_, _ = io.WriteString(conn, bootAck)
}

func closeBoot(ln *net.UnixListener) {
	if ln != nil {
		_ = ln.Close()
	}
}
