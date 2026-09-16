// Package silkdtest fakes a silkd daemon for host-side tests: deterministic frame semantics over any listener, plus the hybrid-vsock muxer handshake for tests that dial a UDS the way sandboxd does.
package silkdtest

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Serve accepts connections until l closes, serving RPCs back to back on each like silkd.
func Serve(l net.Listener) {
	acceptLoop(l, ServeConn)
}

// ServeConn serves RPCs on an open connection until the peer closes it.
func ServeConn(conn net.Conn) {
	serveConn(conn, bufio.NewReader(conn), wire.KeepAliveProto, serveStateless)
}

// ServeConnOnce serves one RPC and closes, like a daemon from before proto 2.
func ServeConnOnce(conn net.Conn) {
	serveConn(conn, bufio.NewReader(conn), 1, serveStateless)
}

// ListenHybrid serves the muxer handshake on a UDS: each connection must open with "CONNECT <port>", is answered "OK <port>", then speaks the Serve protocol.
func ListenHybrid(sockPath string, port int) (io.Closer, error) {
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	go acceptLoop(l, func(c net.Conn) {
		r := bufio.NewReader(c)
		line, err := r.ReadString('\n')
		if err != nil || strings.TrimSpace(line) != fmt.Sprintf("CONNECT %d", port) {
			_ = c.Close()
			return
		}
		if _, err := fmt.Fprintf(c, "OK %d\n", port); err != nil {
			_ = c.Close()
			return
		}
		serveConn(c, r, wire.KeepAliveProto, serveStateless)
	})
	return l, nil
}

// serveConn dispatches request frames until the peer closes, or after one when proto predates keep-alive; a served RPC's late input frames are dropped like silkd drops them.
func serveConn(conn net.Conn, r *bufio.Reader, proto uint32, serve func(net.Conn, *bufio.Reader, wire.Request, uint32)) {
	defer func() { _ = conn.Close() }()
	served := false
	for {
		req, err := recvRequest(r)
		if err != nil {
			return
		}
		if served && isInput(req) {
			continue
		}
		served = true
		serve(conn, r, req, proto)
		if proto < wire.KeepAliveProto {
			return
		}
	}
}

func serveStateless(conn net.Conn, r *bufio.Reader, req wire.Request, proto uint32) {
	if !serveCommon(conn, r, req, proto) {
		errFrame(conn, wire.KindUnimplemented, "silkdtest: "+req.Op())
	}
}

func serveCommon(conn net.Conn, r *bufio.Reader, req wire.Request, proto uint32) bool {
	switch req := req.(type) {
	case *wire.Info:
		send(conn, &wire.InfoResp{Version: "silkdtest", Proto: proto})
	case *wire.Exec:
		serveExec(conn, r, req)
	case *wire.PortForward:
		portEcho(conn, r, req.Port)
	default:
		return false
	}
	return true
}

func acceptLoop(l net.Listener, serve func(net.Conn)) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go serve(conn)
	}
}

func serveExec(conn net.Conn, r *bufio.Reader, req *wire.Exec) {
	if len(req.Argv) == 0 {
		errFrame(conn, wire.KindBadRequest, "empty argv")
		return
	}
	send(conn, &wire.Started{PID: 4242})
	switch req.Argv[0] {
	case "echo":
		send(conn, &wire.Stdout{Data: []byte(strings.Join(req.Argv[1:], " ") + "\n")})
		send(conn, &wire.Exit{Code: 0})
	case "cat":
		for {
			in, err := recvRequest(r)
			if err != nil {
				return
			}
			switch in := in.(type) {
			case *wire.Stdin:
				send(conn, &wire.Stdout{Data: in.Data})
			case *wire.StdinClose:
				send(conn, &wire.Exit{Code: 0})
				return
			default:
				errFrame(conn, wire.KindBadRequest, "expected stdin")
				return
			}
		}
	case "false":
		send(conn, &wire.Exit{Code: 1})
	case "sleep":
		_, _ = io.Copy(io.Discard, r) // hold the RPC open until disconnect
	default:
		errFrame(conn, wire.KindNotFound, "silkdtest: no such command "+req.Argv[0])
	}
}

func isInput(req wire.Request) bool {
	switch req.(type) {
	case *wire.Stdin, *wire.StdinClose, *wire.Data, *wire.DataEnd:
		return true
	}
	return false
}

func recvRequest(r *bufio.Reader) (wire.Request, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return wire.DecodeRequest([]byte(line))
}

func send(conn net.Conn, resp wire.Response) {
	frame, err := wire.EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(frame, '\n'))
}
