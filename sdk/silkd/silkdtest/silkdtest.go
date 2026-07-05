// Package silkdtest fakes a silkd daemon for host-side tests: deterministic
// frame semantics over any listener, plus the CH/FC hybrid-vsock muxer
// handshake for tests that dial a UDS the way sandboxd does.
package silkdtest

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

// Serve accepts connections until l closes, speaking one RPC per connection
// and closing it after the terminal frame, like silkd. Semantics: info →
// InfoResp; exec echo → stdout of the args; exec cat → echoes stdin frames
// until stdin_close; exec false → exit 1; exec sleep → started, then blocks
// until the client disconnects; anything else → an error frame.
func Serve(l net.Listener) {
	acceptLoop(l, ServeConn)
}

// ServeConn speaks one RPC on an already-open connection and closes it —
// for tests that produce the conn themselves (e.g. after an HTTP hijack).
func ServeConn(conn net.Conn) {
	handle(conn, bufio.NewReader(conn))
}

// ListenHybrid serves the muxer handshake on a UDS: each connection must
// open with "CONNECT <port>", is answered "OK <port>", then speaks the
// Serve protocol. The returned closer stops the listener.
func ListenHybrid(sockPath string, port int) (io.Closer, error) {
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
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
				handle(c, r)
			}(conn)
		}
	}()
	return l, nil
}

// handle speaks one RPC; r must be the connection's only reader — a second
// buffered layer would swallow frames the first one read ahead.
func handle(conn net.Conn, r *bufio.Reader) {
	defer func() { _ = conn.Close() }()
	req, err := recvRequest(r)
	if err != nil {
		return
	}
	if !serveCommon(conn, r, req) {
		errFrame(conn, "unimplemented", "silkdtest: "+req.Op())
	}
}

// serveCommon handles the verbs shared by the stateless server and the Fake,
// reporting whether it recognized the request.
func serveCommon(conn net.Conn, r *bufio.Reader, req silkd.Request) bool {
	switch req := req.(type) {
	case *silkd.Info:
		send(conn, &silkd.InfoResp{Version: "silkdtest", Proto: silkd.ProtoVersion})
	case *silkd.Exec:
		serveExec(conn, r, req)
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

func serveExec(conn net.Conn, r *bufio.Reader, req *silkd.Exec) {
	if len(req.Argv) == 0 {
		errFrame(conn, "bad_request", "empty argv")
		return
	}
	send(conn, &silkd.Started{PID: 4242})
	switch req.Argv[0] {
	case "echo":
		send(conn, &silkd.Stdout{Data: []byte(strings.Join(req.Argv[1:], " ") + "\n")})
		send(conn, &silkd.Exit{Code: 0})
	case "cat":
		for {
			in, err := recvRequest(r)
			if err != nil {
				return
			}
			switch in := in.(type) {
			case *silkd.Stdin:
				send(conn, &silkd.Stdout{Data: in.Data})
			case *silkd.StdinClose:
				send(conn, &silkd.Exit{Code: 0})
				return
			default:
				errFrame(conn, "bad_request", "expected stdin")
				return
			}
		}
	case "false":
		send(conn, &silkd.Exit{Code: 1})
	case "sleep":
		_, _ = io.Copy(io.Discard, r) // hold the RPC open until disconnect
	default:
		errFrame(conn, "not_found", "silkdtest: no such command "+req.Argv[0])
	}
}

func recvRequest(r *bufio.Reader) (silkd.Request, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return silkd.DecodeRequest([]byte(line))
}

func send(conn net.Conn, resp silkd.Response) {
	frame, err := silkd.EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(frame, '\n'))
}
