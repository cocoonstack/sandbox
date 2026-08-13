package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestInstallCACertWritesCertAndUpdates(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	if err := New("cocoon", nil, nil, false, "").InstallCACert(t.Context(), path, []byte("CERT-PEM")); err != nil {
		t.Fatalf("InstallCACert: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.writePath != caCertGuestPath {
		t.Errorf("cert path = %q, want %q", fake.writePath, caCertGuestPath)
	}
	if fake.writeMode != 0o644 {
		t.Errorf("cert mode = %o, want 644", fake.writeMode)
	}
	if string(fake.writeData) != "CERT-PEM" {
		t.Errorf("cert data = %q, want CERT-PEM", fake.writeData)
	}
	if len(fake.execArgv) < 3 || fake.execArgv[0] != "sh" {
		t.Fatalf("exec argv = %v, want an sh -c invocation", fake.execArgv)
	}
	cmd := fake.execArgv[len(fake.execArgv)-1]
	if !strings.Contains(cmd, caCertGuestPath) || !strings.Contains(cmd, caBundlePath) {
		t.Errorf("exec appends %q, want it to cat %s into %s", cmd, caCertGuestPath, caBundlePath)
	}
	if fake.execEnv["PATH"] == "" {
		t.Errorf("exec env has no PATH; the guest command is run with an empty env")
	}
}

func TestInstallCACertNonzeroExitFails(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.execCode = 3
	err := New("cocoon", nil, nil, false, "").InstallCACert(t.Context(), path, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "exit code 3") {
		t.Errorf("got %v, want exit code 3 failure", err)
	}
}

func TestInstallCACertWriteErrorFrameFails(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.writeErr = "disk full"
	err := New("cocoon", nil, nil, false, "").InstallCACert(t.Context(), path, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("got %v, want fs_write error-frame failure", err)
	}
}

type fakeSilkd struct {
	mu         sync.Mutex
	writePath  string
	writeMode  uint32
	writeData  []byte
	execArgv   []string
	execCalls  [][]string
	execEnv    map[string]string
	execCode   int32
	execFailAt int
	writeErr   string
	execErr    string
	block      []wire.DirEntry
	serial     map[string][]byte
	readMisses map[string]int
	listErr    string
	readErr    map[string]string
	listCalls  int
	readCalls  []string
	stat       map[string]bool
	statMisses map[string]int
	statErr    map[string]string
	statCalls  []string
}

func serveFakeSilkd(t *testing.T, path string) *fakeSilkd {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	f := &fakeSilkd{
		serial:     make(map[string][]byte),
		readMisses: make(map[string]int),
		readErr:    make(map[string]string),
		stat:       make(map[string]bool),
		statMisses: make(map[string]int),
		statErr:    make(map[string]string),
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	return f
}

func (f *fakeSilkd) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	if _, err := r.ReadString('\n'); err != nil { // CONNECT line
		return
	}
	if _, err := io.WriteString(conn, "OK 2048\n"); err != nil {
		return
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	var req struct {
		Op   string            `json:"op"`
		Path string            `json:"path"`
		Mode uint32            `json:"mode"`
		Argv []string          `json:"argv"`
		Env  map[string]string `json:"env"`
	}
	if json.Unmarshal([]byte(line), &req) != nil {
		return
	}
	switch req.Op {
	case "fs_write":
		f.handleWrite(conn, r, req.Path, req.Mode)
	case "fs_list":
		f.handleList(conn, req.Path)
	case "fs_read":
		f.handleRead(conn, req.Path)
	case "fs_stat":
		f.handleStat(conn, req.Path)
	case "exec":
		f.handleExec(conn, req.Argv, req.Env)
	}
}

func (f *fakeSilkd) handleWrite(conn net.Conn, r *bufio.Reader, path string, mode uint32) {
	var data []byte
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		var frame struct {
			Op   string `json:"op"`
			Data []byte `json:"data"`
		}
		if json.Unmarshal([]byte(line), &frame) != nil {
			return
		}
		if frame.Op == "data_end" {
			break
		}
		data = append(data, frame.Data...)
	}
	f.mu.Lock()
	f.writePath, f.writeMode, f.writeData = path, mode, data
	reply := f.writeErr
	f.mu.Unlock()
	if reply != "" {
		_, _ = io.WriteString(conn, `{"type":"error","kind":"internal","message":"`+reply+`"}`+"\n")
		return
	}
	_, _ = io.WriteString(conn, `{"type":"done"}`+"\n")
}

func (f *fakeSilkd) handleExec(conn net.Conn, argv []string, env map[string]string) {
	f.mu.Lock()
	f.execArgv = argv
	f.execCalls = append(f.execCalls, argv)
	f.execEnv = env
	code, reply := f.execCode, f.execErr
	if f.execFailAt > 0 && len(f.execCalls) != f.execFailAt {
		code, reply = 0, ""
	}
	f.mu.Unlock()
	if reply != "" {
		_, _ = io.WriteString(conn, `{"type":"error","kind":"internal","message":"`+reply+`"}`+"\n")
		return
	}
	_, _ = io.WriteString(conn, `{"type":"started","pid":1}`+"\n")
	_, _ = fmt.Fprintf(conn, `{"type":"exit","code":%d}`+"\n", code)
}

func (f *fakeSilkd) handleList(conn net.Conn, path string) {
	f.mu.Lock()
	f.listCalls++
	entries, reply := append([]wire.DirEntry(nil), f.block...), f.listErr
	f.mu.Unlock()
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindInternal, Message: reply})
		return
	}
	if path != "/sys/block" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindNotFound, Message: path})
		return
	}
	writeFakeSilkdResponse(conn, &wire.Entries{Entries: entries})
	writeFakeSilkdResponse(conn, &wire.Done{})
}

func (f *fakeSilkd) handleRead(conn net.Conn, path string) {
	f.mu.Lock()
	f.readCalls = append(f.readCalls, path)
	data, ok := f.serial[path]
	reply := f.readErr[path]
	if f.readMisses[path] > 0 {
		f.readMisses[path]--
		ok = false
	}
	data = append([]byte(nil), data...)
	f.mu.Unlock()
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindInternal, Message: reply})
		return
	}
	if !ok {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindNotFound, Message: path})
		return
	}
	writeFakeSilkdResponse(conn, &wire.DataResp{Data: data})
	writeFakeSilkdResponse(conn, &wire.Done{})
}

func (f *fakeSilkd) handleStat(conn net.Conn, path string) {
	f.mu.Lock()
	f.statCalls = append(f.statCalls, path)
	ok := f.stat[path]
	reply := f.statErr[path]
	if f.statMisses[path] > 0 {
		f.statMisses[path]--
		ok = false
	}
	f.mu.Unlock()
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindInternal, Message: reply})
		return
	}
	if !ok {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindNotFound, Message: path})
		return
	}
	writeFakeSilkdResponse(conn, &wire.Stat{Info: wire.FileInfo{Kind: wire.FileKindFile}})
}

func writeFakeSilkdResponse(conn net.Conn, resp wire.Response) {
	buf, err := wire.EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(buf, '\n'))
}
