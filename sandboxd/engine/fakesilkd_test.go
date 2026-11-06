package engine

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json/v2"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

type fakeEdit func(*fakeSilkd)

type fakeSilkd struct {
	mu sync.Mutex

	connects []string

	writePath string
	writeMode uint32
	writeData []byte
	writeErr  string
	writeKind string

	execArgv     []string
	execCalls    [][]string
	execDetach   []bool
	execEnv      map[string]string
	execCode     int32
	execFailAt   int
	execFailFrom int
	execHook     func([]string)
	execErr      string
	execDrop     bool
	notReady     int
	realpath     map[string]string
	noRealpath   bool

	block     []wire.DirEntry
	listErr   string
	listCalls int

	serial     map[string][]byte
	readMisses map[string]int
	blankReads map[string]int
	readErr    map[string]string
	readCalls  []string

	stat       map[string]bool
	statMisses map[string]int
	statErr    map[string]string
	statCalls  []string

	tunOps []string
	tunErr string
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
		blankReads: make(map[string]int),
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

func (f *fakeSilkd) set(fn fakeEdit) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeSilkd) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	connect, err := r.ReadString('\n')
	if err != nil {
		return
	}
	f.mu.Lock()
	f.connects = append(f.connects, strings.TrimSpace(connect))
	f.mu.Unlock()
	if _, err = io.WriteString(conn, "OK 2048\n"); err != nil {
		return
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	var req struct {
		Op     string            `json:"op"`
		Path   string            `json:"path"`
		Mode   uint32            `json:"mode"`
		Argv   []string          `json:"argv"`
		Env    map[string]string `json:"env"`
		Detach bool              `json:"detach"`
	}
	if json.Unmarshal([]byte(line), &req) != nil {
		return
	}
	if req.Op == "exec" {
		f.mu.Lock()
		f.execDetach = append(f.execDetach, req.Detach)
		f.mu.Unlock()
	}
	switch req.Op {
	case "info":
		writeFakeSilkdResponse(conn, &wire.InfoResp{Version: "test"})
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
	case "tun_up", "tun_down":
		f.handleTun(conn, req.Op)
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
	reply, kind := f.writeErr, cmp.Or(f.writeKind, wire.KindInternal)
	f.mu.Unlock()
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: kind, Message: reply})
		return
	}
	writeFakeSilkdResponse(conn, &wire.Done{})
}

func (f *fakeSilkd) handleExec(conn net.Conn, argv []string, env map[string]string) {
	f.mu.Lock()
	f.execArgv = argv
	f.execCalls = append(f.execCalls, argv)
	f.execEnv = env
	code, reply, disconnect := f.execCode, f.execErr, f.execDrop
	if argv[0] == "realpath" {
		code, reply, disconnect = 0, "", false
		if f.noRealpath {
			code = 1
		}
	}
	if f.execFailAt > 0 && len(f.execCalls) != f.execFailAt || len(f.execCalls) < f.execFailFrom {
		code, reply = 0, ""
	}
	stderr := ""
	if argv[0] == "mount" && f.notReady > 0 {
		f.notReady--
		code, stderr = 32, "mount: /data: /dev/vdc is not a valid block device."
	}
	if code == 0 && reply == "" && f.execHook != nil {
		f.execHook(argv)
	}
	resolved := f.realpath[argv[len(argv)-1]]
	f.mu.Unlock()
	if disconnect {
		return
	}
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindInternal, Message: reply})
		return
	}
	writeFakeSilkdResponse(conn, &wire.Started{PID: 1})
	if stderr != "" {
		writeFakeSilkdResponse(conn, &wire.Stderr{Data: []byte(stderr)})
	}
	if argv[0] == "realpath" && code == 0 {
		writeFakeSilkdResponse(conn, &wire.Stdout{Data: []byte(cmp.Or(resolved, argv[len(argv)-1]) + "\n")})
	}
	writeFakeSilkdResponse(conn, &wire.Exit{Code: code})
}

func (f *fakeSilkd) handleList(conn net.Conn, path string) {
	f.mu.Lock()
	f.listCalls++
	entries, reply := slices.Clone(f.block), f.listErr
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
	if f.blankReads[path] > 0 {
		f.blankReads[path]--
		data = []byte("\n")
	}
	data = bytes.Clone(data)
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

func (f *fakeSilkd) handleTun(conn net.Conn, op string) {
	f.mu.Lock()
	f.tunOps = append(f.tunOps, op)
	reply := f.tunErr
	f.mu.Unlock()
	if reply != "" {
		writeFakeSilkdResponse(conn, &wire.ErrorResp{Kind: wire.KindInternal, Message: reply})
		return
	}
	writeFakeSilkdResponse(conn, &wire.Done{})
}

func writeFakeSilkdResponse(conn net.Conn, resp wire.Response) {
	buf, err := wire.EncodeResponse(resp)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(buf, '\n'))
}
