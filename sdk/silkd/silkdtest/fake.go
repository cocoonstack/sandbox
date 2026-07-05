package silkdtest

import (
	"bufio"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cocoonstack/sandbox/sdk/silkd"
)

// Fake is a stateful silkd fake backing the fs verbs with a real directory
// and tracking sessions, so an SDK write-then-read round-trips through it.
// exec/info reuse the stateless handlers. It exists for host-side unit tests;
// the authoritative fs/session behavior is silkd's own Rust test suite.
type Fake struct {
	Root string

	mu       sync.Mutex
	sessions map[string]bool
	seq      int
	branches []string
	current  string
}

// NewFake returns a Fake serving files under root.
func NewFake(root string) *Fake {
	return &Fake{Root: root, sessions: map[string]bool{}, branches: []string{"main"}, current: "main"}
}

// Serve accepts connections until l closes, one RPC per connection.
func (f *Fake) Serve(l net.Listener) {
	acceptLoop(l, f.ServeConn)
}

// ServeConn speaks one RPC on an already-open connection (e.g. after an HTTP
// hijack) and closes it.
func (f *Fake) ServeConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	req, err := recvRequest(r)
	if err != nil {
		return
	}
	switch req := req.(type) {
	case *silkd.FsWrite:
		f.fsWrite(conn, r, req)
	case *silkd.FsRead:
		f.fsRead(conn, req.Path)
	case *silkd.FsList:
		f.fsList(conn, req.Path)
	case *silkd.FsStat:
		f.fsStat(conn, req.Path)
	case *silkd.FsMkdir:
		f.done(conn, os.MkdirAll(f.abs(req.Path), 0o750))
	case *silkd.FsRm:
		f.fsRm(conn, req)
	case *silkd.FsRename:
		f.done(conn, os.Rename(f.abs(req.From), f.abs(req.To)))
	case *silkd.SessionCreate:
		f.sessionCreate(conn, req)
	case *silkd.SessionList:
		f.sessionList(conn)
	case *silkd.SessionRm:
		f.sessionRm(conn, req.ID)
	case *silkd.FsFind:
		f.fsFind(conn, req)
	case *silkd.FsReplace:
		f.fsReplace(conn, req)
	case *silkd.FsWatch:
		f.fsWatch(conn, r, req)
	case *silkd.GitPush, *silkd.GitPull:
		send(conn, silkd.Done{})
	case *silkd.GitBranch:
		f.gitBranch(conn, req)
	case *silkd.PtyOpen:
		ptyEcho(conn, r)
	case *silkd.PtyResize:
		send(conn, silkd.Done{})
	default:
		if !serveCommon(conn, r, req) {
			errFrame(conn, silkd.KindUnimplemented, "silkdtest: "+req.Op())
		}
	}
}

// ptyEcho fakes a pty: Started, then echo each stdin chunk as stdout until a
// chunk contains "exit" (→ Exit 0) or the client disconnects.
func ptyEcho(conn net.Conn, r *bufio.Reader) {
	send(conn, &silkd.Started{PID: 9999})
	for {
		req, err := recvRequest(r)
		if err != nil {
			return
		}
		if in, ok := req.(*silkd.Stdin); ok {
			if strings.Contains(string(in.Data), "exit") {
				send(conn, &silkd.Exit{Code: 0})
				return
			}
			send(conn, &silkd.Stdout{Data: in.Data})
		}
	}
}

func (f *Fake) fsWrite(conn net.Conn, r *bufio.Reader, req *silkd.FsWrite) {
	data, err := drainUpload(r)
	if err != nil {
		errFrame(conn, silkd.KindInternal, err.Error())
		return
	}
	mode := os.FileMode(0o644)
	if req.Mode != nil {
		mode = os.FileMode(*req.Mode)
	}
	f.done(conn, os.WriteFile(f.abs(req.Path), data, mode))
}

func (f *Fake) fsRead(conn net.Conn, path string) {
	data, err := os.ReadFile(f.abs(path))
	if err != nil {
		errFrame(conn, silkd.KindNotFound, err.Error())
		return
	}
	send(conn, &silkd.DataResp{Data: data})
	send(conn, silkd.Done{})
}

func (f *Fake) fsList(conn net.Conn, path string) {
	ents, err := os.ReadDir(f.abs(path))
	if err != nil {
		errFrame(conn, silkd.KindNotFound, err.Error())
		return
	}
	var entries []silkd.DirEntry
	for _, e := range ents {
		info, _ := e.Info()
		kind := "file"
		if e.IsDir() {
			kind = "dir"
		}
		var size uint64
		if info != nil {
			size = uint64(info.Size())
		}
		entries = append(entries, silkd.DirEntry{Name: e.Name(), Kind: kind, Size: size})
	}
	send(conn, &silkd.Entries{Entries: entries})
	send(conn, silkd.Done{})
}

func (f *Fake) fsStat(conn net.Conn, path string) {
	info, err := os.Stat(f.abs(path))
	if err != nil {
		errFrame(conn, silkd.KindNotFound, err.Error())
		return
	}
	kind := "file"
	if info.IsDir() {
		kind = "dir"
	}
	send(conn, &silkd.Stat{Info: silkd.FileInfo{
		Kind: kind,
		Size: uint64(info.Size()),
		Mode: uint32(info.Mode().Perm()),
	}})
}

func (f *Fake) fsRm(conn net.Conn, req *silkd.FsRm) {
	if req.Recursive {
		f.done(conn, os.RemoveAll(f.abs(req.Path)))
	} else {
		f.done(conn, os.Remove(f.abs(req.Path)))
	}
}

func (f *Fake) sessionCreate(conn net.Conn, req *silkd.SessionCreate) {
	f.mu.Lock()
	id := req.ID
	if id == "" {
		f.seq++
		id = "sess-" + strconv.Itoa(f.seq)
	}
	f.sessions[id] = true
	f.mu.Unlock()
	send(conn, &silkd.SessionCreated{ID: id})
}

func (f *Fake) sessionList(conn net.Conn) {
	f.mu.Lock()
	ids := make([]string, 0, len(f.sessions))
	for id := range f.sessions {
		ids = append(ids, id)
	}
	f.mu.Unlock()
	send(conn, &silkd.Sessions{Sessions: ids})
}

func (f *Fake) sessionRm(conn net.Conn, id string) {
	f.mu.Lock()
	_, ok := f.sessions[id]
	delete(f.sessions, id)
	f.mu.Unlock()
	if !ok {
		errFrame(conn, silkd.KindNotFound, "no such session")
		return
	}
	send(conn, silkd.Done{})
}

func (f *Fake) fsFind(conn net.Conn, req *silkd.FsFind) {
	re, err := regexp.Compile(req.Pattern)
	if err != nil {
		errFrame(conn, silkd.KindBadRequest, err.Error())
		return
	}
	walkErr := filepath.WalkDir(f.abs(req.Path), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if req.Glob != "" && !strings.Contains(d.Name(), req.Glob) {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		for i, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
			if re.MatchString(line) {
				send(conn, &silkd.Match{
					File:    strings.TrimPrefix(path, f.Root),
					Line:    uint64(i) + 1,
					Content: line,
				})
			}
		}
		return nil
	})
	if walkErr != nil {
		errFrame(conn, silkd.KindNotFound, walkErr.Error())
		return
	}
	send(conn, silkd.Done{})
}

func (f *Fake) fsReplace(conn net.Conn, req *silkd.FsReplace) {
	re, err := regexp.Compile(req.Pattern)
	if err != nil {
		errFrame(conn, silkd.KindBadRequest, err.Error())
		return
	}
	for _, file := range req.Files {
		body, err := os.ReadFile(f.abs(file))
		if err != nil {
			errFrame(conn, silkd.KindNotFound, err.Error())
			return
		}
		count := len(re.FindAll(body, -1))
		if count > 0 {
			if err := os.WriteFile(f.abs(file), re.ReplaceAll(body, []byte(req.Replacement)), 0o600); err != nil {
				errFrame(conn, silkd.KindInternal, err.Error())
				return
			}
		}
		send(conn, &silkd.Replaced{File: file, Replacements: uint64(count)})
	}
	send(conn, silkd.Done{})
}

// fsWatch fakes a watch: one synthetic created event, then it holds the
// stream open until the client disconnects — the connection-bound contract.
func (f *Fake) fsWatch(conn net.Conn, r *bufio.Reader, req *silkd.FsWatch) {
	if _, err := os.Stat(f.abs(req.Path)); err != nil {
		errFrame(conn, silkd.KindNotFound, err.Error())
		return
	}
	send(conn, &silkd.Event{Kind: "created", Path: req.Path + "/silkdtest-event"})
	_, _ = io.Copy(io.Discard, r)
}

func (f *Fake) gitBranch(conn net.Conn, req *silkd.GitBranch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Action {
	case silkd.BranchList:
		send(conn, &silkd.GitBranches{Current: f.current, Branches: slices.Clone(f.branches)})
	case silkd.BranchCreate:
		if !slices.Contains(f.branches, req.Name) {
			f.branches = append(f.branches, req.Name)
		}
		send(conn, silkd.Done{})
	case silkd.BranchDelete:
		f.branches = slices.DeleteFunc(f.branches, func(b string) bool { return b == req.Name })
		send(conn, silkd.Done{})
	case silkd.BranchCheckout:
		if !slices.Contains(f.branches, req.Name) {
			errFrame(conn, silkd.KindInternal, "no such branch "+req.Name)
			return
		}
		f.current = req.Name
		send(conn, silkd.Done{})
	default:
		errFrame(conn, silkd.KindBadRequest, "unknown branch action "+req.Action)
	}
}

func (f *Fake) done(conn net.Conn, err error) {
	if err != nil {
		errFrame(conn, silkd.KindInternal, err.Error())
		return
	}
	send(conn, silkd.Done{})
}

// abs keeps a request path inside Root; an absolute request path is rebased.
func (f *Fake) abs(path string) string {
	return filepath.Join(f.Root, filepath.Clean("/"+path))
}

func errFrame(conn net.Conn, kind, msg string) {
	send(conn, &silkd.ErrorResp{Kind: kind, Message: msg})
}

// drainUpload reads Data frames until DataEnd, concatenating their payloads.
func drainUpload(r *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		req, err := recvRequest(r)
		if err != nil {
			return nil, err
		}
		switch req := req.(type) {
		case *silkd.Data:
			out = append(out, req.Data...)
		case *silkd.DataEnd:
			return out, nil
		default:
			return nil, os.ErrInvalid
		}
	}
}
