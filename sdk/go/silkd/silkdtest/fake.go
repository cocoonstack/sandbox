package silkdtest

import (
	"archive/tar"
	"bufio"
	"bytes"
	"io"
	"io/fs"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Fake is a stateful silkd fake backing the fs verbs with a real directory
// and tracking sessions, so an SDK write-then-read round-trips through it.
// exec/info reuse the stateless handlers. It exists for host-side unit tests;
// the authoritative fs/session behavior is silkd's own Rust test suite.
// readChunk mirrors silkd's BULK_CHUNK so downloads exercise real framing.
const readChunk = 256 * 1024

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
	case *wire.FsWrite:
		f.fsWrite(conn, r, req)
	case *wire.FsRead:
		f.fsRead(conn, req.Path)
	case *wire.FsList:
		f.fsList(conn, req.Path)
	case *wire.FsStat:
		f.fsStat(conn, req.Path)
	case *wire.FsMkdir:
		f.done(conn, os.MkdirAll(f.abs(req.Path), 0o750))
	case *wire.FsRm:
		f.fsRm(conn, req)
	case *wire.FsRename:
		f.done(conn, os.Rename(f.abs(req.From), f.abs(req.To)))
	case *wire.SessionCreate:
		f.sessionCreate(conn, req)
	case *wire.SessionList:
		f.sessionList(conn)
	case *wire.SessionRm:
		f.sessionRm(conn, req.ID)
	case *wire.FsPush:
		f.fsPush(conn, r, req.Dest)
	case *wire.FsPull:
		f.fsPull(conn, req.Path)
	case *wire.FsFind:
		f.fsFind(conn, req)
	case *wire.FsReplace:
		f.fsReplace(conn, req)
	case *wire.FsWatch:
		f.fsWatch(conn, r, req)
	case *wire.GitPush, *wire.GitPull:
		send(conn, wire.Done{})
	case *wire.GitBranch:
		f.gitBranch(conn, req)
	case *wire.PtyOpen:
		ptyEcho(conn, r)
	case *wire.PtyResize:
		send(conn, wire.Done{})
	case *wire.PortForward:
		portEcho(conn, r, req.Port)
	default:
		if !serveCommon(conn, r, req) {
			errFrame(conn, wire.KindUnimplemented, "silkdtest: "+req.Op())
		}
	}
}

func (f *Fake) fsWrite(conn net.Conn, r *bufio.Reader, req *wire.FsWrite) {
	data, err := drainUpload(r)
	if err != nil {
		errFrame(conn, wire.KindInternal, err.Error())
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
		errFrame(conn, wire.KindNotFound, err.Error())
		return
	}
	for len(data) > 0 {
		n := min(readChunk, len(data))
		send(conn, &wire.DataResp{Data: data[:n]})
		data = data[n:]
	}
	send(conn, wire.Done{})
}

func (f *Fake) fsList(conn net.Conn, path string) {
	ents, err := os.ReadDir(f.abs(path))
	if err != nil {
		errFrame(conn, wire.KindNotFound, err.Error())
		return
	}
	var entries []wire.DirEntry
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
		entries = append(entries, wire.DirEntry{Name: e.Name(), Kind: kind, Size: size})
	}
	send(conn, &wire.Entries{Entries: entries})
	send(conn, wire.Done{})
}

func (f *Fake) fsStat(conn net.Conn, path string) {
	info, err := os.Stat(f.abs(path))
	if err != nil {
		errFrame(conn, wire.KindNotFound, err.Error())
		return
	}
	kind := "file"
	if info.IsDir() {
		kind = "dir"
	}
	send(conn, &wire.Stat{Info: wire.FileInfo{
		Kind: kind,
		Size: uint64(info.Size()),
		Mode: uint32(info.Mode().Perm()),
	}})
}

func (f *Fake) fsRm(conn net.Conn, req *wire.FsRm) {
	if req.Recursive {
		f.done(conn, os.RemoveAll(f.abs(req.Path)))
	} else {
		f.done(conn, os.Remove(f.abs(req.Path)))
	}
}

// fsPush extracts the uploaded tar under dest; fsPull streams a path back
// as a tar with the basename as the entry root — matching silkd's contract
// closely enough for SDK round-trip tests.
func (f *Fake) fsPush(conn net.Conn, r *bufio.Reader, dest string) {
	data, err := drainUpload(r)
	if err != nil {
		errFrame(conn, wire.KindInternal, err.Error())
		return
	}
	root := f.abs(dest)
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			errFrame(conn, wire.KindInternal, err.Error())
			return
		}
		target := filepath.Join(root, filepath.Clean("/"+hdr.Name))
		switch hdr.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o750)
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(target), 0o750); err == nil {
				var body []byte
				if body, err = io.ReadAll(tr); err == nil {
					err = os.WriteFile(target, body, 0o600)
				}
			}
		}
		if err != nil {
			errFrame(conn, wire.KindInternal, err.Error())
			return
		}
	}
	f.done(conn, nil)
}

func (f *Fake) fsPull(conn net.Conn, path string) {
	root := f.abs(path)
	base := filepath.Base(path)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		name := filepath.Join(base, rel)
		if d.IsDir() {
			return tw.WriteHeader(&tar.Header{Name: name + "/", Typeflag: tar.TypeDir, Mode: 0o750})
		}
		body, err := os.ReadFile(p) //nolint:gosec // test fake, paths under the fake root
		if err != nil {
			return err
		}
		if hdrErr := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body))}); hdrErr != nil {
			return hdrErr
		}
		_, err = tw.Write(body)
		return err
	})
	if err != nil {
		errFrame(conn, wire.KindNotFound, err.Error())
		return
	}
	_ = tw.Close()
	send(conn, wire.DataResp{Data: buf.Bytes()})
	send(conn, wire.Done{})
}

func (f *Fake) sessionCreate(conn net.Conn, req *wire.SessionCreate) {
	f.mu.Lock()
	id := req.ID
	if id == "" {
		f.seq++
		id = "sess-" + strconv.Itoa(f.seq)
	}
	f.sessions[id] = true
	f.mu.Unlock()
	send(conn, &wire.SessionCreated{ID: id})
}

func (f *Fake) sessionList(conn net.Conn) {
	f.mu.Lock()
	ids := slices.Collect(maps.Keys(f.sessions))
	f.mu.Unlock()
	send(conn, &wire.Sessions{Sessions: ids})
}

func (f *Fake) sessionRm(conn net.Conn, id string) {
	f.mu.Lock()
	_, ok := f.sessions[id]
	delete(f.sessions, id)
	f.mu.Unlock()
	if !ok {
		errFrame(conn, wire.KindNotFound, "no such session")
		return
	}
	send(conn, wire.Done{})
}

func (f *Fake) fsFind(conn net.Conn, req *wire.FsFind) {
	re, err := regexp.Compile(req.Pattern)
	if err != nil {
		errFrame(conn, wire.KindBadRequest, err.Error())
		return
	}
	var nameRe *regexp.Regexp
	if req.Glob != "" {
		nameRe = globRegexp(req.Glob)
	}
	walkErr := filepath.WalkDir(f.abs(req.Path), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if nameRe != nil && !nameRe.MatchString(d.Name()) {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		for i, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
			if re.MatchString(line) {
				send(conn, &wire.Match{
					File:    strings.TrimPrefix(path, f.Root),
					Line:    uint64(i) + 1,
					Content: line,
				})
			}
		}
		return nil
	})
	if walkErr != nil {
		errFrame(conn, wire.KindNotFound, walkErr.Error())
		return
	}
	send(conn, wire.Done{})
}

func (f *Fake) fsReplace(conn net.Conn, req *wire.FsReplace) {
	re, err := regexp.Compile(req.Pattern)
	if err != nil {
		errFrame(conn, wire.KindBadRequest, err.Error())
		return
	}
	for _, file := range req.Files {
		body, err := os.ReadFile(f.abs(file))
		if err != nil {
			errFrame(conn, wire.KindNotFound, err.Error())
			return
		}
		count := len(re.FindAll(body, -1))
		if count > 0 {
			if err := os.WriteFile(f.abs(file), re.ReplaceAll(body, []byte(req.Replacement)), 0o600); err != nil {
				errFrame(conn, wire.KindInternal, err.Error())
				return
			}
		}
		send(conn, &wire.Replaced{File: file, Replacements: uint64(count)})
	}
	send(conn, wire.Done{})
}

// fsWatch fakes a watch: ready, one synthetic created event, then it holds
// the stream open until the client disconnects — the connection-bound
// contract.
func (f *Fake) fsWatch(conn net.Conn, r *bufio.Reader, req *wire.FsWatch) {
	if _, err := os.Stat(f.abs(req.Path)); err != nil {
		errFrame(conn, wire.KindNotFound, err.Error())
		return
	}
	send(conn, wire.Ready{})
	send(conn, &wire.Event{Kind: "created", Path: req.Path + "/silkdtest-event"})
	_, _ = io.Copy(io.Discard, r)
}

func (f *Fake) gitBranch(conn net.Conn, req *wire.GitBranch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Action {
	case wire.BranchList:
		send(conn, &wire.GitBranches{Current: f.current, Branches: slices.Clone(f.branches)})
	case wire.BranchCreate:
		if !slices.Contains(f.branches, req.Name) {
			f.branches = append(f.branches, req.Name)
		}
		send(conn, wire.Done{})
	case wire.BranchDelete:
		f.branches = slices.DeleteFunc(f.branches, func(b string) bool { return b == req.Name })
		send(conn, wire.Done{})
	case wire.BranchCheckout:
		if !slices.Contains(f.branches, req.Name) {
			errFrame(conn, wire.KindInternal, "no such branch "+req.Name)
			return
		}
		f.current = req.Name
		send(conn, wire.Done{})
	default:
		errFrame(conn, wire.KindBadRequest, "unknown branch action "+req.Action)
	}
}

func (f *Fake) done(conn net.Conn, err error) {
	if err != nil {
		errFrame(conn, wire.KindInternal, err.Error())
		return
	}
	send(conn, wire.Done{})
}

// abs keeps a request path inside Root; an absolute request path is rebased.
func (f *Fake) abs(path string) string {
	return filepath.Join(f.Root, filepath.Clean("/"+path))
}

// portEcho fakes port_forward: port 1 is refused (not_found, mirroring a
// dead port), anything else answers Ready and echoes Data frames until
// DataEnd (→ Done, like the server closing after EOF) or disconnect.
func portEcho(conn net.Conn, r *bufio.Reader, port uint16) {
	if port == 1 {
		errFrame(conn, wire.KindNotFound, "connect refused")
		return
	}
	send(conn, wire.Ready{})
	for {
		req, err := recvRequest(r)
		if err != nil {
			return
		}
		switch req := req.(type) {
		case *wire.Data:
			send(conn, &wire.DataResp{Data: req.Data})
		case *wire.DataEnd:
			send(conn, wire.Done{})
			return
		default:
			errFrame(conn, wire.KindBadRequest, "expected data or data_end")
			return
		}
	}
}

// ptyEcho fakes a pty: Started, then echo each stdin chunk as stdout until a
// chunk contains "exit" (→ Exit 0) or the client disconnects.
func ptyEcho(conn net.Conn, r *bufio.Reader) {
	send(conn, &wire.Started{PID: 9999})
	for {
		req, err := recvRequest(r)
		if err != nil {
			return
		}
		if in, ok := req.(*wire.Stdin); ok {
			if strings.Contains(string(in.Data), "exit") {
				send(conn, &wire.Exit{Code: 0})
				return
			}
			send(conn, &wire.Stdout{Data: in.Data})
		}
	}
}

// globRegexp mirrors silkd's glob translation exactly — `*`/`?` wildcards,
// everything else literal (never `filepath.Match`, whose char classes silkd
// does not have). The quoted pattern always compiles.
func globRegexp(glob string) *regexp.Regexp {
	pat := regexp.QuoteMeta(glob)
	pat = strings.ReplaceAll(pat, `\*`, ".*")
	pat = strings.ReplaceAll(pat, `\?`, ".")
	return regexp.MustCompile("^" + pat + "$")
}

func errFrame(conn net.Conn, kind, msg string) {
	send(conn, &wire.ErrorResp{Kind: kind, Message: msg})
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
		case *wire.Data:
			out = append(out, req.Data...)
		case *wire.DataEnd:
			return out, nil
		default:
			return nil, os.ErrInvalid
		}
	}
}
