// Package silkd is the host-side binding of the silkd wire protocol:
// newline-delimited JSON frames over one connection per RPC, requests tagged
// by "op", responses by "type", binary payloads base64 in data fields. The
// authoritative contract is the shared corpus in protocol/fixtures/v1 —
// silkd's Rust tests and this package's tests round-trip the same files.
package silkd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	// ProtoVersion is stamped into every request as "v"; silkd ignores
	// unknown fields, which is the forward-compatibility story.
	ProtoVersion = 1
	// MaxFrame mirrors silkd's frame cap.
	MaxFrame = 8 << 20

	// GitBranch.Action values (silkd's GitBranchOp).
	BranchList     = "list"
	BranchCreate   = "create"
	BranchDelete   = "delete"
	BranchCheckout = "checkout"

	// ErrorResp.Kind values (silkd's ErrorKind).
	KindBadRequest    = "bad_request"
	KindNotFound      = "not_found"
	KindUnimplemented = "unimplemented"
	KindInternal      = "internal"

	// Event.Kind values (silkd's EventKind).
	EventCreated  = "created"
	EventModified = "modified"
	EventDeleted  = "deleted"
	EventRenamed  = "renamed"

	// DirEntry.Kind and FileInfo.Kind values (silkd's FileKind).
	FileKindFile    = "file"
	FileKindDir     = "dir"
	FileKindSymlink = "symlink"
	FileKindOther   = "other"
)

var (
	requestHead = `{"v":` + strconv.Itoa(ProtoVersion) + `,"op":"`

	// requestDecoders maps each op tag to a decoder; table dispatch keeps this
	// (and the verb set) flat instead of a switch that grows past the complexity
	// budget as verbs are added.
	requestDecoders = map[string]func([]byte) (Request, error){
		"exec":           decodeReq[Exec],
		"info":           decodeReq[Info],
		"ps":             decodeReq[Ps],
		"kill":           decodeReq[Kill],
		"attach":         decodeReq[Attach],
		"logs":           decodeReq[Logs],
		"session_create": decodeReq[SessionCreate],
		"session_list":   decodeReq[SessionList],
		"session_rm":     decodeReq[SessionRm],
		"stdin":          decodeReq[Stdin],
		"stdin_close":    decodeReq[StdinClose],
		"fs_write":       decodeReq[FsWrite],
		"fs_read":        decodeReq[FsRead],
		"fs_list":        decodeReq[FsList],
		"fs_stat":        decodeReq[FsStat],
		"fs_mkdir":       decodeReq[FsMkdir],
		"fs_rm":          decodeReq[FsRm],
		"fs_rename":      decodeReq[FsRename],
		"fs_push":        decodeReq[FsPush],
		"fs_pull":        decodeReq[FsPull],
		"fs_find":        decodeReq[FsFind],
		"fs_replace":     decodeReq[FsReplace],
		"fs_watch":       decodeReq[FsWatch],
		"pty_open":       decodeReq[PtyOpen],
		"pty_resize":     decodeReq[PtyResize],
		"port_forward":   decodeReq[PortForward],
		"git_clone":      decodeReq[GitClone],
		"git_status":     decodeReq[GitStatus],
		"git_add":        decodeReq[GitAdd],
		"git_commit":     decodeReq[GitCommit],
		"git_push":       decodeReq[GitPush],
		"git_pull":       decodeReq[GitPull],
		"git_branch":     decodeReq[GitBranch],
		"lsp_start":      decodeReq[LspStart],
		"lsp_request":    decodeReq[LspRequest],
		"lsp_stop":       decodeReq[LspStop],
		"data":           decodeReq[Data],
		"data_end":       decodeReq[DataEnd],
	}

	// responseDecoders maps each type tag to a decoder. Two-stage dispatch is
	// required because the same key differs in shape across variants (info's
	// procs is a count, ps's procs is a list).
	responseDecoders = map[string]func([]byte) (Response, error){
		"started":           decodeResp[Started],
		"stdout":            fastBulk(decodeResp[Stdout], func(d []byte) Response { return &Stdout{Data: d} }),
		"stderr":            fastBulk(decodeResp[Stderr], func(d []byte) Response { return &Stderr{Data: d} }),
		"exit":              decodeResp[Exit],
		"done":              decodeResp[Done],
		"ready":             decodeResp[Ready],
		"error":             decodeResp[ErrorResp],
		"info":              decodeResp[InfoResp],
		"procs":             decodeResp[Procs],
		"data":              fastBulk(decodeResp[DataResp], func(d []byte) Response { return &DataResp{Data: d} }),
		"entries":           decodeResp[Entries],
		"stat":              decodeResp[Stat],
		"session_created":   decodeResp[SessionCreated],
		"lsp_started":       decodeResp[LspStarted],
		"sessions":          decodeResp[Sessions],
		"match":             decodeResp[Match],
		"replaced":          decodeResp[Replaced],
		"event":             decodeResp[Event],
		"git_status_result": decodeResp[GitStatusResult],
		"git_commit_result": decodeResp[GitCommitResult],
		"git_branches":      decodeResp[GitBranches],
	}
)

// Request is a client→server frame; Op is its wire tag.
type Request interface{ Op() string }

// Response is a server→client frame; RespType is its wire tag.
type Response interface{ RespType() string }

// B64 carries request payload bytes. It exists because silkd's deserializer
// requires a base64 string and rejects null — which is exactly what
// encoding/json emits for a nil []byte. Decoding needs no counterpart:
// []byte-kinded types already base64-decode by default.
type B64 []byte

func (b B64) MarshalJSON() ([]byte, error) {
	if b == nil {
		b = B64{}
	}
	return json.Marshal([]byte(b))
}

// Exec starts a process; with Session set it runs inside that persistent
// shell instead. Detach is emitted even when false — it is part of the
// fixture corpus shape.
type Exec struct {
	Argv    []string          `json:"argv"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	User    string            `json:"user,omitempty"`
	Detach  bool              `json:"detach"`
	Session string            `json:"session,omitempty"`
}

func (Exec) Op() string { return "exec" }

// Info asks for the daemon's identity and counters — the readiness probe.
type Info struct{}

func (Info) Op() string { return "info" } //nolint:goconst // wire tag shared with the response type by design

// Ps lists tracked processes.
type Ps struct{}

func (Ps) Op() string { return "ps" }

// Kill signals a process; nil Signal means SIGKILL.
type Kill struct {
	PID    uint32 `json:"pid"`
	Signal *int32 `json:"signal,omitempty"`
}

func (Kill) Op() string { return "kill" }

// Attach streams a running process's buffered and live output.
type Attach struct {
	PID uint32 `json:"pid"`
}

func (Attach) Op() string { return "attach" }

// Logs returns a process's ring-buffered output.
type Logs struct {
	PID uint32 `json:"pid"`
}

func (Logs) Op() string { return "logs" }

// SessionCreate opens a persistent shell; empty ID lets silkd name it.
type SessionCreate struct {
	ID  string            `json:"id,omitempty"`
	Cwd string            `json:"cwd,omitempty"`
	Env map[string]string `json:"env,omitempty"`
}

func (SessionCreate) Op() string { return "session_create" }

// SessionList lists live session ids.
type SessionList struct{}

func (SessionList) Op() string { return "session_list" }

// SessionRm kills a session's shell and process group.
type SessionRm struct {
	ID string `json:"id"`
}

func (SessionRm) Op() string { return "session_rm" }

// Stdin carries a chunk of exec stdin.
type Stdin struct {
	Data B64 `json:"data"`
}

func (Stdin) Op() string { return "stdin" }

// StdinClose signals stdin EOF to the running exec.
type StdinClose struct{}

func (StdinClose) Op() string { return "stdin_close" }

// FsWrite streams Data frames into a file, atomically; nil Mode inherits or
// defaults.
type FsWrite struct {
	Path string  `json:"path"`
	Mode *uint32 `json:"mode,omitempty"`
}

func (FsWrite) Op() string { return "fs_write" }

// FsRead streams a file back as Data frames terminated by Done.
type FsRead struct {
	Path string `json:"path"`
}

func (FsRead) Op() string { return "fs_read" }

// FsList streams a directory as batched Entries frames terminated by Done.
type FsList struct {
	Path string `json:"path"`
}

func (FsList) Op() string { return "fs_list" }

// FsStat returns file metadata.
type FsStat struct {
	Path string `json:"path"`
}

func (FsStat) Op() string { return "fs_stat" }

// FsMkdir creates a directory.
type FsMkdir struct {
	Path    string `json:"path"`
	Parents bool   `json:"parents"`
}

func (FsMkdir) Op() string { return "fs_mkdir" }

// FsRm removes a file or directory tree.
type FsRm struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

func (FsRm) Op() string { return "fs_rm" }

// FsRename moves a file within the guest.
type FsRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (FsRename) Op() string { return "fs_rename" }

// FsPush extracts a client tar stream (Data frames) under dest.
type FsPush struct {
	Dest string `json:"dest"`
}

func (FsPush) Op() string { return "fs_push" }

// FsPull streams a path back as a tar stream (Data frames, then Done).
type FsPull struct {
	Path string `json:"path"`
}

func (FsPull) Op() string { return "fs_pull" }

// FsFind streams Match frames for lines under Path matching Pattern; Glob
// narrows the walk to file names matching it (`*` and `?` wildcards).
type FsFind struct {
	Path    string `json:"path"`
	Pattern string `json:"pattern"`
	Glob    string `json:"glob,omitempty"`
}

func (FsFind) Op() string { return "fs_find" }

// FsReplace rewrites Pattern to Replacement in each file, streaming one
// Replaced frame per file then Done.
type FsReplace struct {
	Files       []string `json:"files"`
	Pattern     string   `json:"pattern"`
	Replacement string   `json:"replacement"`
}

func (FsReplace) Op() string { return "fs_replace" }

// FsWatch streams Event frames under Path until the connection closes.
type FsWatch struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

func (FsWatch) Op() string { return "fs_watch" }

// PtyOpen runs the guest shell under a pseudo-terminal: Started, then Stdout
// frames out and Stdin frames in, until the shell exits (Exit).
type PtyOpen struct {
	Cols uint16            `json:"cols"`
	Rows uint16            `json:"rows"`
	Cwd  string            `json:"cwd,omitempty"`
	Env  map[string]string `json:"env,omitempty"`
	User string            `json:"user,omitempty"`
}

func (PtyOpen) Op() string { return "pty_open" }

// PtyResize resizes a live pty's window by pid.
type PtyResize struct {
	PID  uint32 `json:"pid"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

func (PtyResize) Op() string { return "pty_resize" }

// PortForward relays a guest TCP port over this connection: Ready once
// connected, then Data both ways (DataEnd half-closes the guest socket);
// the guest server closing ends the stream with Done.
type PortForward struct {
	Port uint16 `json:"port"`
}

func (PortForward) Op() string { return "port_forward" }

// GitClone clones a repo; network-lane only. Auth is a token passed as an
// in-memory Authorization header, never written to guest disk.
type GitClone struct {
	URL    string `json:"url"`
	Path   string `json:"path"`
	Branch string `json:"branch,omitempty"`
	Depth  uint32 `json:"depth,omitempty"`
	Auth   string `json:"auth,omitempty"`
}

func (GitClone) Op() string { return "git_clone" }

// GitStatus asks for a repo's structured status.
type GitStatus struct {
	Path string `json:"path"`
}

func (GitStatus) Op() string { return "git_status" }

// GitAdd stages files under a repo.
type GitAdd struct {
	Path  string   `json:"path"`
	Files []string `json:"files"`
}

func (GitAdd) Op() string { return "git_add" }

// GitCommit commits staged changes; Author is "Name <email>".
type GitCommit struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Author  string `json:"author"`
}

func (GitCommit) Op() string { return "git_commit" }

// GitPush pushes the current branch; network-lane only.
type GitPush struct {
	Path string `json:"path"`
	Auth string `json:"auth,omitempty"`
}

func (GitPush) Op() string { return "git_push" }

// GitPull pulls the current branch; network-lane only.
type GitPull struct {
	Path string `json:"path"`
	Auth string `json:"auth,omitempty"`
}

func (GitPull) Op() string { return "git_pull" }

// GitBranch lists, creates, deletes, or checks out a branch. Action is
// list|create|delete|checkout ("op" is reserved by the frame tag).
type GitBranch struct {
	Path   string `json:"path"`
	Action string `json:"action"`
	Name   string `json:"name,omitempty"`
}

func (GitBranch) Op() string { return "git_branch" }

// LspStart spawns the flavor image's language server for Language,
// rooted at Root.
type LspStart struct {
	Language string `json:"language"`
	Root     string `json:"root,omitempty"`
}

func (LspStart) Op() string { return "lsp_start" }

// LspRequest opens the JSON-RPC byte stream to a started language server.
type LspRequest struct {
	ServerID string `json:"server_id"`
}

func (LspRequest) Op() string { return "lsp_request" }

// LspStop kills a started language server.
type LspStop struct {
	ServerID string `json:"server_id"`
}

func (LspStop) Op() string { return "lsp_stop" }

// Data carries one chunk of an upload stream (FsWrite/FsPush payloads).
type Data struct {
	Data B64 `json:"data"`
}

func (Data) Op() string { return "data" } //nolint:goconst // wire tag shared with the response type by design

// DataEnd terminates an upload stream.
type DataEnd struct{}

func (DataEnd) Op() string { return "data_end" }

// Started reports the spawned pid (synthetic when the OS pid is unknown).
type Started struct {
	PID uint32 `json:"pid"`
}

func (Started) RespType() string { return "started" }

// Stdout carries a chunk of process stdout.
type Stdout struct {
	Data []byte `json:"data"`
}

func (Stdout) RespType() string { return "stdout" }

// Stderr carries a chunk of process stderr.
type Stderr struct {
	Data []byte `json:"data"`
}

func (Stderr) RespType() string { return "stderr" }

// Exit is the terminal frame of a foreground exec; -1 means killed or
// unknown.
type Exit struct {
	Code int32 `json:"code"`
}

func (Exit) RespType() string { return "exit" }

// Done is the terminal frame of verbs with no payload result.
type Done struct{}

func (Done) RespType() string { return "done" }

// Ready acknowledges an armed watch (events after it are guaranteed
// captured) or a connected port_forward.
type Ready struct{}

func (Ready) RespType() string { return "ready" }

// ErrorResp is the terminal frame of a failed verb; it implements error.
type ErrorResp struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (ErrorResp) RespType() string { return "error" }

func (e *ErrorResp) Error() string { return e.Kind + ": " + e.Message }

// InfoResp answers Info.
type InfoResp struct {
	Version    string `json:"version"`
	Proto      uint32 `json:"proto"`
	UptimeSecs uint64 `json:"uptime_secs"`
	Procs      int    `json:"procs"`
	Sessions   int    `json:"sessions"`
}

func (InfoResp) RespType() string { return "info" }

// Procs answers Ps.
type Procs struct {
	Procs []ProcInfo `json:"procs"`
}

func (Procs) RespType() string { return "procs" }

// DataResp carries one chunk of a download stream (FsRead/FsPull payloads).
type DataResp struct {
	Data []byte `json:"data"`
}

func (DataResp) RespType() string { return "data" }

// Entries answers FsList.
type Entries struct {
	Entries []DirEntry `json:"entries"`
}

func (Entries) RespType() string { return "entries" }

// Stat answers FsStat.
type Stat struct {
	Info FileInfo `json:"info"`
}

func (Stat) RespType() string { return "stat" }

// SessionCreated answers SessionCreate.
type SessionCreated struct {
	ID string `json:"id"`
}

func (SessionCreated) RespType() string { return "session_created" }

// LspStarted answers LspStart.
type LspStarted struct {
	ServerID string `json:"server_id"`
}

func (LspStarted) RespType() string { return "lsp_started" }

// Sessions answers SessionList.
type Sessions struct {
	Sessions []string `json:"sessions"`
}

func (Sessions) RespType() string { return "sessions" }

// Match is one FsFind hit; Line is 1-based.
type Match struct {
	File    string `json:"file"`
	Line    uint64 `json:"line"`
	Content string `json:"content"`
}

func (Match) RespType() string { return "match" }

// Replaced reports one FsReplace file result.
type Replaced struct {
	File         string `json:"file"`
	Replacements uint64 `json:"replacements"`
}

func (Replaced) RespType() string { return "replaced" }

// Event is one FsWatch filesystem event; Kind is one of the Event* consts.
type Event struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}

func (Event) RespType() string { return "event" }

// GitStatusResult answers GitStatus.
type GitStatusResult struct {
	Branch string          `json:"branch"`
	Ahead  uint32          `json:"ahead"`
	Behind uint32          `json:"behind"`
	Files  []GitFileStatus `json:"files"`
}

func (GitStatusResult) RespType() string { return "git_status_result" }

// GitFileStatus is one porcelain-v2 entry; Staged/Unstaged are XY status codes.
type GitFileStatus struct {
	Path     string `json:"path"`
	Staged   string `json:"staged"`
	Unstaged string `json:"unstaged"`
}

// GitCommitResult answers GitCommit with the new commit hash.
type GitCommitResult struct {
	Hash string `json:"hash"`
}

func (GitCommitResult) RespType() string { return "git_commit_result" }

// GitBranches answers GitBranch list.
type GitBranches struct {
	Current  string   `json:"current"`
	Branches []string `json:"branches"`
}

func (GitBranches) RespType() string { return "git_branches" }

// ProcInfo is one entry of Procs; ExitCode is absent while running.
type ProcInfo struct {
	PID                uint32   `json:"pid"`
	Argv               []string `json:"argv"`
	Detached           bool     `json:"detached"`
	State              string   `json:"state"`
	ExitCode           *int32   `json:"exit_code,omitempty"`
	StartedAtEpochSecs uint64   `json:"started_at_epoch_secs"`
}

// DirEntry is one entry of Entries; Kind is one of the FileKind* consts.
type DirEntry struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	Size uint64 `json:"size"`
}

// FileInfo is the Stat payload; Mode carries permission bits only.
type FileInfo struct {
	Kind           string `json:"kind"`
	Size           uint64 `json:"size"`
	Mode           uint32 `json:"mode"`
	MtimeEpochSecs uint64 `json:"mtime_epoch_secs"`
}

// EncodeRequest renders {"v":1,"op":...,fields} without a trailing newline.
func EncodeRequest(r Request) ([]byte, error) {
	return encodeTagged(requestHead+r.Op()+`"`, r)
}

// EncodeResponse renders {"type":...,fields} without a trailing newline.
func EncodeResponse(r Response) ([]byte, error) {
	return encodeTagged(`{"type":"`+r.RespType()+`"`, r)
}

// DecodeRequest parses one frame into its op's concrete type.
func DecodeRequest(line []byte) (Request, error) {
	op, err := scanTag(line, "op")
	if err != nil {
		return nil, fmt.Errorf("parse request frame: %w", err)
	}
	dec, ok := requestDecoders[op]
	if !ok {
		return nil, fmt.Errorf("unknown op %q", op)
	}
	return dec(line)
}

// DecodeResponse parses one frame into its type's concrete Go type.
func DecodeResponse(line []byte) (Response, error) {
	typ, err := scanTag(line, "type")
	if err != nil {
		return nil, fmt.Errorf("parse response frame: %w", err)
	}
	dec, ok := responseDecoders[typ]
	if !ok {
		return nil, fmt.Errorf("unknown response type %q", typ)
	}
	return dec(line)
}

// fastBulk decodes a bulk frame's base64 data field by slicing it out (base64's
// alphabet is JSON-escape-free) and skipping json.Unmarshal, which otherwise
// dominates the download path; slow is the json fallback for any other shape.
// The slice is only trusted when data is a last, top-level field — no nested
// container or escape before it, nothing after it — so an unexpected shape
// gets the full parse instead of silently picking the wrong bytes.
func fastBulk(slow func([]byte) (Response, error), mk func([]byte) Response) func([]byte) (Response, error) {
	return func(line []byte) (Response, error) {
		head, after, ok := bytes.Cut(line, []byte(`"data":"`))
		if !ok || len(head) == 0 || bytes.ContainsAny(head[1:], `{[\`) {
			return slow(line)
		}
		b64, rest, ok := bytes.Cut(after, []byte(`"`))
		if !ok || !bytes.Equal(rest, []byte(`}`)) {
			return slow(line)
		}
		out := make([]byte, base64.StdEncoding.DecodedLen(len(b64)))
		n, err := base64.StdEncoding.Decode(out, b64)
		if err != nil {
			return slow(line)
		}
		return mk(out[:n]), nil
	}
}

// encodeTagged marshals v as a flat object and splices the tag head in front
// of its fields, so wire tags never live on the structs themselves. The
// spare capacity byte lets Conn.Send append the newline without a copy.
func encodeTagged(head string, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 0, len(head)+len(body)+1)
	frame = append(frame, head...)
	if len(body) == 2 { // "{}"
		return append(frame, '}'), nil
	}
	frame = append(frame, ',')
	return append(frame, body[1:]...), nil
}

// scanTag extracts the string value of a top-level key without decoding the
// whole frame: both producers emit the tag first, so the token walk stops at
// the frame head on bulk data frames instead of pre-scanning every payload
// byte before the real decode. A missing tag returns "" (the callers'
// unknown-tag error); key order never affects correctness, only how early
// the walk stops.
func scanTag(line []byte, key string) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return "", fmt.Errorf("frame is not an object")
	}
	for dec.More() {
		if tok, err = dec.Token(); err != nil {
			return "", err
		}
		if k, ok := tok.(string); ok && k == key {
			if tok, err = dec.Token(); err != nil {
				return "", err
			}
			s, ok := tok.(string)
			if !ok {
				return "", fmt.Errorf("%s tag is not a string", key)
			}
			return s, nil
		}
		if err = skipValue(dec); err != nil {
			return "", err
		}
	}
	return "", nil
}

// skipValue consumes one JSON value, recursing into containers.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); ok && (d == '{' || d == '[') {
		for dec.More() {
			if err = skipValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
	}
	return err
}

func decodeAs[T any](line []byte) (*T, error) {
	v := new(T)
	if err := json.Unmarshal(line, v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeReq / decodeResp adapt decodeAs to the interface-typed decoder maps.
// A table entry pairing an op with a non-Request type is caught by the shared
// fixture round-trip test, not the compiler.
func decodeReq[T any](line []byte) (Request, error) {
	v, err := decodeAs[T](line)
	if err != nil {
		return nil, err
	}
	return any(v).(Request), nil
}

func decodeResp[T any](line []byte) (Response, error) {
	v, err := decodeAs[T](line)
	if err != nil {
		return nil, err
	}
	return any(v).(Response), nil
}
