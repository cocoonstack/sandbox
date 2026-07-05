// Package silkd is the host-side binding of the silkd wire protocol:
// newline-delimited JSON frames over one connection per RPC, requests tagged
// by "op", responses by "type", binary payloads base64 in data fields. The
// authoritative contract is the shared corpus in protocol/fixtures/v1 —
// silkd's Rust tests and this package's tests round-trip the same files.
package silkd

import (
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
)

var requestHead = `{"v":` + strconv.Itoa(ProtoVersion) + `,"op":"`

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

// Info asks for the daemon's identity and counters — the readiness probe.
type Info struct{}

// Ps lists tracked processes.
type Ps struct{}

// Kill signals a process; nil Signal means SIGKILL.
type Kill struct {
	PID    uint32 `json:"pid"`
	Signal *int32 `json:"signal,omitempty"`
}

// Attach streams a running process's buffered and live output.
type Attach struct {
	PID uint32 `json:"pid"`
}

// Logs returns a process's ring-buffered output.
type Logs struct {
	PID uint32 `json:"pid"`
}

// SessionCreate opens a persistent shell; empty ID lets silkd name it.
type SessionCreate struct {
	ID  string            `json:"id,omitempty"`
	Cwd string            `json:"cwd,omitempty"`
	Env map[string]string `json:"env,omitempty"`
}

// SessionList lists live session ids.
type SessionList struct{}

// SessionRm kills a session's shell and process group.
type SessionRm struct {
	ID string `json:"id"`
}

// Stdin carries a chunk of exec stdin.
type Stdin struct {
	Data B64 `json:"data"`
}

// StdinClose signals stdin EOF to the running exec.
type StdinClose struct{}

// FsWrite streams Data frames into a file, atomically; nil Mode inherits or
// defaults.
type FsWrite struct {
	Path string  `json:"path"`
	Mode *uint32 `json:"mode,omitempty"`
}

// FsRead streams a file back as Data frames terminated by Done.
type FsRead struct {
	Path string `json:"path"`
}

// FsList streams a directory as batched Entries frames terminated by Done.
type FsList struct {
	Path string `json:"path"`
}

// FsStat returns file metadata.
type FsStat struct {
	Path string `json:"path"`
}

// FsMkdir creates a directory.
type FsMkdir struct {
	Path    string `json:"path"`
	Parents bool   `json:"parents"`
}

// FsRm removes a file or directory tree.
type FsRm struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

// FsRename moves a file within the guest.
type FsRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// FsPush extracts a client tar stream (Data frames) under dest.
type FsPush struct {
	Dest string `json:"dest"`
}

// FsPull streams a path back as a tar stream (Data frames, then Done).
type FsPull struct {
	Path string `json:"path"`
}

// FsFind streams Match frames for lines under Path matching Pattern; Glob
// narrows the walk to file names containing it.
type FsFind struct {
	Path    string `json:"path"`
	Pattern string `json:"pattern"`
	Glob    string `json:"glob,omitempty"`
}

// FsReplace rewrites Pattern to Replacement in each file, streaming one
// Replaced frame per file then Done.
type FsReplace struct {
	Files       []string `json:"files"`
	Pattern     string   `json:"pattern"`
	Replacement string   `json:"replacement"`
}

// FsWatch streams Event frames under Path until the connection closes.
type FsWatch struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

// PtyOpen runs the guest shell under a pseudo-terminal: Started, then Stdout
// frames out and Stdin frames in, until the shell exits (Exit).
type PtyOpen struct {
	Cols uint16            `json:"cols"`
	Rows uint16            `json:"rows"`
	Cwd  string            `json:"cwd,omitempty"`
	Env  map[string]string `json:"env,omitempty"`
	User string            `json:"user,omitempty"`
}

// PtyResize resizes a live pty's window by pid.
type PtyResize struct {
	PID  uint32 `json:"pid"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// Data carries one chunk of an upload stream (FsWrite/FsPush payloads).
type Data struct {
	Data B64 `json:"data"`
}

// DataEnd terminates an upload stream.
type DataEnd struct{}

func (Exec) Op() string          { return "exec" }
func (Info) Op() string          { return "info" } //nolint:goconst // wire tag shared with the response type by design
func (Ps) Op() string            { return "ps" }
func (Kill) Op() string          { return "kill" }
func (Attach) Op() string        { return "attach" }
func (Logs) Op() string          { return "logs" }
func (SessionCreate) Op() string { return "session_create" }
func (SessionList) Op() string   { return "session_list" }
func (SessionRm) Op() string     { return "session_rm" }
func (Stdin) Op() string         { return "stdin" }
func (StdinClose) Op() string    { return "stdin_close" }
func (FsWrite) Op() string       { return "fs_write" }
func (FsRead) Op() string        { return "fs_read" }
func (FsList) Op() string        { return "fs_list" }
func (FsStat) Op() string        { return "fs_stat" }
func (FsMkdir) Op() string       { return "fs_mkdir" }
func (FsRm) Op() string          { return "fs_rm" }
func (FsRename) Op() string      { return "fs_rename" }
func (FsPush) Op() string        { return "fs_push" }
func (FsPull) Op() string        { return "fs_pull" }
func (FsFind) Op() string        { return "fs_find" }
func (FsReplace) Op() string     { return "fs_replace" }
func (FsWatch) Op() string       { return "fs_watch" }
func (PtyOpen) Op() string       { return "pty_open" }
func (PtyResize) Op() string     { return "pty_resize" }
func (Data) Op() string          { return "data" } //nolint:goconst // wire tag shared with the response type by design
func (DataEnd) Op() string       { return "data_end" }

// Started reports the spawned pid (synthetic when the OS pid is unknown).
type Started struct {
	PID uint32 `json:"pid"`
}

// Stdout carries a chunk of process stdout.
type Stdout struct {
	Data []byte `json:"data"`
}

// Stderr carries a chunk of process stderr.
type Stderr struct {
	Data []byte `json:"data"`
}

// Exit is the terminal frame of a foreground exec; -1 means killed or
// unknown.
type Exit struct {
	Code int32 `json:"code"`
}

// Done is the terminal frame of verbs with no payload result.
type Done struct{}

// ErrorResp is the terminal frame of a failed verb; it implements error.
type ErrorResp struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// InfoResp answers Info.
type InfoResp struct {
	Version    string `json:"version"`
	Proto      uint32 `json:"proto"`
	UptimeSecs uint64 `json:"uptime_secs"`
	Procs      int    `json:"procs"`
	Sessions   int    `json:"sessions"`
}

// Procs answers Ps.
type Procs struct {
	Procs []ProcInfo `json:"procs"`
}

// DataResp carries one chunk of a download stream (FsRead/FsPull payloads).
type DataResp struct {
	Data []byte `json:"data"`
}

// Entries answers FsList.
type Entries struct {
	Entries []DirEntry `json:"entries"`
}

// Stat answers FsStat.
type Stat struct {
	Info FileInfo `json:"info"`
}

// SessionCreated answers SessionCreate.
type SessionCreated struct {
	ID string `json:"id"`
}

// Sessions answers SessionList.
type Sessions struct {
	Sessions []string `json:"sessions"`
}

// Match is one FsFind hit; Line is 1-based.
type Match struct {
	File    string `json:"file"`
	Line    uint64 `json:"line"`
	Content string `json:"content"`
}

// Replaced reports one FsReplace file result.
type Replaced struct {
	File         string `json:"file"`
	Replacements uint64 `json:"replacements"`
}

// Event is one FsWatch filesystem event; Kind is created|modified|deleted|renamed.
type Event struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}

// ProcInfo is one entry of Procs; ExitCode is absent while running.
type ProcInfo struct {
	PID                uint32   `json:"pid"`
	Argv               []string `json:"argv"`
	Detached           bool     `json:"detached"`
	State              string   `json:"state"`
	ExitCode           *int32   `json:"exit_code,omitempty"`
	StartedAtEpochSecs uint64   `json:"started_at_epoch_secs"`
}

// DirEntry is one entry of Entries; Kind is file|dir|symlink|other.
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

func (Started) RespType() string        { return "started" }
func (Stdout) RespType() string         { return "stdout" }
func (Stderr) RespType() string         { return "stderr" }
func (Exit) RespType() string           { return "exit" }
func (Done) RespType() string           { return "done" }
func (ErrorResp) RespType() string      { return "error" }
func (InfoResp) RespType() string       { return "info" }
func (Procs) RespType() string          { return "procs" }
func (DataResp) RespType() string       { return "data" }
func (Entries) RespType() string        { return "entries" }
func (Stat) RespType() string           { return "stat" }
func (SessionCreated) RespType() string { return "session_created" }
func (Sessions) RespType() string       { return "sessions" }
func (Match) RespType() string          { return "match" }
func (Replaced) RespType() string       { return "replaced" }
func (Event) RespType() string          { return "event" }

func (e *ErrorResp) Error() string { return e.Kind + ": " + e.Message }

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
	var tag struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(line, &tag); err != nil {
		return nil, fmt.Errorf("parse request frame: %w", err)
	}
	switch tag.Op {
	case "exec":
		return decodeAs[Exec](line)
	case "info":
		return decodeAs[Info](line)
	case "ps":
		return decodeAs[Ps](line)
	case "kill":
		return decodeAs[Kill](line)
	case "attach":
		return decodeAs[Attach](line)
	case "logs":
		return decodeAs[Logs](line)
	case "session_create":
		return decodeAs[SessionCreate](line)
	case "session_list":
		return decodeAs[SessionList](line)
	case "session_rm":
		return decodeAs[SessionRm](line)
	case "stdin":
		return decodeAs[Stdin](line)
	case "stdin_close":
		return decodeAs[StdinClose](line)
	case "fs_write":
		return decodeAs[FsWrite](line)
	case "fs_read":
		return decodeAs[FsRead](line)
	case "fs_list":
		return decodeAs[FsList](line)
	case "fs_stat":
		return decodeAs[FsStat](line)
	case "fs_mkdir":
		return decodeAs[FsMkdir](line)
	case "fs_rm":
		return decodeAs[FsRm](line)
	case "fs_rename":
		return decodeAs[FsRename](line)
	case "fs_push":
		return decodeAs[FsPush](line)
	case "fs_pull":
		return decodeAs[FsPull](line)
	case "fs_find":
		return decodeAs[FsFind](line)
	case "fs_replace":
		return decodeAs[FsReplace](line)
	case "fs_watch":
		return decodeAs[FsWatch](line)
	case "pty_open":
		return decodeAs[PtyOpen](line)
	case "pty_resize":
		return decodeAs[PtyResize](line)
	case "data":
		return decodeAs[Data](line)
	case "data_end":
		return decodeAs[DataEnd](line)
	default:
		return nil, fmt.Errorf("unknown op %q", tag.Op)
	}
}

// DecodeResponse parses one frame into its type's concrete Go type. The
// dispatch must be two-stage: the same key can differ in shape across
// variants (info's procs is a count, ps's procs is a list).
func DecodeResponse(line []byte) (Response, error) {
	var tag struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &tag); err != nil {
		return nil, fmt.Errorf("parse response frame: %w", err)
	}
	switch tag.Type {
	case "started":
		return decodeAs[Started](line)
	case "stdout":
		return decodeAs[Stdout](line)
	case "stderr":
		return decodeAs[Stderr](line)
	case "exit":
		return decodeAs[Exit](line)
	case "done":
		return decodeAs[Done](line)
	case "error":
		return decodeAs[ErrorResp](line)
	case "info":
		return decodeAs[InfoResp](line)
	case "procs":
		return decodeAs[Procs](line)
	case "data":
		return decodeAs[DataResp](line)
	case "entries":
		return decodeAs[Entries](line)
	case "stat":
		return decodeAs[Stat](line)
	case "session_created":
		return decodeAs[SessionCreated](line)
	case "sessions":
		return decodeAs[Sessions](line)
	case "match":
		return decodeAs[Match](line)
	case "replaced":
		return decodeAs[Replaced](line)
	case "event":
		return decodeAs[Event](line)
	default:
		return nil, fmt.Errorf("unknown response type %q", tag.Type)
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

func decodeAs[T any](line []byte) (*T, error) {
	v := new(T)
	if err := json.Unmarshal(line, v); err != nil {
		return nil, err
	}
	return v, nil
}
