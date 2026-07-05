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

// GitClone clones a repo; network-lane only. Auth is a token passed as an
// in-memory Authorization header, never written to guest disk.
type GitClone struct {
	URL    string `json:"url"`
	Path   string `json:"path"`
	Branch string `json:"branch,omitempty"`
	Depth  uint32 `json:"depth,omitempty"`
	Auth   string `json:"auth,omitempty"`
}

// GitStatus asks for a repo's structured status.
type GitStatus struct {
	Path string `json:"path"`
}

// GitAdd stages files under a repo.
type GitAdd struct {
	Path  string   `json:"path"`
	Files []string `json:"files"`
}

// GitCommit commits staged changes; Author is "Name <email>".
type GitCommit struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Author  string `json:"author"`
}

// GitPush pushes the current branch; network-lane only.
type GitPush struct {
	Path string `json:"path"`
	Auth string `json:"auth,omitempty"`
}

// GitPull pulls the current branch; network-lane only.
type GitPull struct {
	Path string `json:"path"`
	Auth string `json:"auth,omitempty"`
}

// GitBranch lists, creates, deletes, or checks out a branch. Action is
// list|create|delete|checkout ("op" is reserved by the frame tag).
type GitBranch struct {
	Path   string `json:"path"`
	Action string `json:"action"`
	Name   string `json:"name,omitempty"`
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
func (GitClone) Op() string      { return "git_clone" }
func (GitStatus) Op() string     { return "git_status" }
func (GitAdd) Op() string        { return "git_add" }
func (GitCommit) Op() string     { return "git_commit" }
func (GitPush) Op() string       { return "git_push" }
func (GitPull) Op() string       { return "git_pull" }
func (GitBranch) Op() string     { return "git_branch" }
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

// GitStatusResult answers GitStatus.
type GitStatusResult struct {
	Branch string          `json:"branch"`
	Ahead  uint32          `json:"ahead"`
	Behind uint32          `json:"behind"`
	Files  []GitFileStatus `json:"files"`
}

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

// GitBranches answers GitBranch list.
type GitBranches struct {
	Current  string   `json:"current"`
	Branches []string `json:"branches"`
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

func (Started) RespType() string         { return "started" }
func (Stdout) RespType() string          { return "stdout" }
func (Stderr) RespType() string          { return "stderr" }
func (Exit) RespType() string            { return "exit" }
func (Done) RespType() string            { return "done" }
func (ErrorResp) RespType() string       { return "error" }
func (InfoResp) RespType() string        { return "info" }
func (Procs) RespType() string           { return "procs" }
func (DataResp) RespType() string        { return "data" }
func (Entries) RespType() string         { return "entries" }
func (Stat) RespType() string            { return "stat" }
func (SessionCreated) RespType() string  { return "session_created" }
func (Sessions) RespType() string        { return "sessions" }
func (Match) RespType() string           { return "match" }
func (Replaced) RespType() string        { return "replaced" }
func (Event) RespType() string           { return "event" }
func (GitStatusResult) RespType() string { return "git_status_result" }
func (GitCommitResult) RespType() string { return "git_commit_result" }
func (GitBranches) RespType() string     { return "git_branches" }

func (e *ErrorResp) Error() string { return e.Kind + ": " + e.Message }

// EncodeRequest renders {"v":1,"op":...,fields} without a trailing newline.
func EncodeRequest(r Request) ([]byte, error) {
	return encodeTagged(requestHead+r.Op()+`"`, r)
}

// EncodeResponse renders {"type":...,fields} without a trailing newline.
func EncodeResponse(r Response) ([]byte, error) {
	return encodeTagged(`{"type":"`+r.RespType()+`"`, r)
}

// requestDecoders maps each op tag to a decoder; table dispatch keeps this
// (and the verb set) flat instead of a switch that grows past the complexity
// budget as verbs are added.
var requestDecoders = map[string]func([]byte) (Request, error){
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
	"git_clone":      decodeReq[GitClone],
	"git_status":     decodeReq[GitStatus],
	"git_add":        decodeReq[GitAdd],
	"git_commit":     decodeReq[GitCommit],
	"git_push":       decodeReq[GitPush],
	"git_pull":       decodeReq[GitPull],
	"git_branch":     decodeReq[GitBranch],
	"data":           decodeReq[Data],
	"data_end":       decodeReq[DataEnd],
}

// DecodeRequest parses one frame into its op's concrete type.
func DecodeRequest(line []byte) (Request, error) {
	var tag struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(line, &tag); err != nil {
		return nil, fmt.Errorf("parse request frame: %w", err)
	}
	dec, ok := requestDecoders[tag.Op]
	if !ok {
		return nil, fmt.Errorf("unknown op %q", tag.Op)
	}
	return dec(line)
}

// responseDecoders maps each type tag to a decoder. Two-stage dispatch is
// required because the same key differs in shape across variants (info's
// procs is a count, ps's procs is a list).
var responseDecoders = map[string]func([]byte) (Response, error){
	"started":           decodeResp[Started],
	"stdout":            decodeResp[Stdout],
	"stderr":            decodeResp[Stderr],
	"exit":              decodeResp[Exit],
	"done":              decodeResp[Done],
	"error":             decodeResp[ErrorResp],
	"info":              decodeResp[InfoResp],
	"procs":             decodeResp[Procs],
	"data":              decodeResp[DataResp],
	"entries":           decodeResp[Entries],
	"stat":              decodeResp[Stat],
	"session_created":   decodeResp[SessionCreated],
	"sessions":          decodeResp[Sessions],
	"match":             decodeResp[Match],
	"replaced":          decodeResp[Replaced],
	"event":             decodeResp[Event],
	"git_status_result": decodeResp[GitStatusResult],
	"git_commit_result": decodeResp[GitCommitResult],
	"git_branches":      decodeResp[GitBranches],
}

// DecodeResponse parses one frame into its type's concrete Go type.
func DecodeResponse(line []byte) (Response, error) {
	var tag struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &tag); err != nil {
		return nil, fmt.Errorf("parse response frame: %w", err)
	}
	dec, ok := responseDecoders[tag.Type]
	if !ok {
		return nil, fmt.Errorf("unknown response type %q", tag.Type)
	}
	return dec(line)
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

// decodeReq / decodeResp adapt decodeAs to the interface-typed decoder maps.
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
