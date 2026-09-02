package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

var tools = []tool{
	{
		"create_sandbox", "Claim a fresh microVM sandbox and return its id plus deadline. Warm claims take milliseconds; a cold template boots in well under a second. Every sandbox-scoped tool takes the returned sandbox_id. The sandbox is destroyed at its deadline unless released earlier; nothing renews it.",
		schema(props{"template": str("template image ref, or a name published by promote; empty uses the server default"), "net": str("network lane: none (default, no NIC, vsock-only I/O) or egress (bridge NIC, outbound network)"), "size": str("resource tier: small (default), medium, large, xlarge"), "ttl_seconds": integer("sandbox lifetime in seconds; 0 means one hour, and nothing renews it")}), toolCreateSandbox,
	},
	{
		"exec", "Run a shell command in a sandbox, wait for it to exit, and return stdout, stderr, and the exit code as JSON. The call is cut off after 5 minutes; for servers or long jobs use spawn instead. A hibernated sandbox wakes transparently on this call.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "command": str("shell command, run via sh -c"), "cwd": str("working directory; empty runs in the guest's default")}, "sandbox_id", "command"), toolExec,
	},
	{
		"spawn", "Start a shell command detached in a sandbox and return its guest pid immediately. The process keeps running across later tool calls; its output goes to a per-process ring buffer that keeps up to 256 KiB of the newest whole output chunks, which logs replays. Use exec instead when you need the result now.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "command": str("shell command, run via sh -c"), "cwd": str("working directory; empty runs in the guest's default")}, "sandbox_id", "command"), toolSpawn,
	},
	{
		"ps", "List the sandbox's tracked processes (exec, spawn, and pty commands) as JSON: pid, argv, detached, state (running or exited), exit_code once exited, and start time. Processes started inside the guest by other means are not listed.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint")}, "sandbox_id"), toolPs,
	},
	{
		"kill", "Send a signal to a tracked process. Killing a process that already exited is a no-op success; the guest never re-signals a reaped pid.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "pid": integer("guest pid from spawn or ps"), "signal": integer("signal number, e.g. 15 for SIGTERM; 0 sends SIGKILL")}, "sandbox_id", "pid"), toolKill,
	},
	{
		"logs", "Return a tracked process's buffered stdout and stderr, plus exit_code once it has ended. The buffer keeps up to 256 KiB of the newest whole output chunks per process, so redirect long output to a file when it must be complete.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "pid": integer("guest pid from spawn or ps")}, "sandbox_id", "pid"), toolLogs,
	},
	{
		"write_file", "Write text content to a file in a sandbox, replacing any existing file. The write is atomic (temp file plus rename); an existing file keeps its permission bits. The parent directory must already exist.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "path": str("absolute path of the file"), "content": str("full file content; written verbatim")}, "sandbox_id", "path", "content"), toolWriteFile,
	},
	{
		"read_file", "Return the whole content of a file in a sandbox as text; bytes that are not valid UTF-8 are replaced, so binary content is lossy. For large or binary files use exec with head, tail, or a checksum instead. A missing path is an error.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "path": str("absolute path of the file")}, "sandbox_id", "path"), toolReadFile,
	},
	{
		"list_dir", "List one directory in a sandbox (not recursive) as JSON entries with name, kind (file, dir, symlink, or other), and size in bytes.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "path": str("absolute path of the directory")}, "sandbox_id", "path"), toolListDir,
	},
	{
		"fork", "Clone a sandbox into N independent children that start from its exact memory and disk state, including running processes; each child gets its own id and lives one hour. N is capped by the node's max_fork_count (default 16). All-or-nothing: on failure no child survives. The parent keeps running.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "count": integer("number of children, 1 to the node's max_fork_count (default 16)")}, "sandbox_id", "count"), toolFork,
	},
	{
		"checkpoint", "Capture a sandbox's full state (memory, disk, running processes) without stopping it and return a checkpoint id. branch_checkpoint claims new sandboxes from that moment any number of times; a checkpoint outlives this session until it is deleted or expires under the node's checkpoint TTL.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "name": str("optional human label")}, "sandbox_id"), toolCheckpoint,
	},
	{
		"branch_checkpoint", "Claim a fresh sandbox that resumes from a checkpoint's exact captured moment and return its id; it lives one hour. The checkpoint is unchanged and can be branched again.",
		schema(props{"checkpoint_id": str("id returned by checkpoint or list_checkpoints")}, "checkpoint_id"), toolBranchCheckpoint,
	},
	{"list_checkpoints", "List the node's checkpoints newest first: checkpoint_id, name, source sandbox_id, created_at.", schema(props{}), toolListCheckpoints},
	{"delete_checkpoint", "Delete the node's copy of a checkpoint; a replica a peer node healed stays branchable until its own TTL sweep. Sandboxes already branched from it are unaffected.", schema(props{"checkpoint_id": str("id returned by checkpoint or list_checkpoints")}, "checkpoint_id"), toolDeleteCheckpoint},
	{
		"hibernate", "Snapshot a sandbox and stop its VM, freeing memory while keeping its id, files, processes, and shell state. The next call that reaches the guest (exec, spawn, ps, kill, logs, the file tools) wakes it transparently in tens of milliseconds; fork, checkpoint, promote and release act on the snapshot without waking it.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint")}, "sandbox_id"), toolHibernate,
	},
	{
		"promote", "Publish the sandbox's current state as a named template on its node; later create_sandbox calls with that name (and the same net and size) start from it. Re-promoting to the same name replaces the template.",
		schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint"), "template_name": str("template name to publish as")}, "sandbox_id", "template_name"), toolPromote,
	},
	{"release", "Destroy a sandbox and free its resources; files and processes inside it are lost. This session forgets the id, so a second release of it is rejected as unknown.", schema(props{"sandbox_id": str("id returned by create_sandbox, fork, or branch_checkpoint")}, "sandbox_id"), toolRelease},
	{"node_info", "Report the connected node's warm pools, live claims, drain state, capacity, and mesh peers as JSON.", schema(props{}), toolNodeInfo},
}

// tool binds one MCP tool's spec to its handler, so a tool can never exist
// in the listing without a dispatch entry.
type tool struct {
	name        string
	description string
	schema      map[string]any
	handler     func(context.Context, *server, json.RawMessage) (string, error)
}

func toolSpecs() []map[string]any {
	specs := make([]map[string]any, len(tools))
	for i, t := range tools {
		specs[i] = map[string]any{"name": t.name, "description": t.description, "inputSchema": t.schema}
	}
	return specs
}

func toolCreateSandbox(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		Template   string `json:"template"`
		Net        string `json:"net"`
		Size       string `json:"size"`
		TTLSeconds int    `json:"ttl_seconds"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	// An agent session outlives the node's 5-minute default and nothing renews
	// a lease, so the sandbox would vanish mid-conversation.
	ttl := cmp.Or(time.Duration(args.TTLSeconds)*time.Second, defaultToolTTL)
	opts := []sandbox.Option{sandbox.WithTimeout(ttl)}
	if args.Net != "" {
		opts = append(opts, sandbox.WithNetwork(sandbox.NetShape(args.Net)))
	}
	if args.Size != "" {
		opts = append(opts, sandbox.WithSize(sandbox.Size(args.Size)))
	}
	sb, err := s.client.New(ctx, cmp.Or(args.Template, s.template), opts...)
	if err != nil {
		return "", err
	}
	s.trackBox(sb)
	return jsonText(map[string]any{"sandbox_id": sb.ID, "deadline": sb.Deadline}), nil
}

type sandboxArg struct {
	SandboxID string `json:"sandbox_id"`
}

func (a sandboxArg) id() string { return a.SandboxID }

type sandboxIDer interface{ id() string }

// parseAndBox decodes a tool's arguments and resolves their sandbox_id to a
// live handle — the prologue every sandbox-scoped tool shares.
func parseAndBox[T sandboxIDer](s *server, raw json.RawMessage) (T, *sandbox.Sandbox, error) {
	var args T
	if err := parse(raw, &args); err != nil {
		return args, nil, err
	}
	sb, err := s.box(args.id())
	return args, sb, err
}

type cmdArgs struct {
	sandboxArg
	Command string `json:"command"`
	Cwd     string `json:"cwd"`
}

func toolExec(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[cmdArgs](s, raw)
	if err != nil {
		return "", err
	}
	var stdout, stderr strings.Builder
	code, err := sb.Run(ctx, sandbox.Cmd{
		Argv: []string{"sh", "-c", args.Command}, Cwd: args.Cwd,
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
		return "", err
	}
	return jsonText(map[string]any{"exit_code": code, "stdout": stdout.String(), "stderr": stderr.String()}), nil
}

func toolSpawn(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[cmdArgs](s, raw)
	if err != nil {
		return "", err
	}
	pid, err := sb.Spawn(ctx, sandbox.Cmd{Argv: []string{"sh", "-c", args.Command}, Cwd: args.Cwd})
	if err != nil {
		return "", err
	}
	return jsonText(map[string]uint32{"pid": pid}), nil
}

func toolPs(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	sb, err := s.boxArg(raw)
	if err != nil {
		return "", err
	}
	procs, err := sb.Ps(ctx)
	if err != nil {
		return "", err
	}
	return jsonText(procs), nil
}

type killArgs struct {
	sandboxArg
	PID    uint32 `json:"pid"`
	Signal int32  `json:"signal"`
}

func toolKill(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[killArgs](s, raw)
	if err != nil {
		return "", err
	}
	if err := sb.Kill(ctx, args.PID, args.Signal); err != nil {
		return "", err
	}
	return "killed", nil
}

type logsArgs struct {
	sandboxArg
	PID uint32 `json:"pid"`
}

func toolLogs(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[logsArgs](s, raw)
	if err != nil {
		return "", err
	}
	var stdout, stderr strings.Builder
	code, exited, err := sb.Logs(ctx, args.PID, &stdout, &stderr)
	if err != nil {
		return "", err
	}
	out := map[string]any{"stdout": stdout.String(), "stderr": stderr.String()}
	if exited {
		out["exit_code"] = code
	}
	return jsonText(out), nil
}

type writeFileArgs struct {
	sandboxArg
	Path    string `json:"path"`
	Content string `json:"content"`
}

func toolWriteFile(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[writeFileArgs](s, raw)
	if err != nil {
		return "", err
	}
	if err := sb.WriteFile(ctx, args.Path, []byte(args.Content), nil); err != nil {
		return "", err
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(args.Content), args.Path), nil
}

type pathArgs struct {
	sandboxArg
	Path string `json:"path"`
}

func toolReadFile(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[pathArgs](s, raw)
	if err != nil {
		return "", err
	}
	data, err := sb.ReadFile(ctx, args.Path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func toolListDir(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[pathArgs](s, raw)
	if err != nil {
		return "", err
	}
	entries, err := sb.ListDir(ctx, args.Path)
	if err != nil {
		return "", err
	}
	return jsonText(entries), nil
}

type forkArgs struct {
	sandboxArg
	Count int `json:"count"`
}

func toolFork(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[forkArgs](s, raw)
	if err != nil {
		return "", err
	}
	children, err := sb.Fork(ctx, args.Count, defaultToolTTL)
	if err != nil {
		return "", err
	}
	ids := make([]string, len(children))
	for i, c := range children {
		s.trackBox(c)
		ids[i] = c.ID
	}
	return jsonText(map[string]any{"sandbox_ids": ids}), nil
}

type checkpointArgs struct {
	sandboxArg
	Name string `json:"name"`
}

func toolCheckpoint(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[checkpointArgs](s, raw)
	if err != nil {
		return "", err
	}
	ckpt, err := sb.Checkpoint(ctx, args.Name)
	if err != nil {
		return "", err
	}
	s.trackCkpt(ckpt)
	return jsonText(map[string]any{"checkpoint_id": ckpt.ID, "name": ckpt.Name}), nil
}

func toolBranchCheckpoint(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	ckpt, err := s.checkpointArg(raw)
	if err != nil {
		return "", err
	}
	sb, err := ckpt.New(ctx, sandbox.WithTimeout(defaultToolTTL))
	if err != nil {
		return "", err
	}
	s.trackBox(sb)
	return jsonText(map[string]any{"sandbox_id": sb.ID}), nil
}

func toolListCheckpoints(ctx context.Context, s *server, _ json.RawMessage) (string, error) {
	ckpts, err := s.client.Checkpoints(ctx)
	if err != nil {
		return "", err
	}
	out := make([]map[string]any, len(ckpts))
	for i, ck := range ckpts {
		out[i] = map[string]any{"checkpoint_id": ck.ID, "name": ck.Name, "sandbox_id": ck.SandboxID, "created_at": ck.CreatedAt}
	}
	return jsonText(out), nil
}

func toolDeleteCheckpoint(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	ckpt, err := s.checkpointArg(raw)
	if err != nil {
		return "", err
	}
	if err := ckpt.Delete(ctx); err != nil {
		return "", err
	}
	s.dropCkpt(ckpt.ID)
	return "deleted", nil
}

func toolHibernate(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	sb, err := s.boxArg(raw)
	if err != nil {
		return "", err
	}
	if err := sb.Hibernate(ctx); err != nil {
		return "", err
	}
	return "hibernated (any later call wakes it transparently)", nil
}

type promoteArgs struct {
	sandboxArg
	TemplateName string `json:"template_name"`
}

func toolPromote(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	args, sb, err := parseAndBox[promoteArgs](s, raw)
	if err != nil {
		return "", err
	}
	tpl, err := sb.Promote(ctx, args.TemplateName)
	if err != nil {
		return "", err
	}
	return jsonText(map[string]any{"template": tpl.Name}), nil
}

func toolRelease(_ context.Context, s *server, raw json.RawMessage) (string, error) {
	sb, err := s.boxArg(raw)
	if err != nil {
		return "", err
	}
	if err := sb.Close(); err != nil {
		return "", err
	}
	s.dropBox(sb.ID)
	return "released", nil
}

func toolNodeInfo(ctx context.Context, s *server, _ json.RawMessage) (string, error) {
	info, err := s.client.Info(ctx)
	if err != nil {
		return "", err
	}
	return jsonText(info), nil
}

func (s *server) boxArg(raw json.RawMessage) (*sandbox.Sandbox, error) {
	_, sb, err := parseAndBox[sandboxArg](s, raw)
	return sb, err
}

// checkpointArg resolves a checkpoint_id argument: a handle minted in this
// session when available, else a fresh one — checkpoints outlive sessions.
func (s *server) checkpointArg(raw json.RawMessage) (*sandbox.Checkpoint, error) {
	var args struct {
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := parse(raw, &args); err != nil {
		return nil, err
	}
	if ckpt, ok := s.ckpt(args.CheckpointID); ok {
		return ckpt, nil
	}
	if args.CheckpointID == "" {
		return nil, fmt.Errorf("checkpoint_id is required")
	}
	return s.client.Checkpoint(args.CheckpointID), nil
}

func parse(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	return nil
}

func jsonText(v any) string {
	out, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return string(out)
}

type props map[string]map[string]any

func str(description string) map[string]any {
	p := map[string]any{"type": "string"}
	if description != "" {
		p["description"] = description
	}
	return p
}

func integer(description string) map[string]any {
	return map[string]any{"type": "integer", "description": description}
}

func schema(p props, required ...string) map[string]any {
	out := map[string]any{"type": "object", "properties": p}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}
