package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

// tool binds one MCP tool's spec to its handler, so a tool can never exist
// in the listing without a dispatch entry.
type tool struct {
	name        string
	description string
	schema      map[string]any
	handler     func(context.Context, *server, json.RawMessage) (string, error)
}

var tools = []tool{
	{
		"create_sandbox", "Claim a fresh microVM sandbox; returns its id. Warm claims are milliseconds.",
		schema(props{"template": str("template image ref; empty uses the server default"), "ttl_seconds": integer("sandbox lifetime; 0 = server default")}), toolCreateSandbox,
	},
	{
		"exec", "Run a command in a sandbox and return stdout/stderr/exit code. A hibernated sandbox wakes transparently.",
		schema(props{"sandbox_id": str("sandbox to run in"), "command": str("shell command, run via sh -c"), "cwd": str("working directory (optional)")}, "sandbox_id", "command"), toolExec,
	},
	{
		"write_file", "Write text to a file in a sandbox (atomic on the guest).",
		schema(props{"sandbox_id": str(""), "path": str("absolute path"), "content": str("file content")}, "sandbox_id", "path", "content"), toolWriteFile,
	},
	{
		"read_file", "Read a text file from a sandbox.",
		schema(props{"sandbox_id": str(""), "path": str("absolute path")}, "sandbox_id", "path"), toolReadFile,
	},
	{
		"list_dir", "List a directory in a sandbox.",
		schema(props{"sandbox_id": str(""), "path": str("absolute path")}, "sandbox_id", "path"), toolListDir,
	},
	{
		"fork", "Clone a sandbox into N independent children carrying its exact memory and disk state.",
		schema(props{"sandbox_id": str(""), "count": integer("children, 1-16")}, "sandbox_id", "count"), toolFork,
	},
	{
		"checkpoint", "Capture a sandbox's full state without stopping it; returns a checkpoint id to branch from.",
		schema(props{"sandbox_id": str(""), "name": str("optional label")}, "sandbox_id"), toolCheckpoint,
	},
	{
		"branch_checkpoint", "Claim a fresh sandbox branched from a checkpoint's exact captured moment.",
		schema(props{"checkpoint_id": str("")}, "checkpoint_id"), toolBranchCheckpoint,
	},
	{"list_checkpoints", "List checkpoints on the node, newest first.", schema(props{}), toolListCheckpoints},
	{"delete_checkpoint", "Delete a checkpoint.", schema(props{"checkpoint_id": str("")}, "checkpoint_id"), toolDeleteCheckpoint},
	{
		"hibernate", "Snapshot and stop a sandbox, freeing its memory; any later tool call wakes it transparently.",
		schema(props{"sandbox_id": str("")}, "sandbox_id"), toolHibernate,
	},
	{
		"promote", "Publish a sandbox's state as a named template; later create_sandbox calls can claim it by name.",
		schema(props{"sandbox_id": str(""), "template_name": str("template name to publish as")}, "sandbox_id", "template_name"), toolPromote,
	},
	{"release", "Release a sandbox; its VM is destroyed.", schema(props{"sandbox_id": str("")}, "sandbox_id"), toolRelease},
	{"node_info", "The node's pool and claim counters.", schema(props{}), toolNodeInfo},
}

// toolSpecs renders the tools/list payload from the single table.
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
		TTLSeconds int    `json:"ttl_seconds"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	var opts []sandbox.Option
	if args.TTLSeconds > 0 {
		opts = append(opts, sandbox.WithTimeout(time.Duration(args.TTLSeconds)*time.Second))
	}
	template := args.Template
	if template == "" {
		template = s.template
	}
	sb, err := s.client.New(ctx, template, opts...)
	if err != nil {
		return "", err
	}
	s.trackBox(sb)
	return jsonText(map[string]any{"sandbox_id": sb.ID, "deadline": sb.Deadline}), nil
}

func toolExec(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		SandboxID string `json:"sandbox_id"`
		Command   string `json:"command"`
		Cwd       string `json:"cwd"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.box(args.SandboxID)
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

func toolWriteFile(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		SandboxID string `json:"sandbox_id"`
		Path      string `json:"path"`
		Content   string `json:"content"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.box(args.SandboxID)
	if err != nil {
		return "", err
	}
	if err := sb.WriteFile(ctx, args.Path, []byte(args.Content), nil); err != nil {
		return "", err
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(args.Content), args.Path), nil
}

func toolReadFile(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.boxArg(raw)
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
	var args struct {
		Path string `json:"path"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.boxArg(raw)
	if err != nil {
		return "", err
	}
	entries, err := sb.ListDir(ctx, args.Path)
	if err != nil {
		return "", err
	}
	return jsonText(entries), nil
}

func toolFork(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		SandboxID string `json:"sandbox_id"`
		Count     int    `json:"count"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.box(args.SandboxID)
	if err != nil {
		return "", err
	}
	children, err := sb.Fork(ctx, args.Count, 0)
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

func toolCheckpoint(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		SandboxID string `json:"sandbox_id"`
		Name      string `json:"name"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.box(args.SandboxID)
	if err != nil {
		return "", err
	}
	ckpt, err := sb.Checkpoint(ctx, args.Name)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.ckpts[ckpt.ID] = ckpt
	s.mu.Unlock()
	return jsonText(map[string]any{"checkpoint_id": ckpt.ID, "name": ckpt.Name}), nil
}

func toolBranchCheckpoint(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	ckpt, err := s.checkpointArg(ctx, raw)
	if err != nil {
		return "", err
	}
	sb, err := ckpt.New(ctx)
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
	ckpt, err := s.checkpointArg(ctx, raw)
	if err != nil {
		return "", err
	}
	if err := ckpt.Delete(ctx); err != nil {
		return "", err
	}
	s.mu.Lock()
	delete(s.ckpts, ckpt.ID)
	s.mu.Unlock()
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

func toolPromote(ctx context.Context, s *server, raw json.RawMessage) (string, error) {
	var args struct {
		SandboxID    string `json:"sandbox_id"`
		TemplateName string `json:"template_name"`
	}
	if err := parse(raw, &args); err != nil {
		return "", err
	}
	sb, err := s.box(args.SandboxID)
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

// boxArg resolves a sandbox_id argument to a live handle; handlers with
// more arguments parse their own struct from the same raw bytes.
func (s *server) boxArg(raw json.RawMessage) (*sandbox.Sandbox, error) {
	var args struct {
		SandboxID string `json:"sandbox_id"`
	}
	if err := parse(raw, &args); err != nil {
		return nil, err
	}
	return s.box(args.SandboxID)
}

// checkpointArg resolves a checkpoint_id argument: a handle minted in this
// session when available, else a listing lookup (checkpoints outlive
// sessions).
func (s *server) checkpointArg(ctx context.Context, raw json.RawMessage) (*sandbox.Checkpoint, error) {
	var args struct {
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := parse(raw, &args); err != nil {
		return nil, err
	}
	s.mu.Lock()
	ckpt, ok := s.ckpts[args.CheckpointID]
	s.mu.Unlock()
	if ok {
		return ckpt, nil
	}
	ckpts, err := s.client.Checkpoints(ctx)
	if err != nil {
		return nil, err
	}
	for _, ck := range ckpts {
		if ck.ID == args.CheckpointID {
			return ck, nil
		}
	}
	return nil, fmt.Errorf("unknown checkpoint %q", args.CheckpointID)
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
