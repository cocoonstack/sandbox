package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

const (
	protocolVersion = "2024-11-05"
	execTimeout     = 5 * time.Minute
	defaultToolTTL  = time.Hour
)

// server owns one sandboxd client and the handles minted over this stdio
// session: MCP tools address sandboxes and checkpoints by id, so the live
// handles (with their tokens) stay here.
type server struct {
	client   *sandbox.Client
	template string

	mu    sync.Mutex
	boxes map[string]*sandbox.Sandbox
	ckpts map[string]*sandbox.Checkpoint
}

func newServer(addr, token, template string) (*server, error) {
	var opts []sandbox.ClientOption
	if token != "" {
		opts = append(opts, sandbox.WithAPIToken(token))
	}
	client, err := sandbox.Connect(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &server{
		client:   client,
		template: template,
		boxes:    map[string]*sandbox.Sandbox{},
		ckpts:    map[string]*sandbox.Checkpoint{},
	}, nil
}

// serve answers newline-delimited JSON-RPC 2.0 over r/w until EOF — the MCP
// stdio transport. Requests are handled strictly in order: sandbox tools are
// stateful, and an agent's tool calls arrive sequentially anyway.
func (s *server) serve(ctx context.Context, r *bufio.Reader, w io.Writer) error {
	defer s.closeBoxes()
	for {
		line, err := r.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		var req rpcRequest
		if unmarshalErr := json.Unmarshal(line, &req); unmarshalErr != nil || req.Method == "" {
			continue
		}
		if req.ID == nil {
			continue // notification (e.g. notifications/initialized)
		}
		resp := s.dispatch(ctx, &req)
		out, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(out, '\n')); err != nil {
			return err
		}
	}
}

func (s *server) dispatch(ctx context.Context, req *rpcRequest) rpcResponse {
	switch req.Method {
	case "initialize":
		return result(req.ID, map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "sandbox-mcp", "version": "0.1.0"},
		})
	case "tools/list":
		return result(req.ID, map[string]any{"tools": toolSpecs()})
	case "tools/call":
		var call struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &call); err != nil {
			return rpcError(req.ID, -32602, "invalid params")
		}
		text, err := s.callTool(ctx, call.Name, call.Arguments)
		if err != nil {
			return result(req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": err.Error()}},
				"isError": true,
			})
		}
		return result(req.ID, map[string]any{
			"content": []map[string]any{{"type": "text", "text": text}},
		})
	case "ping":
		return result(req.ID, map[string]any{})
	default:
		return rpcError(req.ID, -32601, "method not found: "+req.Method)
	}
}

func (s *server) callTool(ctx context.Context, name string, args json.RawMessage) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()
	for _, t := range tools {
		if t.name == name {
			return t.handler(ctx, s, args)
		}
	}
	return "", fmt.Errorf("unknown tool %q", name)
}

func (s *server) box(id string) (*sandbox.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, ok := s.boxes[id]
	if !ok {
		return nil, fmt.Errorf("unknown sandbox %q (never created here, or already released)", id)
	}
	return sb, nil
}

// closeBoxes releases everything this session claimed; the lease would
// otherwise hold the VMs until it expires.
func (s *server) closeBoxes() {
	s.mu.Lock()
	boxes := slices.Collect(maps.Values(s.boxes))
	clear(s.boxes)
	s.mu.Unlock()
	for _, sb := range boxes {
		_ = sb.Close()
	}
}

func (s *server) trackBox(sb *sandbox.Sandbox) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.boxes[sb.ID] = sb
}

func (s *server) dropBox(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.boxes, id)
}

func (s *server) ckpt(id string) (*sandbox.Checkpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ck, ok := s.ckpts[id]
	return ck, ok
}

func (s *server) trackCkpt(ck *sandbox.Checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ckpts[ck.ID] = ck
}

func (s *server) dropCkpt(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ckpts, id)
}

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcErrorBody   `json:"error,omitempty"`
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func result(id json.RawMessage, v any) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Result: v}
}

func rpcError(id json.RawMessage, code int, msg string) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcErrorBody{Code: code, Message: msg}}
}
