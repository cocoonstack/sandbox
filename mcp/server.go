package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
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
	toolTimeout     = 5 * time.Minute
	execOutputCap   = 1 << 20
	// Outlives the node's 5-minute default; nothing renews a tool claim.
	defaultToolTTL = time.Hour
)

var (
	version = "devel"

	replyOpts = json.JoinOptions(jsontext.AllowInvalidUTF8(true), json.Deterministic(true))
)

type rpcRequest struct {
	ID     jsontext.Value `json:"id"`
	Method string         `json:"method"`
	Params jsontext.Value `json:"params"`
}

type rpcResponse struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id"`
	Result  any            `json:"result,omitzero"`
	Error   *rpcErrorBody  `json:"error,omitzero"`
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// server owns one sandboxd client and the live handles (with their tokens) this stdio session minted.
type server struct {
	client   *sandbox.Client
	template string

	mu      sync.Mutex
	boxes   map[string]*sandbox.Sandbox
	ckpts   map[string]*sandbox.Checkpoint
	release sync.Once
}

func newServer(addr, token, template string) (*server, error) {
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
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

// serve answers the MCP stdio transport's JSON-RPC 2.0 lines until EOF, in order, since sandbox tools are stateful.
func (s *server) serve(ctx context.Context, r *bufio.Reader, w io.Writer) error {
	defer s.closeBoxes()
	for {
		line, err := r.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var req rpcRequest
		if unmarshalErr := unmarshalInput(line, &req); unmarshalErr != nil || req.Method == "" {
			continue
		}
		if req.ID == nil {
			continue // notification (e.g. notifications/initialized)
		}
		resp := s.dispatch(ctx, &req)
		out, err := json.Marshal(resp, replyOpts)
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
			"serverInfo":      map[string]any{"name": "sandbox-mcp", "version": version},
		})
	case "tools/list":
		return result(req.ID, map[string]any{"tools": toolSpecs()})
	case "tools/call":
		var call struct {
			Name      string         `json:"name"`
			Arguments jsontext.Value `json:"arguments"`
		}
		if err := unmarshalInput(req.Params, &call); err != nil {
			return rpcError(req.ID, -32602, "invalid params")
		}
		i := slices.IndexFunc(tools, func(t tool) bool { return t.name == call.Name })
		if i < 0 {
			return rpcError(req.ID, -32602, "unknown tool: "+call.Name)
		}
		text, err := s.callTool(ctx, tools[i], call.Arguments)
		if err != nil {
			return result(req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": cmp.Or(text, err.Error())}},
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

func (s *server) callTool(ctx context.Context, t tool, args jsontext.Value) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	return t.handler(ctx, s, args)
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

// closeBoxes releases everything this session claimed and returns once the releases are done; a second caller waits for the first.
func (s *server) closeBoxes() {
	s.release.Do(func() {
		s.mu.Lock()
		boxes := slices.Collect(maps.Values(s.boxes))
		clear(s.boxes)
		s.mu.Unlock()
		var wg sync.WaitGroup
		for _, sb := range boxes {
			wg.Go(func() { _ = sb.Close() })
		}
		wg.Wait()
	})
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

func (s *server) parseAndBox[T sandboxIDer](raw jsontext.Value) (T, *sandbox.Sandbox, error) {
	var args T
	if err := parse(raw, &args); err != nil {
		return args, nil, err
	}
	sb, err := s.box(args.id())
	return args, sb, err
}

func (s *server) boxArg(raw jsontext.Value) (*sandbox.Sandbox, error) {
	_, sb, err := s.parseAndBox[sandboxArg](raw)
	return sb, err
}

// checkpointArg resolves a checkpoint_id argument: a handle minted in this session when available, else a fresh one, because checkpoints outlive sessions.
func (s *server) checkpointArg(raw jsontext.Value) (*sandbox.Checkpoint, error) {
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
		return nil, errors.New("checkpoint_id is required")
	}
	return s.client.Checkpoint(args.CheckpointID), nil
}

func result(id jsontext.Value, v any) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Result: v}
}

func rpcError(id jsontext.Value, code int, msg string) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcErrorBody{Code: code, Message: msg}}
}
