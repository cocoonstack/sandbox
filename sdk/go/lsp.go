package sandbox

import (
	"context"
	"errors"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

// Lsp is a language server in the sandbox; silkd pipes JSON-RPC bytes and the caller frames them.
type Lsp struct {
	ServerID string

	s *Sandbox
}

// Request opens the server's one JSON-RPC byte stream; closing it reaps the server.
func (l *Lsp) Request(ctx context.Context) (*PortConn, error) {
	return l.s.openStream(ctx, &wire.LspRequest{ServerID: l.ServerID})
}

// Stop kills the language server.
func (l *Lsp) Stop(ctx context.Context) error {
	return l.s.doneRPC(ctx, &wire.LspStop{ServerID: l.ServerID})
}

// StartLsp spawns the flavor image's language server rooted at root; an image without one fails with not_found.
func (s *Sandbox) StartLsp(ctx context.Context, language, root string) (*Lsp, error) {
	started, err := s.oneShotRPC[wire.LspStarted](ctx, &wire.LspStart{Language: language, Root: root})
	if err != nil {
		return nil, err
	}
	if started.ServerID == "" {
		return nil, errors.New("lsp start: empty server id")
	}
	return &Lsp{ServerID: started.ServerID, s: s}, nil
}
