package sandbox

import (
	"context"
	"fmt"

	"github.com/cocoonstack/sandbox/sdk/go/silkd"
)

// Lsp is a language server running in the sandbox, spoken to over the relay.
// silkd is a broker: it spawns the flavor image's server and pipes JSON-RPC
// bytes; the caller frames and correlates (agents already speak LSP).
type Lsp struct {
	ServerID string

	s *Sandbox
}

// Request opens the JSON-RPC byte stream to the language server: the returned
// PortConn writes go to the server's stdin, reads come from its stdout. A
// server serves one Request for its lifetime — closing the stream ends the
// session and reaps the server (start a new one to work again).
func (l *Lsp) Request(ctx context.Context) (*PortConn, error) {
	return l.s.openStream(ctx, &silkd.LspRequest{ServerID: l.ServerID})
}

// Stop kills the language server.
func (l *Lsp) Stop(ctx context.Context) error {
	return l.s.doneRPC(ctx, &silkd.LspStop{ServerID: l.ServerID})
}

// StartLsp spawns the language server the flavor image provides for language
// (rooted at root), returning a handle. On the base image, which ships no
// language servers, this fails with silkd's typed not_found.
func (s *Sandbox) StartLsp(ctx context.Context, language, root string) (*Lsp, error) {
	conn, done, err := s.call(ctx, &silkd.LspStart{Language: language, Root: root})
	if err != nil {
		return nil, err
	}
	defer done()
	started, err := expect[silkd.LspStarted](ctx, conn)
	if err != nil {
		return nil, err
	}
	if started.ServerID == "" {
		return nil, fmt.Errorf("lsp start: empty server id")
	}
	return &Lsp{ServerID: started.ServerID, s: s}, nil
}
