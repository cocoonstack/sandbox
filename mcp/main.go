// sandbox-mcp is a Model Context Protocol server over stdio: it exposes the
// sandbox surface (claim, exec, files, fork, checkpoint, promote, hibernate)
// as MCP tools, so MCP clients — Claude Code, Cursor, agent frameworks —
// drive real microVM sandboxes with no extra glue. One process serves one
// sandboxd endpoint, configured by flags or environment.
package main

import (
	"bufio"
	"cmp"
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/projecteru2/core/log"
	coretypes "github.com/projecteru2/core/types"
)

func main() {
	ctx := context.Background()
	if err := log.SetupLog(ctx, &coretypes.ServerLogConfig{Level: "error"}, ""); err != nil {
		fmt.Fprintln(os.Stderr, "setup log:", err)
		os.Exit(1)
	}
	addr := flag.String("addr", cmp.Or(os.Getenv("SANDBOXD_ADDR"), "127.0.0.1:7777"), "sandboxd address")
	token := flag.String("token", os.Getenv("SANDBOXD_TOKEN"), "node api token")
	template := flag.String("template", cmp.Or(os.Getenv("SANDBOXD_TEMPLATE"), "rt:24.04"), "default template ref")
	flag.Parse()

	srv, err := newServer(*addr, *token, *template)
	if err != nil {
		log.WithFunc("main").Fatalf(ctx, err, "connect sandboxd")
	}
	if err := srv.serve(ctx, bufio.NewReader(os.Stdin), os.Stdout); err != nil {
		log.WithFunc("main").Fatalf(ctx, err, "serve stdio")
	}
}
