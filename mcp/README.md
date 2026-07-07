# sandbox-mcp

An MCP stdio server (JSON-RPC 2.0) exposing the sandbox surface as tools
for Claude Code, Cursor, and other MCP clients — create/exec/file ops/
fork/checkpoint/branch/hibernate/promote/release/info, built on the Go SDK.

```bash
sandbox-mcp -addr 10.0.0.5:7777 -token "$SANDBOXD_TOKEN" -template rt:24.04
```

Flags fall back to `SANDBOXD_ADDR` / `SANDBOXD_TOKEN` / `SANDBOXD_TEMPLATE`.
Tool reference: [docs/mcp.md](../docs/mcp.md).
