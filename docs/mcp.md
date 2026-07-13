# MCP server

`sandbox-mcp` exposes the sandbox surface as Model Context Protocol tools
over stdio, so MCP clients — Claude Code, Cursor, agent frameworks — drive
real microVM sandboxes with no glue code.

```jsonc
// .mcp.json (Claude Code) / mcp.json (Cursor)
{
  "mcpServers": {
    "sandbox": {
      "command": "/usr/local/bin/sandbox-mcp",
      "args": ["-addr", "10.0.0.5:7777", "-token", "...",
               "-template", "ghcr.io/cocoonstack/sandbox/rt:24.04"]
    }
  }
}
```

Flags fall back to `SANDBOXD_ADDR`, `SANDBOXD_TOKEN`, `SANDBOXD_TEMPLATE`.
Build: `cd mcp && go build -o sandbox-mcp .`

## Tools

| tool | what it does |
|---|---|
| `create_sandbox` | claim a microVM (warm claims are milliseconds); returns `sandbox_id` |
| `exec` | run a shell command; returns stdout/stderr/exit code; a hibernated sandbox wakes transparently |
| `spawn` | start a command detached; returns its pid immediately |
| `ps` | list tracked processes with state and exit codes |
| `logs` | replay a process's buffered stdout/stderr (+ exit code once ended) |
| `kill` | signal a tracked process (default SIGKILL) |
| `write_file` / `read_file` / `list_dir` | text file operations |
| `fork` | clone into N children carrying exact memory + disk state |
| `checkpoint` | capture full state without stopping; returns `checkpoint_id` |
| `branch_checkpoint` | claim a fresh sandbox from a checkpoint's captured moment |
| `list_checkpoints` / `delete_checkpoint` | checkpoint lifecycle |
| `hibernate` | snapshot + stop, freeing memory; wakes on the next tool call |
| `promote` | publish the sandbox as a named template |
| `release` | destroy the sandbox |
| `node_info` | pool and claim counters |

Sandbox handles (and their tokens) are held by the server process for the
session. Checkpoints outlive sessions: `branch_checkpoint` resolves ids it
did not mint against the connected node's listing — on a cluster that
covers the connected node's checkpoints; one created on a redirected
(peer-owned) sandbox in a *previous* session needs a server pointed at that
node.
