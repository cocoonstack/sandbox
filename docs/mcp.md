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
| `create_sandbox` | claim a microVM and return its id plus deadline; warm claims take milliseconds, nothing renews the deadline |
| `exec` | run a shell command to completion (5-minute cap); returns stdout, stderr and the exit code; a hibernated sandbox wakes transparently |
| `spawn` | start a command detached and return its pid; output goes to a 256 KiB ring buffer that `logs` replays |
| `ps` | list tracked processes (exec, spawn, pty) with state, exit code and start time |
| `logs` | replay a tracked process's last 256 KiB of stdout/stderr (+ exit code once ended) |
| `kill` | signal a tracked process (0 = SIGKILL); an exited process is a no-op success |
| `write_file` / `read_file` / `list_dir` | atomic whole-file write (parent must exist), whole-file text read, one-level directory listing |
| `fork` | clone into N children (1-16) carrying exact memory + disk state, all-or-nothing; the parent keeps running |
| `checkpoint` | capture full state without stopping; returns a `checkpoint_id` that can be branched repeatedly |
| `branch_checkpoint` | claim a fresh sandbox from a checkpoint's captured moment |
| `list_checkpoints` / `delete_checkpoint` | checkpoint lifecycle |
| `hibernate` | snapshot + stop, freeing memory while keeping id, files and processes; wakes on the next tool call |
| `promote` | publish the sandbox as a named template on its node; re-promoting replaces it |
| `release` | destroy the sandbox and its files; releasing twice succeeds |
| `node_info` | warm pools, live claims, drain state, capacity and mesh peers |

Sandbox handles (and their tokens) are held by the server process for the
session. Checkpoints outlive sessions: `branch_checkpoint` accepts any known
id without a listing round-trip. If the connected node does not hold it, the
claim follows a live owner probe and redirect, or heals the checkpoint locally
when peer healing is enabled. `delete_checkpoint` is different: it acts on the
node bound to the handle, so deleting an id recovered in a later session needs
the MCP server pointed at a node that holds it.
