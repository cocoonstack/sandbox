# cocoon sandbox

MicroVM sandboxes for AI agents, built on
[cocoon](https://github.com/cocoonstack/cocoon). Warm claims are
sub-millisecond; a pool miss clones from a golden snapshot in tens of
milliseconds; cold boot is ~200ms on bare metal.

```
SDK (Go)                sandboxd (per node)              guest microVM
sandbox.New() ── HTTP ─► claim: warm pool / golden clone  CH (egress) | FC (none)
sb.Exec/Files/… ─ HTTP upgrade ─► byte relay ── vsock ──► silkd :2048
                        memberlist mesh: warm-count gossip,
                        MOVED-style redirect to the owning node
```

Two network lanes, derived from the claim: `net=none` → Firecracker, no NIC,
vsock-only (hardened default); `net=egress` → Cloud Hypervisor with a
bridge/CNI NIC. Backends are never user-selected.

## Guides

- [Deploying sandboxd](deploy.md) — single node, configuration reference,
  images, running as a service
- [Clusters](cluster.md) — joining a mesh, querying members, how redirect
  placement works, relocating handles
- [sandboxd HTTP API](sandboxd-api.md) — claim/release/relay endpoints and
  wire types
- [Go SDK](sdk.md) — connecting (single node and clusters), every option,
  the full sandbox surface, error handling
- [Python SDK](sdk-python.md) — the same surface for the Python-first
  agent ecosystem, stdlib-only
- [MCP server](mcp.md) — sandboxes as Model Context Protocol tools for
  Claude Code, Cursor, and agent frameworks
- [silkd](silkd.md) — the in-guest daemon: protocol, verb reference, lanes
  and limits
- [Performance](performance.md) — measured latencies and the environments
  they were measured in

## Repository

Source, issue tracker and design docs:
[github.com/cocoonstack/sandbox](https://github.com/cocoonstack/sandbox).
Deep-dive design documents live in
[cocoon-specs/design](https://github.com/cocoonstack/cocoon-specs/tree/main/design)
(`sandbox-fast-boot.md`, `sandbox-control-plane.md`, `sandbox-silkd.md`).
