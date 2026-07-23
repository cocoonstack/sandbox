# cocoon sandbox

MicroVM sandboxes for AI agents, built on
[cocoon](https://github.com/cocoonstack/cocoon). Warm claims are
sub-millisecond; a pool miss clones from a golden snapshot in tens of
milliseconds; cold boot is ~200ms on bare metal.

```
SDK (Go)                sandboxd (per node)              guest microVM
sandbox.New() ── HTTP ─► claim: warm pool / golden clone  Cloud Hypervisor
sb.Exec/Files/… ─ HTTP upgrade ─► byte relay ── vsock ──► silkd :2048
                        memberlist mesh: warm-count gossip,
                        MOVED-style redirect to the owning node
```

Cloud Hypervisor serves both network lanes. `net=none` has no NIC and uses
vsock-only I/O (hardened default); `net=egress` attaches a bridge/CNI NIC.

## Guides

- [Deploying sandboxd](deploy.md) — single node, configuration reference,
  images, running as a service
- [Clusters](cluster.md) — joining a mesh, querying members, how redirect
  placement works, relocating handles
- [sandboxd HTTP API](sandboxd-api.md) — every endpoint: claim/release,
  hibernate, fork, promote, checkpoints, preview, online pool reconfigure,
  metrics, the usage journal
- [Go SDK](sdk.md) — connecting (single node and clusters), every option,
  the full sandbox surface, error handling
- [Python SDK](sdk-python.md) — the same surface for the Python-first
  agent ecosystem, stdlib-only
- [LangChain toolkit](langchain.md) — sandbox tools for LangChain/LangGraph
  agents, with checkpoint branching
- [MCP server](mcp.md) — sandboxes as Model Context Protocol tools for
  Claude Code, Cursor, and agent frameworks
- [OpenAI Agents SDK adapter](openai-adapter.md) — run Agents SDK tools
  inside cocoon microVMs via the custom sandbox-provider interface
- [Android sandboxes](android.md) — the redroid flavor: claim shape, adb
  access through the relay or the network, checkpoint/branch
- [Browser sandboxes](browser.md) — headless Chromium with CDP through
  the relay: Playwright/Puppeteer access, checkpoint/branch of a live
  browser
- [Guarded egress](egress.md) — allow-listed, audited outbound access with
  host-side credential injection, on both lanes: no NIC (none) or an
  nftables-locked NIC (egress)
- [Security model](security.md) — trust boundaries, what a compromised
  guest can reach, the deployment assumptions the guarantees rest on, and
  the honest limitations
- [silkd](silkd.md) — the in-guest daemon: protocol, verb reference, lanes
  and limits
- [Performance](performance.md) — measured latencies and the environments
  they were measured in
- [Benchmarks](benchmarks.md) — what each number measures, `make bench`
  reproduction, and the dated results log

## Repository

Source and issue tracker:
[github.com/cocoonstack/sandbox](https://github.com/cocoonstack/sandbox).
