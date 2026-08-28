# sandbox

MicroVM sandboxes for AI agents, built on
[cocoon](https://github.com/cocoonstack/cocoon): a fast-boot guest stack, an
in-guest product daemon, a per-node control plane with warm pools, and a Go
SDK. Warm claims are sub-millisecond; a pool miss clones from a golden
snapshot in tens of milliseconds; cold boot is ~200ms on bare metal.

```
SDK (Go)                sandboxd (per node)              guest microVM
sandbox.New() ── HTTP ─► claim: warm pool / golden clone  Cloud Hypervisor
sb.Exec/Files/… ─ HTTP upgrade ─► byte relay ── vsock ──► silkd :2048
                        memberlist mesh: warm-count gossip,
                        MOVED-style redirect to the owning node
```

Cloud Hypervisor serves both network lanes. `net=none` has no NIC and uses
vsock-only I/O (hardened default); `net=egress` attaches a bridge/CNI NIC.

**Documentation**: [cocoonstack.github.io/sandbox](https://cocoonstack.github.io/sandbox/)
(deployment, clusters, HTTP API, Go + Python SDK references, the MCP server,
the OpenAI Agents SDK and LangChain adapters, silkd protocol,
performance) — source in
[`docs/`](docs/).

## Layout

- `silkd/` — in-guest product daemon (Rust, tokio): exec with context,
  persistent shell sessions, streaming fs, tar-stream tree push/pull,
  find/replace, watch (ready-acked), pty, structured git, guest port
  relay (`port_forward`), and an LSP broker for flavor-shipped language
  servers — newline-JSON frames over vsock 2048, one connection per RPC;
  baked into the base image
- `sandboxd/` — per-node control plane (Go): warm pools refilled from golden
  snapshot exports (online-retunable), claim/release/hibernate/fork/promote/
  checkpoint HTTP API, operator-catalog dataset volumes (read-only or
  writable), signed preview URLs, the HTTP-upgrade byte relay to silkd,
  usage + audit journals, /metrics, reap + restart reconcile, memberlist
  mesh with redirect placement
- `sdk/go/` — Go SDK (stdlib-only): `Connect/New/Lookup`, `Exec/Run`, files,
  `Push/Pull`, sessions, `Find/Replace`, `Watch`, git verbs, `OpenPty`,
  `Fork/Hibernate/Promote/Checkpoint`, `DialPort/ProxyPort/PreviewURL`,
  `StartLsp`, `Spawn/Ps/Kill/Logs/Attach`; `protocol/wire` carries the frame
  vocabulary, `sdk/go/silkd` the conn layer, `silkdtest` a test fake
- `sdk/python/` — Python SDK (stdlib-only, sync), matching the Go guest and
  data-plane surface for the Python-first agent ecosystem; node pool retuning
  remains Go-only; round-trips the shared fixture corpus
- `mcp/` — `sandbox-mcp`, an MCP stdio server exposing the surface as tools
  for Claude Code / Cursor / agent frameworks
- `sdk/openai/` — `cocoonstack-sandbox-openai`, a custom sandbox provider
  for the OpenAI Agents SDK (Python, over the Python SDK)
- `sdk/langchain/` — `cocoonstack-sandbox-langchain`, a LangChain toolkit
  (StructuredTools over the Python SDK, checkpoint branching)
- `protocol/wire/fixtures/` — golden frame corpus; the Rust, Go, and Python
  protocol tests all round-trip it, so wire drift fails CI
- `e2e/` — in-process full-stack tests (real pool/engine/relay/SDK, fake
  cocoon+guest) plus bare-metal acceptance drivers under `cmd/`: `demo`,
  `smoke`, `meshsmoke`, `crossnode`, `coldproof`, `egresssmoke`,
  `interceptsmoke`, `volumesmoke`, `lifecycle` (idle→hibernate→archive),
  `androidsmoke`, `browsersmoke`, and the `pullbench`/`pushbench`/`rpcbench`
  perf drivers
- `boot/kernel/` — kernel version pin (`VERSION` + matching tarball `SHA256`,
  bump both together) + config fragment (amd64: over `x86_64_defconfig` +
  `kvm_guest.config`; arm64: over `defconfig` + `sandbox-arm64.config`)
- `boot/init/` — `sandbox-init`, the entire initramfs userland (Rust, static
  musl build)
- `boot/Dockerfile` — multi-stage: kernel → init → cpio → scratch image with
  `/boot/vmlinuz-sandbox` + `/boot/initrd.img-sandbox`
- `os-image/` — VM images consuming the boot artifact: `base` (layered,
  for builds), `rt` (base squashed to one layer — the default template in
  examples), `python`, `python-rt`, `browser`, and `android`
- `scripts/` — `boot-bench.sh` (boot phase timing), `bench.sh` (the published
  benchmark procedure), `sandboxd-e2e.sh` (bare-metal e2e, below), plus the
  `archive`/`egress`/`intercept` e2e drivers

## Build & test

```bash
make help          # this list
make lint test     # Rust: boot/init + silkd (fmt --check, clippy -D warnings, tests)
make go-lint       # Go: sandboxd + sdk/go + e2e + mcp, GOOS linux AND darwin
make go-test       # Go: go test -race across the Go modules
make sandboxd      # build dist/sandboxd
make boot          # kernel + initramfs artifact image (docker)
                   #   KERNEL_MIRROR=… if kernel.org tarball paths 404 locally
make silkd-image   # silkd release binary in a scratch carrier image
make images        # base + python images against the local boot + silkd images
```

The parent workspace's `go.work` excludes these modules; the Makefile forces
`GOWORK=off` so local runs match CI. silkd's integration tests spawn real
processes — run them in a Linux container too (`docker run rust:1 … cargo
test`) before touching platform-sensitive paths; macOS green alone has hidden
Linux-only breakage before.

## Bare-metal e2e

`scripts/sandboxd-e2e.sh` drives the real stack on a node with cocoon and a
silkd-baked template image: golden build → warm pool → claim tiers → the
full v2-verb smoke (files/session/find/replace/watch/git/pty) → reap →
restart reconcile.

```bash
TEMPLATE=rt:24.04 scripts/sandboxd-e2e.sh
# BRIDGE=br0 adds an egress pool and the egress lane-detect check; a plain
#   `ip link add br0 type bridge` with no uplink is enough (NIC, not network).
# SANDBOXD_BIN/DEMO_BIN/SMOKE_BIN point at prebuilt binaries for nodes
#   without a Go toolchain.
# VOLUME_IMAGE=/absolute/dataset.img enables the volumes proof (ro sharing
#   + rw claim/release); the image contains volume-e2e.txt. Prebuilt runs
#   also set VOLUME_SMOKE_BIN.
```

## CI

- `silkd.yml` / `sandboxd.yml` — Rust and Go test+lint suites
- `python.yml` — ruff + pytest for the three Python packages
- `images.yml` — the single image entry point: on a push touching
  `boot/**`, `silkd/**`, `protocol/**`, or `os-image/**` it builds the
  changed carriers (via `build-boot.yml` / `build-silkd.yml`,
  `workflow_call`) then os-images, in order, so the chain is deterministic
- `build-os-images.yml` — bakes base + flavors FROM the sha-pinned carriers
- `release.yml` — on a version tag, builds the release binaries and archives
- `publish-pypi.yml` — on an `sdk-*-v*` tag, builds and publishes the
  matching package via PyPI Trusted Publishing (OIDC, per-package
  environment)

On a fresh repo run build-boot first — images build `FROM` the boot artifact.

## Boot chain

```
cloud-hypervisor
  → vmlinux (PVH ELF, everything =y, no decompress stage)
  → uncompressed ~1.5MB cpio: /init = sandbox-init (static Rust)
  → resolve virtio-blk serials via sysfs (2ms poll, no udev)
  → mount EROFS layers → overlayfs + ext4 COW → switch_root
  → exec /sbin/init  (systemd, trimmed; cocoon-agent + silkd start at sysinit)
```

Boot contract (cmdline keys consumed by sandbox-init):

| cmdline key | meaning |
|---|---|
| `cocoon.layers=a,b,…` | EROFS layer disks resolved from virtio-blk serials, lowerdir order |
| `cocoon.cow=x` | writable ext4 COW disk (same resolution rules) |
| `cocoon.timeout=10` | per-disk wait budget, seconds |
| `cocoon.hostname=h` | set via `sethostname(2)` before handoff |
| `ip=addr::gw:mask:host:ethN:off[:dns0[:dns1]]` | cocoon CNI static config: persisted as a MAC-matched networkd unit in the new root (not applied in the initramfs); absent → the image's DHCP fallback covers the NIC |
| `sandbox.init=/path` | handoff target, default `/sbin/init` |
| `sandbox.debug=1` | fatal errors drop to `/bin/sh` (debug initramfs) instead of poweroff |
| `sandbox.trace=1` | emit one pre-handoff line with per-phase µs timings |

`boot=cocoon-overlay` is ignored. Everything cocoon passes today keeps
working — images built here boot with an unmodified cocoon.

## License

The server stack — sandboxd, silkd, the boot chain, the OS images, and the
MCP server — is licensed under [AGPL-3.0](LICENSE). The client SDKs
(`sdk/go`, `sdk/python`, `sdk/openai`, `sdk/langchain`) are licensed under
Apache-2.0 (see the `LICENSE` file in each directory), so embedding a
client in a proprietary agent stack carries no copyleft obligation.
