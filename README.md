# sandbox

MicroVM sandboxes for AI agents, built on
[cocoon](https://github.com/cocoonstack/cocoon): a fast-boot guest stack, an
in-guest product daemon, a per-node control plane with warm pools, and SDKs
for Go and Python. Warm claims are sub-millisecond; a pool miss clones from a
golden snapshot in tens of milliseconds; cold boot is ~0.2–0.4 s on bare metal.

**Documentation: [cocoonstack.github.io/sandbox](https://cocoonstack.github.io/sandbox/)**
(source in [`docs/`](docs/)).

## Architecture

```
SDK (Go/Python)         sandboxd (per node)              guest microVM
client.New/new ─ HTTP ─► claim: warm pool / golden clone   Cloud Hypervisor
exec/files/… ─ HTTP upgrade (kept) ─► byte relay ─ vsock ─► silkd :2048
                        memberlist mesh: warm-count gossip,
                        MOVED-style redirect to the owning node
```

Cloud Hypervisor serves both network lanes. `net=none` has no NIC and uses
vsock-only I/O (hardened default); `net=egress` attaches a bridge/CNI NIC.

## Quick start

On a Linux node with KVM and [cocoon](https://github.com/cocoonstack/cocoon)
v0.6.9 or newer, as root, from a checkout with Go installed:

```bash
make sandboxd
cocoon image pull ghcr.io/cocoonstack/sandbox/rt:24.04

cat >config.json <<'EOF'
{
  "listen": "127.0.0.1:7777",
  "api_token": "change-me",
  "pools": [
    {"template": "ghcr.io/cocoonstack/sandbox/rt:24.04", "net": "none", "size": "small", "warm": 2}
  ]
}
EOF
dist/sandboxd -config config.json &

# Wait for the golden snapshot and the warm pool.
until curl -sf -H 'Authorization: Bearer change-me' http://127.0.0.1:7777/v1/info |
  jq -e '(.pools | length) > 0 and all(.pools[]; .golden and .warm >= .target)' >/dev/null; do sleep 1; done

# Claim, exec and release three times; prints each latency.
(cd e2e && GOWORK=off go run ./cmd/demo -addr 127.0.0.1:7777 -token change-me \
  -template ghcr.io/cocoonstack/sandbox/rt:24.04)
```

`demo` is a small program over the [Go SDK](docs/sdk.md); the
[Python SDK](docs/sdk-python.md) has the same calls. [Deploying
sandboxd](docs/deploy.md) covers release binaries, the full config and systemd.

## Related projects

- [cocoon](https://github.com/cocoonstack/cocoon) — the microVM engine underneath
- [vk-sandbox](https://github.com/cocoonstack/vk-sandbox) — the Kubernetes provider that claims from sandboxd
- [sandbox-operator](https://github.com/cocoonstack/sandbox-operator) — the Kubernetes apiserver and e2b-compatible data plane over warm pools

## Layout

- `silkd/` — in-guest product daemon (Rust), baked into the base image;
  newline-JSON frames over vsock 2048 ([reference](docs/silkd.md))
- `sandboxd/` — per-node control plane (Go): warm pools, the claim API and the
  relays to silkd ([deploy](docs/deploy.md), [API](docs/sandboxd-api.md))
- `sdk/go/` — [Go SDK](docs/sdk.md); `silkd/` is its connection layer and
  `silkd/silkdtest/` a test fake
- `sdk/python/` — [Python SDK](docs/sdk-python.md), stdlib-only and synchronous
- `mcp/` — `sandbox-mcp`, an [MCP](docs/mcp.md) stdio server
- `sdk/openai/` — `cocoonstack-sandbox-openai`, a sandbox provider for the
  [OpenAI Agents SDK](docs/openai-adapter.md)
- `sdk/langchain/` — `cocoonstack-sandbox-langchain`, a
  [LangChain toolkit](docs/langchain.md)
- `protocol/wire/` — the frame vocabulary and `fixtures/`, the golden corpus the
  Rust, Go and Python tests all round-trip, so wire drift fails CI
- `e2e/` — in-process full-stack tests (real pool/engine/relay/SDK, fake
  cocoon + guest) and the [bare-metal drivers](docs/deploy.md#acceptance-drivers)
- `boot/kernel/` — kernel version pin (`VERSION` + matching tarball `SHA256`,
  bump both together) and config fragments
- `boot/init/` — `sandbox-init`, the entire initramfs userland (static Rust)
- `boot/Dockerfile` — multi-stage: kernel → init → cpio → scratch image with
  `/boot/vmlinuz-sandbox` + `/boot/initrd.img-sandbox`
- `os-image/` — VM images consuming the boot artifact: `base`, `rt`, `python`,
  `python-rt`, `node`, `node-rt`, `browser`, `android`, `e2b-rt`, `e2b-ci`
- `scripts/` — benchmarks and bare-metal e2e, see
  [Verifying a node](docs/deploy.md#verifying-a-node)
- `packaging/` — the systemd unit deploy installs

## CI

- `silkd.yml` / `sandboxd.yml` — Rust and Go test+lint suites
- `boot-init.yml` — the boot/init crate's own fmt+clippy+test gate
- `python.yml` — ruff + pytest for the three Python packages, mypy for the
  base SDK
- `shell.yml` — shellcheck over every tracked shell script
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
  → guest kernel (amd64: PVH ELF vmlinux, everything =y, no decompress stage; arm64: flat Image)
  → uncompressed ~1.5MB cpio: /init = sandbox-init (static Rust)
  → resolve virtio-blk serials via sysfs (2ms poll, no udev)
  → mount EROFS layers → overlayfs + ext4 COW → switch_root
  → exec /sbin/init  (systemd, trimmed; cocoon-agent + silkd start at sysinit)
```

The cmdline keys `sandbox-init` consumes are in the
[boot contract](docs/deploy.md#boot-contract).

## Development

```bash
make help          # every target; the ones below are the daily set
make lint test     # Rust: boot/init + silkd (fmt --check, clippy -D warnings, tests)
make go-lint       # Go: protocol/wire + sandboxd + sdk/go + e2e + mcp, GOOS linux AND darwin
make go-test       # Go: go test -race across the Go modules
make sh-lint       # shellcheck every tracked shell script
make sandboxd      # build dist/sandboxd
make bench         # claim-tier + data-plane benchmarks on this node
make boot          # kernel + initramfs artifact image (docker)
                   #   KERNEL_MIRROR=… if kernel.org tarball paths 404 locally
make silkd-image   # silkd release binary in a scratch carrier image
make images        # base + python images against the local boot + silkd images
```

The parent workspace's `go.work` includes these modules, which is why the
Makefile forces `GOWORK=off`: CI has no workspace, and a workspace build
resolves sibling checkouts instead of the pinned versions. silkd's integration
tests spawn real processes — run them in a Linux container too (`docker run
rust:1 … cargo test`) before touching platform-sensitive paths; macOS green
alone has hidden Linux-only breakage before.

## License

The server stack — sandboxd, silkd, the boot chain, the OS images, and the
MCP server — is a product, and is licensed under [AGPL-3.0](LICENSE). The client SDKs
(`sdk/go`, `sdk/python`, `sdk/openai`, `sdk/langchain`) and the
`protocol/wire` module the Go SDK requires are licensed under Apache-2.0
(see the `LICENSE` file in each directory) and carry no copyleft obligation.
