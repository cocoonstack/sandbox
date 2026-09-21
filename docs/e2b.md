# e2b sandboxes

The `e2b-rt` flavor carries [e2b](https://e2b.dev)'s `envd` alongside silkd, so
an **unmodified e2b SDK** can drive a cocoon microVM. It is the guest half of a
compatibility layer whose other two halves live in
[sandbox-operator](https://github.com/cocoonstack/sandbox-operator): the
apiserver's e2b REST surface answers `Sandbox.create()`, and `envd-proxy`
carries everything after it.

Nothing is translated and nothing is replaced. silkd stays the product daemon —
the native Go and Python SDKs, MCP, preview, the LSP broker and the egress lane
all speak to it. `envd` is an adapter for clients that already exist.

## The path in

```
unmodified e2b SDK
   │  {port}-{sandboxID}.{domain}
   ▼
envd-proxy                        ← off-node, resolves the sandbox and its token
   │  GET /v1/sandboxes/{id}/ports/49983   (Upgrade: tcp)
   ▼
sandboxd                          ← verifies the sandbox's own token
   │  vsock → silkd port_forward
   ▼
guest 127.0.0.1:49983             ← envd
```

A sandbox on the `none` lane has **no NIC**, so this relay is the only way in —
the same property that makes the lane the security default. No client learns a
node address, and the guest is never given a listener the network can reach.

See [sandboxd-api](sandboxd-api.md#get-v1sandboxesidportsport) for the relay
endpoint, and `envd-proxy`'s own docs for the edge.

## Running a pool

`envd` starts under systemd at boot, but a warm clone must not be handed out
before it answers. The pool's existing `warmup` is the gate — a clone whose
warmup fails is destroyed and replaced:

```json
{"template": "ghcr.io/cocoonstack/sandbox/e2b-rt:24.04",
 "net": "none", "size": "small", "warm": 4,
 "warmup": ["sh", "-c",
   "for i in $(seq 1 200); do curl -sf -o /dev/null http://127.0.0.1:49983/health && exit 0; sleep 0.05; done; exit 1"]}
```

The apiserver's `--e2b-envd-version` must name the version this image ships
(`/etc/envd-version` records it). The SDK version-compares that value and
**kills the sandbox** when it cannot parse one, so reporting a floor instead of
the truth is not a safe default.

## What the flavor does

- Installs a pinned `envd` release, verified by SHA-256 at build time.
- Writes `/etc/envd-version` so the deployment can read back what it shipped.
- Enables `envd.service`, ordered after a guard unit, alongside `silkd.service`.
- **amd64 only**: e2b publishes no arm64 build of `envd`.

`envd` hardcodes a `0.0.0.0` listener and takes no bind address, so the image
drops inbound traffic to 49983 from anything but loopback (`envd-guard.service`,
one nftables rule). On the `none` lane there is no other interface for it to
have bound; the rule is what holds on the egress lane, whose NIC is real.

## Limits worth knowing

- **`envd` serves HTTP/1.1 only** in the clear as of 0.8.0 — it installs no h2c
  handler. The relay is protocol-blind, so this is a property of the daemon, not
  of the path; `envd-proxy` matches it and has a flag for the day it changes.
- **Pause is not `envd`'s.** Its `/freeze`, `/init` and siblings are the
  orchestrator control plane e2b runs on Firecracker; cocoon hibernates the
  whole VM instead, through `POST /v1/sandboxes/{id}/hibernate`. The edge
  refuses those paths.
- **`envd` also runs a port forwarder** that tries to republish guest listeners
  on `eth0`. There is no `eth0` on the `none` lane, so it finds nothing to do.

## Proving it

`scripts/envd-e2e.sh` runs the guest half on a node: it builds the pool with the
warmup gate above and then drives `envdsmoke`, which claims a sandbox and
reaches the real `envd` through the relay — health, `GET`/`POST /files`, and a
ConnectRPC unary — while asserting silkd still answers on the same VM.

```bash
K=<kit> TEMPLATE=ghcr.io/cocoonstack/sandbox/e2b-rt:24.04 bash scripts/envd-e2e.sh
```

`envdsmoke -hold` then keeps that sandbox alive for sandbox-operator's
`envdproxysmoke -guest envd`, which puts the real proxy in front of it.
