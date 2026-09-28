# e2b sandboxes

The `e2b-rt` flavor carries [e2b](https://e2b.dev)'s `envd` alongside silkd, so
an **unmodified e2b SDK** can drive a cocoon microVM; `e2b-ci` adds e2b's code
interpreter on top, for `@e2b/code-interpreter` and `e2b_code_interpreter`. It is the guest half of a
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
endpoint. The edge half lives in sandbox-operator and is not released yet.

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

The image records the version it ships in `/etc/envd-version`. Whatever serves
the e2b control plane must report that value: the SDK version-compares it and
**kills the sandbox** when it cannot parse one, so a floor is not a safe stand-in
for the truth.

## What the flavor does

- Installs a pinned `envd` release, verified by SHA-256 at build time.
- Writes `/etc/envd-version` so the deployment can read back what it shipped.
- Enables `envd.service`, ordered after a guard unit, alongside `silkd.service`.
- Ships the SDK's default account: `user`, home `/home/user`, passwordless
  sudo. `envd` itself defaults to root until its `/init` names `user`.
- Runs `envd` in Firecracker mode, which reads its MMDS from
  `169.254.169.254`. The guard unit aliases that address on `lo`, so silkd
  serves the sandbox's
  [instance-metadata document](sandboxd-api.md#put-v1sandboxesidinstance-metadata)
  there, `{}` until the operator sets one. `envd`'s `/init` checks a token
  against the document's `accessTokenHash`, so a forked child can take a new
  token; with no hash, the first `/init` sets the token. Another guard rule
  drops traffic to the alias from anywhere but `lo`. A document without a log
  `address` leaves `envd`'s HTTP log exporter unstarted; it holds at most 8 MiB
  of its own lines.
- **amd64 only**: e2b publishes no arm64 build of `envd`.

## The code-interpreter flavor

`e2b-ci` is built from e2b's own recipe, `e2b-dev/code-interpreter`
`template/` at the `CI_RECIPE_SHA` the Dockerfile pins: the build fetches that
commit's archive, checks its sha256, and applies the one local change listed in
`os-image/e2b-ci/PATCHES`; e2b's license ships in the image under
`/usr/share/doc/e2b-code-interpreter/`. Moving to a newer recipe is a bump of the
two `CI_RECIPE_*` args plus a refresh of the constraints files. On top of `e2b-rt`'s contents it runs a Jupyter server on
loopback 8888 and the code-interpreter API on 49999, both enabled at boot, and
the guard drops off-guest traffic to 49983, 49999 and 8888. Only the python
kernel ships: the recipe installs its javascript, R and Java kernels from
sources that cannot be pinned (a `curl | bash` setup script, an unversioned git
dependency, moving release archives), so they wait for a reproducible source.

Every pip dependency is pinned by `constraints.txt` and `server-constraints.txt`
beside the Dockerfile, and bytecode is compiled hash-checked at the end, so a
rebuild of one commit against the same base yields the same layer. That holds
for the OCI and registry exporters with `SOURCE_DATE_EPOCH` and
`rewrite-timestamp=true`, as the image workflow builds; the `docker` exporter
ignores `rewrite-timestamp`, so a local reproducibility check exports OCI.

The pool's warmup waits on both daemons:

```json
{"template": "ghcr.io/cocoonstack/sandbox/e2b-ci:24.04",
 "net": "none", "size": "medium", "warm": 2,
 "warmup": ["sh", "-c",
   "for i in $(seq 1 1200); do curl -sf -m 1 -o /dev/null http://127.0.0.1:49983/health && curl -sf -m 1 -o /dev/null http://127.0.0.1:49999/health && exit 0; sleep 0.05; done; exit 1"]}
```

`medium` (2 vCPU, 1 GiB) is e2b's own size for the interpreter. `small` runs
it too, with about 90 MiB left once the server and its default kernel are up,
which a heavier import (pandas, matplotlib) quickly spends.

`envd` hardcodes a `0.0.0.0` listener and takes no bind address, so the image
drops inbound traffic to 49983 from anything but loopback (`envd-guard.service`,
one nftables rule). On the `none` lane there is no other interface for it to
have bound. On the egress lane the NIC is real, but sandboxd locks the tap
first — a netdev ingress chain that passes only broadcast DHCP — so nothing
off the guest reaches it anyway. The guard is the layer underneath, for the
window where that lock is absent.

## Limits worth knowing

- **`envd` serves HTTP/1.1 only** in the clear as of 0.8.0 — it installs no h2c
  handler. The relay is protocol-blind, so this is a property of the daemon, not
  of the path; whatever proxies to it has to match, not assume.
- **Pause is not `envd`'s.** Its `/freeze`, `/init` and siblings are the
  orchestrator control plane e2b runs on Firecracker; cocoon hibernates the
  whole VM instead, through `POST /v1/sandboxes/{id}/hibernate`. The edge
  refuses those paths.
- **Half-close works in both directions** through the relay: either peer can
  stop writing and keep reading (see
  [sandboxd-api](sandboxd-api.md#get-v1sandboxesidportsport)).
- **`envd` also runs a port forwarder** that tries to republish guest listeners
  on `eth0`. There is no `eth0` on the `none` lane, so it finds nothing to do.

## Proving it

The flavor's daemon is proved from the consumer's side: sandbox-operator's
`scripts/envd-e2e.sh` builds the pool with the warmup gate above and drives
`test/envdsmoke`, which claims a sandbox and reaches the real `envd` through
the relay (health, the version the compat API reports, files, ConnectRPC, the
`user` account, HTTP/1.1 only, and the code-interpreter API with
`CODE_INTERPRETER=1`). sandboxd itself knows nothing of `envd`: the pool's
warmup is a shell command like any other, and the relay carries bytes.

The suite does **not** cover the guard's drop. Reaching 49983 from off the
guest means lifting sandboxd's tap lock, which no e2e run should do. Measured
by hand instead, on one egress claim: with the lock on, neither 49983 nor an
unguarded control port answered and ARP never resolved; with it lifted, the
control port answered `204` while 49983 timed out, and the relay answered
`101` throughout.
