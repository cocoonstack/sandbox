# Benchmarks

Every number this project publishes is reproducible with one command on
your own hardware. This page defines exactly what each number measures —
because in this market most published "cold start" figures do not measure
a cold start — and hosts the dated results log. The analysis behind the
numbers (backend asymmetry, boot anatomy, hibernate costs) lives in
[Performance](performance.md).

## The three claim tiers

All tiers measure the same wall clock: from the SDK issuing
`POST /v1/claim` to the sandbox handle returning. A claim returns only
when the in-guest agent (silkd) has answered a readiness round-trip — the
sandbox can run a command the microsecond the call returns. "Process
created" or "VM resumed" moments that a guest cannot yet act on are never
the stop line.

| tier | what happens between start and stop |
|---|---|
| **warm pool hit** | ownership transfer of a pre-booted, probed VM; no VM lifecycle work on the request path |
| **clone from golden** | restore a full VM (memory + disk) from a golden snapshot, reseed entropy/machine identity, re-probe readiness |
| **cold boot** | boot from the template image: kernel + initramfs + rootfs assembly + init to a probed silkd |

When comparing against other systems, match tiers — not headlines:

- A "cold start" that resumes a paused or snapshotted environment is our
  **clone** tier.
- A "cold start" that starts a container shares the host kernel and boots
  nothing; it has no equivalent tier here — compare it to **warm** or
  **clone** depending on whether the filesystem is prebuilt.
- A true boot — a kernel starting — is the only thing our **cold** tier
  reports.
- Never compare numbers measured under nested virtualization against
  bare-metal claims: nesting inflates VM-lifecycle latency ~2.4× in our
  measurements and punishes restore harder than boot.

## Reproduce

Prerequisites: a KVM host (`/dev/kvm`), [cocoon](https://github.com/cocoonstack/cocoon)
installed, `jq`, Go (or prebuilt binaries via `SANDBOXD_BIN`/`DEMO_BIN`/
`RPCBENCH_BIN`/`PULLBENCH_BIN`), and the silkd-baked template images
pulled:

```bash
cocoon image pull ghcr.io/cocoonstack/sandbox/rt:24.04
cocoon image pull ghcr.io/cocoonstack/sandbox/python:3.12

TEMPLATE=ghcr.io/cocoonstack/sandbox/rt:24.04 \
COLD_TEMPLATE=ghcr.io/cocoonstack/sandbox/python:3.12 \
make bench
```

The harness starts a throwaway sandboxd on its own port and data dir,
builds one warm pool (small, the warm tier), one golden-only pool
(medium, warm=0 — every claim is a clone), claims the cold tier from an
unpooled template, then measures the data plane (exec round-trips via
`rpcbench`, `fs_pull` throughput via `pullbench`). It prints a markdown
table stamped with the host evidence: virtualization
(`systemd-detect-virt`), CPU, kernel, cocoon version, image digest.

Knobs (environment variables): `WARM`/`WARM_N` (warm-pool depth and burst
size — the burst must stay within the depth, or refill loses the race and
the tail measures clones), `CLONE_N`, `COLD_N`, `RPC_N`, `PULL_MB`,
`PULL_N`.

Boot anatomy (where inside the cold tier the milliseconds go — kernel,
initramfs phases, rootfs handoff) has its own harness:
`scripts/boot-bench.sh`.

Noise guidance: p50 is the robust figure; p90/max show tail behavior.
Run at least twice and discard a first run that pulled images or built
goldens. Publish nothing without the environment stamp.

## Results log

Newest first. Bare-metal numbers are the headline; nested runs are
labeled and must only be compared against other nested runs.

<!-- paste `make bench` output below -->

### 2026-07-10 — bare metal

| environment | |
|---|---|
| host | bare metal, AMD Ryzen 7 9700X 8-Core Processor, 16 cores, 60 GiB |
| kernel | 6.17.0-35-generic |
| cocoon | v0.4.8-master.c48fe84 |
| template | ghcr.io/cocoonstack/sandbox/rt:24.04 @ sha256:d489c07f9907 |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.2 ms | 0.3 ms | 0.6 ms | 6 |
| clone from golden | 37.1 ms | 39.7 ms | 40.9 ms | 10 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12) | 379.7 ms | 379.7 ms | 402.6 ms | 3 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=0.22ms p90=1.71ms p99=3.40ms |
| fs_pull throughput (128 MiB) | 595.9 MiB/s best of 3 |

### 2026-07-08 — bare metal

| environment | |
|---|---|
| host | bare metal, AMD Ryzen 7 9700X 8-Core Processor, 16 cores, 60 GiB |
| kernel | 6.17.0-35-generic |
| cocoon | v0.4.8-master.d0252ce |
| template | ghcr.io/cocoonstack/sandbox/rt:24.04 @ sha256:c8cab53a1e16 |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.2 ms | 0.2 ms | 0.6 ms | 6 |
| clone from golden | 38.6 ms | 42.8 ms | 47.6 ms | 10 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12) | 331.5 ms | 331.5 ms | 404.2 ms | 3 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=0.17ms p90=0.22ms p99=0.26ms |
| fs_pull throughput (128 MiB) | 232.6 MiB/s best of 3 |

### 2026-07-07 — nested (google)

| environment | |
|---|---|
| host | nested (google), Intel(R) Xeon(R) CPU @ 2.80GHz, 8 cores, 31 GiB |
| kernel | 6.17.0-1020-gcp |
| cocoon | v0.4.8-master.d0252ce |
| template | ghcr.io/cocoonstack/sandbox/rt:24.04 @ sha256:6a11bb747ba7 |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.5 ms | 0.6 ms | 1.1 ms | 6 |
| clone from golden | 138.0 ms | 349.9 ms | 353.0 ms | 10 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12) | 1449.6 ms | 1449.6 ms | 1502.0 ms | 3 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=1.43ms p90=5.49ms p99=8.27ms |
| fs_pull throughput (128 MiB) | 95.7 MiB/s best of 3 |

