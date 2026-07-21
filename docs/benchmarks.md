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
| **burst** | `BURST_N` clone-tier claims issued concurrently — per-claim latency under restore contention plus the batch wall clock. Runs last so its churn cannot contaminate the RTT and throughput windows |

The harness also reports **warm refill recovery**: after fully draining the
warm pool it times the refill loop rebuilding to target — the number bounded
by refill admission (`refill_concurrency`, see
[Deployment](deploy.md)) rather than by a single restore.

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
`PULL_N`, `BURST_N` (concurrent clone-claim burst; 0 skips the stage).

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

### 2026-07-14 — bare metal (205d3f3, first round on the rust-review silkd)

| environment | |
|---|---|
| host | bare metal, AMD Ryzen 7 9700X 8-Core Processor, 16 cores, 60 GiB |
| kernel | 6.17.0-35-generic |
| cpufreq | powersave/balance_performance |
| cocoon | unstamped local build |
| template | ghcr.io/cocoonstack/sandbox/rt:24.04 @ sha256:71c7f3638e2e |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.2 ms | 0.3 ms | 0.6 ms | 6 |
| clone from golden | 30.6 ms | 37.1 ms | 41.7 ms | 10 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12) | 395.6 ms | 395.6 ms | 403.7 ms | 3 |
| burst: 16 concurrent clones | 98.6 ms | 190.9 ms | 206.8 ms | 16 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=0.20ms p90=0.26ms p99=0.37ms |
| fs_pull throughput (128 MiB) | 595.2 MiB/s best of 3 |
| burst wall (16 concurrent clones) | 285 ms |
| warm refill recovery (0 → 6) | 1639 ms |

Second of two rounds, both in band; the 16-step e2e smoke (egress lane
included) passed on the same images first. fs_push steady state measured
separately (`pushbench`, 128 MiB, 3 runs): 702/741/762 MiB/s against the
~343 MiB/s of record — the inbound base64 decode now feeds the borrowed
slice straight to the decoder, and the guest's musl allocator amplifies
what the codec-level A/B (+6.5% on glibc) understates; a toolchain bump
rides along, so treat the split between the two as unattributed.

### 2026-07-13 — bare metal (88fda81, first burst + refill-recovery round)

| environment | |
|---|---|
| host | bare metal, AMD Ryzen 7 9700X 8-Core Processor, 16 cores, 60 GiB |
| kernel | 6.17.0-35-generic |
| cpufreq | powersave/balance_performance |
| cocoon | master-acfc815 |
| template | ghcr.io/cocoonstack/sandbox/rt:24.04 @ sha256:d489c07f9907 |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.3 ms | 0.3 ms | 0.7 ms | 6 |
| clone from golden | 33.2 ms | 40.1 ms | 40.7 ms | 10 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12) | 381.0 ms | 381.0 ms | 393.7 ms | 3 |
| burst: 16 concurrent clones | 149.4 ms | 190.9 ms | 249.9 ms | 16 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=0.19ms p90=0.23ms p99=0.36ms |
| fs_pull throughput (128 MiB) | 611.6 MiB/s best of 3 |
| burst wall (16 concurrent clones) | 308 ms |
| warm refill recovery (0 → 6) | 1638 ms |

Second of two rounds (first: burst per-claim p50 126.5 ms, wall 331 ms;
sequential tiers within the band of the two same-day pre-burst rounds).
Refill recovery measured 1638 ms in both rounds — identical to the
millisecond, so the recovery is refill-cadence-quantized, not
restore-bound; measure the tick before re-tuning refill admission.

### 2026-07-10 — bare metal (2c8f5c6, locally built image)

| environment | |
|---|---|
| host | bare metal, AMD Ryzen 7 9700X 8-Core Processor, 16 cores, 60 GiB |
| kernel | 6.17.0-35-generic |
| cpufreq | powersave/performance |
| cocoon | v0.4.8-master.32bcbc6 |
| template | rt-2c8f5c6:24.04 @ sha256:ec268d5498fa (full local chain: silkd carrier → base → rt) |

| claim tier | p50 | p90 | max | n |
|---|---|---|---|---|
| warm pool hit | 0.2 ms | 0.3 ms | 0.6 ms | 6 |
| clone from golden | 26.0 ms | 29.3 ms | 30.6 ms | 10 |
| cold boot (pool-less node, coldproof claim→first exec) | 304 ms / 306 ms | — | 314 ms | 3 |
| cold boot (unpooled ghcr.io/cocoonstack/sandbox/python:3.12, coldproof) | 399.5 ms | — | 413.9 ms | 3 |

| data plane | measured |
|---|---|
| exec RTT (dial per RPC) | n=200 p50=0.21ms p90=1.27ms p99=2.56ms |
| fs_pull throughput (128 MiB) | 609.5 MiB/s best of 3 |

exec RTT is power-policy sensitive on this host: under the performance
governor the same stack measures p50=0.18 p90=0.22 p99=0.27 (n=1000, ×2),
reproducing the 07-08 entry; a host reboot reset the policy to
powersave, whose C-state/freq-ramp latency lands in the tail. The guest is
up 0.26s (its own /proc/uptime) when the first cold exec returns — silkd
answers long before a console login prompt would appear.

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

