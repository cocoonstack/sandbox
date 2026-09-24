# Performance

All numbers below are real measurements, not targets. Environment matters
enormously for microVM latency — every table states where it was measured.
Reproduce with `make bench` (tier definitions and the dated results log:
[Benchmarks](benchmarks.md)), `scripts/sandboxd-e2e.sh` (claim/verb
latencies print per run), and `scripts/boot-bench.sh` (boot phases).

**Bare metal** = a 16-core AMD (SVM) node, Ubuntu 24.04, kernel 6.x, local
NVMe, KVM. **Nested** = a cloud VM with nested virtualization; nested
inflates VM-lifecycle latencies ~2.4× and punishes restore harder than cold
boot — do not compare nested numbers against other systems' bare-metal
claims.

## Claim latency (what the SDK sees)

Measured through the full stack (SDK → sandboxd → cocoon → guest silkd),
bare metal, `small` tier:

| tier | latency | what happens |
|---|---|---|
| warm pool hit | **0.2–0.7 ms** | ownership transfer only; refill re-tops the pool in the background |
| pool miss, golden exists | **~26–39 ms** | clone from the golden snapshot + entropy/machine-id reseed + readiness probe |
| cold boot (no golden yet) | **~215–400 ms** | full boot from the template image to silkd answering |

A guarded-egress claim binds its proxy doors at refill rather than at claim
(#177, cocoon-test2 bare metal, 2026-09-14): that took the warm claim with the
HTTP door from 307 to 263 µs p50, and with both doors from 351 to 274 µs.

Cloud Hypervisor lifecycle latency (bare metal, vsock agent-ready):

| path | latency |
|---|---|
| cold boot | ~230 ms |
| clone from golden (eager) | ~44 ms |

Both network lanes use Cloud Hypervisor; `net=none` only removes the NIC.
Eager memory restore beats UFFD on-demand for sandbox-sized VMs in every
configuration measured (the working set is small and mostly touched during
readiness).

Burst degradation under concurrent restores is real, and warm pools exist
to absorb it — they move provisioning off the request path entirely.
Provisioning concurrency is bounded by `refill_concurrency`, auto-scaled
from node CPUs by default ([Deployment](deploy.md)). `make bench` measures
both sides: the burst stage issues `BURST_N` concurrent clone claims, and
the refill-recovery stage times the warm pool rebuilding after a full
drain ([Benchmarks](benchmarks.md)).

## Verb round-trips (SDK over the relay, bare metal)

The published smoke ranges below predate connection reuse: each call includes
a fresh HTTP upgrade and vsock connection. Current Go and Python SDK handles
keep an idle relay for 30 seconds and send later RPCs on it; use mode C of
`rpcbench` for the current steady-state path.

| verb | typical RTT |
|---|---|
| exec (echo) | 2–14 ms |
| file write+read back | 2–12 ms |
| session exec (persistent shell) | 3–26 ms |
| find (regex over a tree) | 2–7 ms |
| replace (atomic rewrite) | 1–5 ms |
| watch: armed→event delivered | 1–3 ms |
| git add+commit+status (real repo) | 30–130 ms |
| pty open→echo→exit | 5–28 ms |

## Relay connection reuse

Every data-plane RPC used to pay a TCP dial, an HTTP Upgrade and, behind a
TLS edge, a handshake. From proto 2 silkd serves RPCs back to back, and both
SDKs keep one relay connection per handle with a 30s idle window.

`e2e/cmd/rpcbench`, warm `rt:24.04` sandbox, `net=none`, n=200 `fs_stat`
RPCs per arm, cocoon-test1 bare metal, client on the node, 2026-09-16 at
5158a14. The two arms interleave sample by sample and swap which one leads,
so drift cannot land on one of them. The pre-dialed spare row is a third arm
that rpcbench ran after them at that commit and no longer carries.

| per-RPC wall | plain p50 | plain p90 | TLS edge p50 | TLS edge p90 |
| --- | --- | --- | --- | --- |
| dial per RPC | 0.29ms | 0.33ms | 1.15ms | 1.34ms |
| kept connection | 0.10ms | 0.13ms | 0.12ms | 0.16ms |
| pre-dialed spare | 0.19ms | 0.25ms | 0.95ms | 1.24ms |

Reuse removes ~0.19ms per RPC on a plain node and ~1.0ms behind the edge,
where the handshake dominates and the kept connection is the only arm that
escapes it. Three runs per leg agreed within 0.02ms at p50; one run on a
loaded node moved every arm together and left the ratio intact.

The edge here is a local HTTP/1.1 TLS terminator on loopback, so its
handshake carries no network RTT — a client across a WAN saves more, not
less. An old daemon reporting proto 1 makes the SDK dial per call, which is
the plain dial-per-RPC row.

## Boot chain

Kernel entry → rootfs handoff (custom all-builtin kernel + Rust initramfs,
single-layer image): **~40–50 ms** on the bench node — kernel→sandbox-init is
effectively all of it, and init→handoff rounds to 0 ms (per-phase µs trace
via `sandbox.trace=1`). Spawn → silkd answering measures ~136–208 ms bare
metal, median ~151. silkd starts in parallel at sysinit and adds ~1–10 ms
cold / ~3–12 ms on restore paths. The dated rows behind both figures are in
[Benchmarks](benchmarks.md#results-log).

## Hibernate

> The `vm hibernate` / `vm restore` rows below were measured with cocoon's
> own tooling on the same hardware; they are not reproducible from this
> repository alone.

cocoon's atomic hibernate
([cocoonstack/cocoon#87](https://github.com/cocoonstack/cocoon/pull/87))
snapshots and stops in one pause window: capture, persist, and VMM
termination are atomic — the VMM dies only after the snapshot is durably
stored, and a failed save (disk full, snapshot DB error) resumes the VM with
nothing lost. Direct capture
([cocoonstack/cocoon#109](https://github.com/cocoonstack/cocoon/pull/109))
fsyncs and renames the snapshot into the store instead of streaming it
through a tar pass, cutting the CH pause window ~1.5×. Measured bare
metal (16c/60G), `small` 512 M, cocoon `master-e9502f6`, CH dev `d77dcf12`,
N=5:

| op | latency | notes |
|---|---|---|
| `vm hibernate` | ~170–270 ms | pause → snapshot → persist into the store → VMM killed; memory freed, snapshot point and stop coincide |
| `vm restore` (stopped VM, eager) | ~55–190 ms (median ~65) | machine identity preserved, tmpfs contents intact, in-guest daemons resume |
| `vm restore` (stopped VM, `restore_mode: mmap`) | ~40–67 ms (median ~55) | the node's `restore_mode` rides sandboxd wakes too; the wake drops its snapshot right after — safe, restore stages a private copy |

## Method notes

- "agent-ready"/"silkd-ready" = create/clone start → the vsock daemon
  completes a round-trip; this includes CLI and probe harness overhead, so
  it is an honest end-to-end number, not a VMM-internal one.
- Warm/miss/verb numbers come straight from `sandboxd-e2e.sh` output on a
  real node; run it on your own hardware for numbers that apply to you.

## Regression discipline

There is deliberately no CI performance gate: CI has no KVM, and a numbers
gate that measures a different machine class guards nothing. The manual
harnesses are `scripts/boot-bench.sh` and the e2e smoke's per-step timings;
re-measure and update this page when touching the boot chain, the relay, or
snapshot paths.

## Claims-journal write path

`claim`/`release`/`hibernate`/`wake` persist `claims.json`, but the write is
kept off the manager mutex — the lock every data-plane op contends. `set`/`del`
update a projection map and bump a sequence under the store's own mutex;
`commit()` clones that map under it, then marshals, writes, and renames off
both the manager and the store mutex, serialized by its own write lock and
coalescing by sequence so an older snapshot never overwrites a newer one. The
write is unsynced by default: `sync_claims` adds an fsync of the file and its
directory, about 0.3 ms per commit on NVMe (2000 claims, 280 KB) against
0.05 ms, for a fleet that must keep its hibernated and archived sandboxes
through a host power loss.
Only the startup `Reconcile`
pass (pre-contention) still marshals and writes in one call under the lock.

`BenchmarkStorePersistContention` measures the ns a concurrent manager-mutex
acquire waits during a persist. Moving first the write, then the marshal,
off the lock cut that wait from tens of µs to tens of ns — the
marshal-off-lock split alone is a ~6× drop at 1000 live claims, a margin that
grows with claim count. `BenchmarkStoreSaveScaling` (the combined `save()`
Reconcile uses) stays as a regression sentinel.

## Decision data

**Relay connection strategies** (2026-07-07, GCE nested, n=300 `fs_stat`
RPCs): dial-per-RPC measured p50 1.23ms / p90 4.69ms / p99 10.51ms; one
connection pre-dialed ahead measured p50 1.05ms / p90 4.47ms / p99 15.49ms.
That experiment rejected the background pre-dialer because its ~0.2ms p50 win
did not survive the tail. Current main instead implements protocol-level
sequential reuse: silkd proto 2 serves RPCs back to back and each SDK handle
parks completed relay connections for its next call. This is neither the
pre-dialer nor concurrent multiplexing. `e2e/cmd/rpcbench` reports A (dial
per RPC) and C (SDK keep-alive); the numbers in Relay connection reuse above,
the pre-dialed spare row included, keep the pre-dialer declined — behind a TLS
edge it tracks the dial arm, because it hides a handshake only while RPCs
arrive slower than a dial completes.

**Template pre-check meta GET**: a single `ReadMeta` against MinIO
measured 4.76ms cross-host (sub-ms node-local, 20-50ms on WAN S3; a
2026-07 spot check, host and commit not recorded). It
would fire once per warm-miss claim of a promoted template whose key
gossip advertises, against a provision that already costs a >=48ms clone
plus the export fetch. Declined until a measurement shows it matters.
