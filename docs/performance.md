# Performance

All numbers below are real measurements, not targets. Environment matters
enormously for microVM latency — every table states where it was measured.
Reproduce with `scripts/sandboxd-e2e.sh` (claim/verb latencies print per
run) and `scripts/boot-bench.sh` (boot phases).

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
| pool miss, golden exists | **~45–75 ms** | clone from the golden snapshot + entropy/machine-id reseed + readiness probe |
| cold boot (no golden yet) | **~200–350 ms** | full boot from the template image to silkd answering |

Backend cold-boot vs restore asymmetry (bare metal, vsock agent-ready):

| path | Cloud Hypervisor | Firecracker |
|---|---|---|
| cold boot | ~230 ms | ~330 ms |
| clone from golden (eager) | ~44 ms | ~28 ms |

CH wins cold (block I/O), FC wins restore — which is why the no-network
lane (FC) is also the fastest-restore lane. Eager memory restore beats
UFFD on-demand for sandbox-sized VMs in every configuration measured
(the working set is small and mostly touched during readiness).

Burst behavior: 10 concurrent cold clones degrade P99 to ~650–900 ms —
that is the case for warm pools, which move provisioning off the request
path entirely.

## Verb round-trips (SDK over the relay, bare metal)

Steady-state times from the e2e smoke, one live sandbox, including the
HTTP-upgrade relay and vsock hops:

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

## Boot chain

Kernel entry → rootfs handoff (custom all-builtin kernel + Rust initramfs,
single-layer image): **~120–130 ms** on the bench node; the initramfs
itself accounts for ~4 ms (per-phase µs trace via `sandbox.trace=1`).
Agent-ready ~490 ms nested / ~206–230 ms bare metal. silkd starts in
parallel at sysinit and adds ~1–10 ms cold / ~3–12 ms on restore paths.

## Hibernate

cocoon's atomic hibernate
([cocoonstack/cocoon#87](https://github.com/cocoonstack/cocoon/pull/87))
snapshots and stops in one pause window: capture, persist, and VMM
termination are atomic — the VMM dies only after the snapshot is durably
stored, and a failed save (disk full, snapshot DB error) resumes the VM with
nothing lost. Measured bare metal, FC, `small`:

| op | latency | notes |
|---|---|---|
| `vm hibernate` | ~330–460 ms | pause → snapshot → persist → VMM killed; memory freed, snapshot point and stop coincide |
| `vm restore` (stopped VM) | ~27–35 ms | machine identity preserved, tmpfs contents intact, in-guest daemons resume |
| SDK hibernate → wake loop | ~440–470 ms | `sb.Hibernate()` + transparent wake on the next exec, through the relay; a live shell session survives with its state intact (smoke-measured, bare metal) |

## Method notes

- "agent-ready"/"silkd-ready" = create/clone start → the vsock daemon
  completes a round-trip; this includes CLI and probe harness overhead, so
  it is an honest end-to-end number, not a VMM-internal one.
- Warm/miss/verb numbers come straight from `sandboxd-e2e.sh` output on a
  real node; run it on your own hardware for numbers that apply to you.
