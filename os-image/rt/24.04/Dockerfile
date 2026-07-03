# Squashed runtime image: the entire base rootfs (incl. /boot) flattened to
# ONE layer. Each image layer is one virtio-blk disk the guest kernel must
# probe (~10-20ms each) — the layered base is 3 disks + COW, this is 1 + COW.
# Tradeoff: no layer dedup/delta on pull; keep the layered base for builds
# and dev, use rt for latency-sensitive production sandboxes.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE} AS src

FROM scratch
COPY --from=src / /

CMD ["/sbin/init"]
