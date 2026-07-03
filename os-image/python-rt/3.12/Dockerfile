# Squashed runtime python image: the entire python rootfs (incl. /boot)
# flattened to ONE layer — same rationale as rt (each layer is one more
# virtio-blk disk the guest kernel probes, ~10-20ms each).
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE} AS base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv git \
    && rm -rf /var/lib/apt/lists/*

FROM scratch
COPY --from=base / /

CMD ["/sbin/init"]
