# sandbox base: Ubuntu 24.04 userland on the sandbox boot chain. No distro
# kernel, no initramfs-tools — /boot ships from the prebuilt boot artifact.
# CI overrides BOOT_IMAGE from boot/kernel/VERSION; the default is only a
# local-build fallback and may lag the pin.
ARG BOOT_IMAGE=ghcr.io/cocoonstack/sandbox/boot:6.18.37

FROM ${BOOT_IMAGE} AS boot

# sandbox-shim (WS-3): sandbox-init hands off here; it forks cocoon-agent
# before exec'ing systemd, so agent-ready doesn't wait for systemd.
FROM rust:1-alpine AS shimbuild
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY shim/ .
RUN cargo build --release && cp target/release/sandbox-shim /sandbox-shim

# Staging so boot files + shim land as ONE image layer — every layer is one
# more EROFS disk for sandbox-init to resolve and mount (~10-20ms each).
FROM scratch AS extras
COPY --from=shimbuild /sandbox-shim /sbin/sandbox-shim
COPY --from=boot /boot/ /boot/

FROM ubuntu:24.04
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
# Content hash of the image dir's non-Dockerfile inputs (CI computes it).
# Secret-mounted scripts never participate in layer cache keys — this ARG
# busts the RUN below exactly when any input actually changes.
ARG INPUTS_HASH=

RUN --mount=type=secret,id=sandbox_install_agent \
    apt-get update && apt-get install -y --no-install-recommends \
    systemd \
    systemd-sysv \
    systemd-resolved \
    systemd-timesyncd \
    udev \
    iproute2 iputils-ping curl wget ca-certificates \
    openssh-server \
    && \
    # [Boot] No fsck/fstab/remount: the root is an overlay assembled pre-init.
    truncate -s 0 /etc/fstab && \
    systemctl mask systemd-fsck-root.service systemd-remount-fs.service systemd-fsck@.service && \
    # [Boot] Nothing may serialize boot on network-online.
    systemctl mask systemd-networkd-wait-online.service && \
    # [Identity] sandbox-init sets the kernel hostname; nothing may override it.
    rm -f /etc/hostname && \
    # [Networking] DHCP on virtio NICs (cocoon-net leases), resolved for DNS.
    systemctl enable systemd-networkd systemd-resolved systemd-timesyncd && \
    # Docker bind-mounts /etc/resolv.conf during build (EBUSY on ln), so the
    # resolved-stub symlink is materialized at first boot via tmpfiles.
    echo 'L+ /etc/resolv.conf - - - - ../run/systemd/resolve/stub-resolv.conf' > /usr/lib/tmpfiles.d/sandbox-resolv.conf && \
    mkdir -p /etc/systemd/network && \
    printf "[Match]\nName=e* v*\n[Network]\nDHCP=yes\n\n[DHCPv4]\nClientIdentifier=mac\n" > /etc/systemd/network/20-wired.network && \
    # [SSH] Socket-activated: nothing in the boot path, spawned on first connect.
    systemctl disable ssh.service && systemctl enable ssh.socket && \
    # [Agent] vsock exec daemon, started at sysinit — the readiness signal.
    sh /run/secrets/sandbox_install_agent && \
    echo 'root:cocoon' | chpasswd && \
    rm -rf /var/lib/apt/lists/*

COPY --from=extras / /

CMD ["/sbin/init"]
