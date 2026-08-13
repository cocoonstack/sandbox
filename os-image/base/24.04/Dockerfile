# sandbox base: Ubuntu 24.04 userland on the sandbox boot chain. No distro
# kernel, no initramfs-tools — /boot ships from the prebuilt boot artifact.
# CI overrides BOOT_IMAGE from boot/kernel/VERSION; the default is only a
# local-build fallback and may lag the pin.
ARG BOOT_IMAGE=ghcr.io/cocoonstack/sandbox/boot:6.18.37
# silkd carrier image (built from silkd/Dockerfile). CI overrides with the
# version-pinned tag; a silkd change refreshes this via the workflow_run chain.
ARG SILKD_IMAGE=ghcr.io/cocoonstack/sandbox/silkd:0.1.0

FROM ${BOOT_IMAGE} AS boot
FROM ${SILKD_IMAGE} AS silkd

FROM ubuntu:24.04@sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea
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
    git \
    && \
    # [Boot] No fsck/fstab/remount: the root is an overlay assembled pre-init.
    truncate -s 0 /etc/fstab && \
    systemctl mask systemd-fsck-root.service systemd-remount-fs.service systemd-fsck@.service && \
    # [Boot] Nothing may serialize boot on network-online.
    systemctl mask systemd-networkd-wait-online.service && \
    # [Identity] sandbox-init sets the kernel hostname; nothing may override it.
    rm -f /etc/hostname && \
    # [Identity] Drop the dbus-baked machine-id: systemd silently adopts it
    # when /etc/machine-id is empty, making every cold-booted VM identical.
    rm -f /var/lib/dbus/machine-id && truncate -s 0 /etc/machine-id && \
    # [Networking] DHCP on virtio NICs (cocoon-net leases), resolved for DNS.
    systemctl enable systemd-networkd systemd-resolved systemd-timesyncd && \
    # Docker bind-mounts /etc/resolv.conf during build (EBUSY on ln), so the
    # resolved-stub symlink is materialized at first boot via tmpfiles.
    echo 'L+ /etc/resolv.conf - - - - ../run/systemd/resolve/stub-resolv.conf' > /usr/lib/tmpfiles.d/sandbox-resolv.conf && \
    mkdir -p /etc/systemd/network && \
    printf "[Match]\nName=e* v*\n[Network]\nDHCP=yes\n\n[DHCPv4]\nClientIdentifier=mac\n" > /etc/systemd/network/20-wired.network && \
    # [SSH] Socket-activated: nothing in the boot path, spawned on first connect.
    systemctl disable ssh.service && systemctl enable ssh.socket && \
    # [Agent] vsock exec daemon (VM tier), started at sysinit.
    sh /run/secrets/sandbox_install_agent && \
    echo 'root:cocoon' | chpasswd && \
    # [silkd] product-tier daemon on vsock 2048, started at sysinit alongside
    # the agent — this is the sandbox product's readiness signal. Same
    # measured-fast ordering as the agent unit (no local-fs wait).
    #
    # 3128 is silkd's loopback relay over vsock to the host proxy, the none
    # lane's only way out. silkd forwards these into execs only on the
    # no-network lane, so a lane with its own NIC never dials a closed relay.
    printf '%s\n' \
      '[Unit]' \
      'Description=silkd (sandbox product daemon: exec, files, sessions over vsock)' \
      'Documentation=https://github.com/cocoonstack/sandbox' \
      'DefaultDependencies=no' \
      'Before=basic.target' \
      '' \
      '[Service]' \
      'Type=simple' \
      'ExecStart=/usr/local/bin/silkd' \
      'Environment=SILKD_PORT=2048' \
      'Environment=http_proxy=http://127.0.0.1:3128' \
      'Environment=https_proxy=http://127.0.0.1:3128' \
      'Environment=no_proxy=localhost,127.0.0.1,::1' \
      'Restart=always' \
      'RestartSec=2s' \
      'LimitNOFILE=65536' \
      '' \
      '[Install]' \
      'WantedBy=sysinit.target' \
      > /etc/systemd/system/silkd.service && \
    systemctl enable silkd.service && \
    rm -rf /var/lib/apt/lists/*

COPY --from=boot /boot/ /boot/
COPY --chmod=0755 --from=silkd /silkd /usr/local/bin/silkd

CMD ["/sbin/init"]
