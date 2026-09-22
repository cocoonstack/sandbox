# rt's one-layer shape plus e2b's envd, for the e2b-compatible data plane.
# envd is an addition, never a replacement: silkd stays the daemon the native
# SDKs, MCP, preview, LSP and the egress lane speak to.
#
# amd64 only: e2b publishes no arm64 build of envd.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04
# The bucket object is mutable, so the build verifies a SHA-256 rather than trusting the URL.
ARG ENVD_VERSION=v0.8.0
ARG ENVD_SHA256=752cfbc7ba66649ec1bffdc9b59529dd3527e7d3899c69351e05bd21dfc0dbf0

FROM ${BASE_IMAGE} AS src
ARG ENVD_VERSION
ARG ENVD_SHA256
ENV DEBIAN_FRONTEND=noninteractive

RUN curl -fsSL -o /usr/local/bin/envd \
      "https://storage.googleapis.com/e2b-artifact-binaries/envd/${ENVD_VERSION}/envd" && \
    echo "${ENVD_SHA256}  /usr/local/bin/envd" | sha256sum -c - && \
    chmod 0755 /usr/local/bin/envd && \
    echo "${ENVD_VERSION#v}" > /etc/envd-version && \
    # [envd] envd hardcodes a 0.0.0.0 listener with no bind flag, so the guest
    # drops inbound 49983 from anywhere but loopback itself. The none lane has
    # no NIC at all; this is what keeps the egress lane's tap from reaching it.
    apt-get update && apt-get install -y --no-install-recommends nftables && \
    printf '%s\n' \
      'table inet e2bguard {' \
      '  chain input {' \
      '    type filter hook input priority filter; policy accept;' \
      '    iif != "lo" tcp dport 49983 drop' \
      '  }' \
      '}' \
      > /etc/nftables-envd.conf && \
    printf '%s\n' \
      '[Unit]' \
      'Description=drop non-loopback traffic to envd' \
      'DefaultDependencies=no' \
      'Before=envd.service' \
      '' \
      '[Service]' \
      'Type=oneshot' \
      'RemainAfterExit=yes' \
      'ExecStart=/usr/sbin/nft -f /etc/nftables-envd.conf' \
      '' \
      '[Install]' \
      'WantedBy=sysinit.target' \
      > /etc/systemd/system/envd-guard.service && \
    # [envd] -isnotfc: no MMDS and no HTTP log exporter outside Firecracker.
    # -no-cgroups: envd's freeze/thaw is Firecracker's pause model; cocoon
    # hibernates the whole VM, and the guest's cgroup tree is systemd's.
    printf '%s\n' \
      '[Unit]' \
      'Description=envd (e2b data-plane daemon, loopback only)' \
      'Documentation=https://github.com/e2b-dev/infra/tree/main/packages/envd' \
      'DefaultDependencies=no' \
      'After=envd-guard.service' \
      'Requires=envd-guard.service' \
      'Before=basic.target' \
      '' \
      '[Service]' \
      'Type=simple' \
      'ExecStart=/usr/local/bin/envd -isnotfc -no-cgroups' \
      'Restart=always' \
      'RestartSec=2s' \
      'LimitNOFILE=65536' \
      '' \
      '[Install]' \
      'WantedBy=sysinit.target' \
      > /etc/systemd/system/envd.service && \
    systemctl enable envd-guard.service envd.service && \
    rm -rf /var/lib/apt/lists/*

FROM scratch
COPY --from=src / /

CMD ["/sbin/init"]
