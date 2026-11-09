#!/bin/sh
# Bake cocoon-agent into a sandbox image, as cocoon/os-image/ubuntu/install-agent.sh does.
# Differences: the unit starts at sysinit (its vsock listener is cocoon's readiness signal), and sshd stays socket-activated.
set -eu

AGENT_VERSION="${COCOON_AGENT_VERSION:-0.2.5}"
ARCH="${TARGETARCH:-$(dpkg --print-architecture)}"
case "$ARCH" in
    amd64) AGENT_ARCH="x86_64"; AGENT_SHA256="4507e0348b017f12f477444ac5022ce175980d31f538ba0e0d1f2d3d33441734" ;;
    arm64) AGENT_ARCH="arm64";  AGENT_SHA256="f327ba2aa32bce05ae1c0cb34dab39ef87f14335e20541287913d1220b1e825c" ;;
    *) echo "install-agent: unsupported arch '$ARCH'" >&2; exit 1 ;;
esac

# sshd: permit root login (stack-wide root:cocoon assumption).
mkdir -p /run/sshd
sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

# Pinned-version tarball; per-arch SHA256 so a version bump without updated
# checksums fails loudly instead of shipping a wrong binary.
TARBALL="cocoon-agent_${AGENT_VERSION}_Linux_${AGENT_ARCH}.tar.gz"
URL="https://github.com/cocoonstack/cocoon-agent/releases/download/v${AGENT_VERSION}/${TARBALL}"
TMP_TARBALL="$(mktemp)"
trap 'rm -f "$TMP_TARBALL"' EXIT
curl -fsSL "$URL" -o "$TMP_TARBALL"
echo "$AGENT_SHA256  $TMP_TARBALL" | sha256sum -c -
tar -xz -C /usr/local/bin/ -f "$TMP_TARBALL" cocoon-agent
chmod 0755 /usr/local/bin/cocoon-agent

cat > /etc/systemd/system/cocoon-agent.service <<'EOF'
[Unit]
Description=Cocoon agent (vsock command exec)
Documentation=https://github.com/cocoonstack/cocoon-agent
# No After=: the rootfs is whole before PID 1 and /dev/vsock is devtmpfs; waiting for local-fs.target cost ~140ms.
DefaultDependencies=no
Before=basic.target

[Service]
Type=simple
User=root
Group=root
ExecStart=/usr/local/bin/cocoon-agent serve
Environment=AGENT_LOG_LEVEL=info
Restart=always
RestartSec=2s
LimitNOFILE=65536
OOMPolicy=continue

[Install]
WantedBy=sysinit.target
EOF

systemctl enable cocoon-agent.service
