#!/bin/sh
# Bake cocoon-agent (vsock exec daemon) into a sandbox image.
# Mirrors cocoon/os-image/ubuntu/install-agent.sh with two deltas:
#   - the unit starts at sysinit (DefaultDependencies=no): the agent listening
#     on vsock IS the sandbox readiness signal, so it must not wait for
#     network or multi-user; the sandbox kernel builds vsock in (no modprobe).
#   - sshd stays socket-activated (Dockerfile enables ssh.socket), so there is
#     no `systemctl enable ssh` here.
set -eu

AGENT_VERSION="${COCOON_AGENT_VERSION:-0.1.3}"
ARCH="${TARGETARCH:-$(dpkg --print-architecture)}"
case "$ARCH" in
    amd64) AGENT_ARCH="x86_64"; AGENT_SHA256="7a7247008e70d7d2d5d30f11c9d501ffe950e1c2731bd7099af4c9fb904c8935" ;;
    arm64) AGENT_ARCH="arm64";  AGENT_SHA256="574615f28049a7d2db29c0c1c3cb5f505e1d11de2389b766dfecc94b23b2ce2f" ;;
    *) echo "install-agent: unsupported arch '$ARCH'" >&2; exit 1 ;;
esac

# sshd: permit root login (stack-wide root:cocoon assumption, see
# cocoon-specs/design/credential-assumptions.md).
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
# Readiness signal for the whole sandbox: start as early as systemd allows.
# vsock is compiled into the sandbox kernel; /dev/vsock exists via devtmpfs.
DefaultDependencies=no
After=local-fs.target
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

[Install]
WantedBy=sysinit.target
EOF

systemctl enable cocoon-agent.service
