#!/bin/sh
# Bake cocoon-agent (vsock exec daemon) into a sandbox image.
# Mirrors cocoon/os-image/ubuntu/install-agent.sh with two deltas:
#   - the unit starts at sysinit (DefaultDependencies=no): the agent listening
#     on vsock IS the sandbox readiness signal, so it must not wait for
#     network or multi-user; the sandbox kernel builds vsock in (no modprobe).
#   - sshd stays socket-activated (Dockerfile enables ssh.socket), so there is
#     no `systemctl enable ssh` here.
set -eu

AGENT_VERSION="${COCOON_AGENT_VERSION:-0.1.8}"
ARCH="${TARGETARCH:-$(dpkg --print-architecture)}"
case "$ARCH" in
    amd64) AGENT_ARCH="x86_64"; AGENT_SHA256="88247fe230e4064fce4515e14aa0d70d0b8d070f876bf3295240392698dfaee0" ;;
    arm64) AGENT_ARCH="arm64";  AGENT_SHA256="26eb63095eb8d7934d7c7f583002abf70ec363e29d9768c8dfb23a022152429a" ;;
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
# No After= at all: sandbox-init hands over a fully-assembled rootfs before
# PID 1 runs and /dev/vsock is devtmpfs — measured, waiting for
# local-fs.target cost ~140ms of pure ordering delay on the critical chain.
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

[Install]
WantedBy=sysinit.target
EOF

systemctl enable cocoon-agent.service
