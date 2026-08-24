# syntax=docker/dockerfile:1
# Boot artifact: custom guest kernel (amd64: PVH ELF vmlinux; arm64: flat
# Image) plus the sandbox-init micro-initramfs, shipped as a scratch image
# under /boot/ for os-image builds to COPY --from. Built per-platform on
# native runners (a kernel build under QEMU emulation takes hours).

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS kbuild
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential bc bison flex libelf-dev libssl-dev \
    xz-utils curl ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*
ARG KERNEL_VERSION=6.18.37
# Mirror chain: cdn.kernel.org currently 404s /pub/linux/kernel tarball paths
# (observed from GitHub runners and CN networks alike), while rsync mirrors
# still carry the tree. Download to a file first — a mid-stream failure must
# not leave a half-extracted tree — and verify against the pinned SHA256
# (kernel/SHA256, updated together with VERSION): the fallback mirrors are
# third parties and this kernel boots untrusted-code sandboxes.
ARG KERNEL_MIRROR=https://cdn.kernel.org/pub/linux/kernel
WORKDIR /linux
COPY kernel/SHA256 /tmp/SHA256
# The hash is taken from the exact line naming this kernel version, so a
# multi-line SHA256 file (e.g. keeping the previous kernel's entry during a
# bump) can never pair the tarball with the wrong hash.
RUN set -e; \
    hash=$(awk -v f="linux-${KERNEL_VERSION}.tar.xz" '$2==f{print $1; exit}' /tmp/SHA256); \
    [ -n "$hash" ] || { echo "kernel/SHA256 has no entry for linux-${KERNEL_VERSION}.tar.xz"; exit 1; }; \
    rel="v${KERNEL_VERSION%%.*}.x/linux-${KERNEL_VERSION}.tar.xz"; \
    for m in "${KERNEL_MIRROR}" \
             https://cdn.kernel.org/pub/linux/kernel \
             https://mirrors.edge.kernel.org/pub/linux/kernel \
             https://mirrors.tuna.tsinghua.edu.cn/kernel; do \
        echo "fetching ${m}/${rel}"; \
        if curl -fSL --connect-timeout 15 --max-time 600 "${m}/${rel}" -o /tmp/linux.tar.xz; then break; fi; \
    done; \
    echo "$hash  /tmp/linux.tar.xz" | sha256sum -c -; \
    tar -xJf /tmp/linux.tar.xz --strip-components=1 && rm /tmp/linux.tar.xz /tmp/SHA256
ARG TARGETARCH
COPY kernel/sandbox.config /tmp/sandbox.config
COPY kernel/sandbox-arm64.config /tmp/sandbox-arm64.config
# merge_config only warns and olddefconfig silently drops renamed or
# dependency-orphaned symbols — assert the boot- and container-critical set
# so a kernel bump cannot lose one with a green build. Boot entry and console
# are per-arch: amd64 = PVH, arm64 = direct Image boot + PL031 RTC.
RUN set -e; \
    case "$TARGETARCH" in \
      amd64) make x86_64_defconfig kvm_guest.config; \
             ./scripts/kconfig/merge_config.sh -m .config /tmp/sandbox.config; \
             arch_asserts="PVH VIRTIO_CONSOLE" ;; \
      arm64) make defconfig; \
             ./scripts/kconfig/merge_config.sh -m .config /tmp/sandbox.config /tmp/sandbox-arm64.config; \
             arch_asserts="RTC_DRV_PL031" ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH"; exit 1 ;; \
    esac; \
    make olddefconfig; \
    for s in $arch_asserts DEVTMPFS BLK_DEV_INITRD \
             VIRTIO_PCI VIRTIO_BLK VIRTIO_NET \
             VIRTIO_CONSOLE VIRTIO_VSOCKETS \
             EROFS_FS EROFS_FS_ZIP OVERLAY_FS EXT4_FS \
             NAMESPACES USER_NS SECCOMP_FILTER \
             BRIDGE VETH BRIDGE_NETFILTER NF_NAT NFT_COMPAT \
             NETFILTER_XT_TARGET_MASQUERADE NETFILTER_XTABLES_LEGACY; do \
        grep -q "^CONFIG_${s}=y" .config \
            || { echo "CONFIG_${s} lost after olddefconfig"; exit 1; }; \
    done; \
    if grep -q "^CONFIG_MODULES=y" .config; then \
        echo "CONFIG_MODULES must be off"; exit 1; \
    fi
# amd64 ships the uncompressed ELF vmlinux; arm64 ships the flat Image.
RUN set -e; \
    if [ "$TARGETARCH" = amd64 ]; then \
        make -j"$(nproc)" vmlinux && cp vmlinux /linux/boot-kernel; \
    else \
        make -j"$(nproc)" Image && cp arch/arm64/boot/Image /linux/boot-kernel; \
    fi; \
    gcc usr/gen_init_cpio.c -o /usr/local/bin/gen_init_cpio

FROM rust:1-alpine@sha256:a10e64dd139b7387337c7fbe8aca31b959b57b2fd4c8ae20a02cf1d6ea424dce AS initbuild
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY init/Cargo.toml init/Cargo.lock ./
COPY init/src ./src
RUN cargo build --release --locked && cp target/release/sandbox-init /sandbox-init

FROM busybox:musl@sha256:32b5cdad7cce41dfd53d0ae06baebcf8357a147ee7694dc706911c373bc30c37 AS bb

FROM kbuild AS pack
ARG INITRD_DEBUG=0
WORKDIR /pack
COPY --from=initbuild /sandbox-init sandbox-init
COPY --from=bb /bin/busybox busybox
# gen_init_cpio writes device nodes without root tricks; the cpio stays
# uncompressed — ~1.5MB unpacks in ~1ms, a decompressor would only add time.
RUN <<EOF
set -e
cat > initrd.desc <<'DESC'
dir /dev 0755 0 0
nod /dev/console 0600 0 0 c 5 1
dir /proc 0755 0 0
dir /sys 0755 0 0
dir /newroot 0755 0 0
dir /l 0755 0 0
dir /cow 0755 0 0
file /init /pack/sandbox-init 0755 0 0
DESC
if [ "$INITRD_DEBUG" = "1" ]; then
cat >> initrd.desc <<'DESC'
dir /bin 0755 0 0
file /bin/busybox /pack/busybox 0755 0 0
slink /bin/sh /bin/busybox 0777 0 0
DESC
fi
gen_init_cpio initrd.desc > initrd.img-sandbox
EOF

# Names keep cocoon's scanBootFiles discovery working: base name prefixes
# "vmlinuz" / "initrd.img" under /boot. The "vmlinuz" is an ELF vmlinux on
# amd64 and a flat Image on arm64.
FROM scratch
COPY --from=kbuild /linux/boot-kernel /boot/vmlinuz-sandbox
COPY --from=pack /pack/initrd.img-sandbox /boot/initrd.img-sandbox
# Never runs; lets `docker create` work on this scratch image (make extract).
CMD ["/boot"]
