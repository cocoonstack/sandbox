# syntax=docker/dockerfile:1
# Boot artifact: custom guest kernel (PVH ELF vmlinux, everything =y) plus the
# sandbox-init micro-initramfs, shipped as a scratch image under /boot/ for
# os-image builds to COPY --from. See cocoon-specs/design/sandbox-fast-boot.md.

FROM debian:bookworm-slim AS kbuild
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential bc bison flex libelf-dev libssl-dev \
    xz-utils curl ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*
ARG KERNEL_VERSION=6.18.37
# Mirror chain: cdn.kernel.org currently 404s /pub/linux/kernel tarball paths
# (observed from GitHub runners and CN networks alike), while rsync mirrors
# still carry the tree. Download to a file first — a mid-stream failure must
# not leave a half-extracted tree.
ARG KERNEL_MIRROR=https://cdn.kernel.org/pub/linux/kernel
WORKDIR /linux
RUN set -e; \
    rel="v${KERNEL_VERSION%%.*}.x/linux-${KERNEL_VERSION}.tar.xz"; \
    for m in "${KERNEL_MIRROR}" \
             https://cdn.kernel.org/pub/linux/kernel \
             https://mirrors.edge.kernel.org/pub/linux/kernel \
             https://mirrors.tuna.tsinghua.edu.cn/kernel; do \
        echo "fetching ${m}/${rel}"; \
        if curl -fSL --connect-timeout 15 "${m}/${rel}" -o /tmp/linux.tar.xz; then break; fi; \
    done; \
    tar -xJf /tmp/linux.tar.xz --strip-components=1 && rm /tmp/linux.tar.xz
COPY kernel/sandbox.config /tmp/sandbox.config
RUN make x86_64_defconfig kvm_guest.config \
    && ./scripts/kconfig/merge_config.sh -m .config /tmp/sandbox.config \
    && make olddefconfig
# vmlinux only: the uncompressed ELF is the shipped artifact (no bzImage).
RUN make -j"$(nproc)" vmlinux \
    && gcc usr/gen_init_cpio.c -o /usr/local/bin/gen_init_cpio

FROM rust:1-alpine AS initbuild
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY init/Cargo.toml init/Cargo.lock ./
COPY init/src ./src
RUN cargo build --release --locked && cp target/release/sandbox-init /sandbox-init

FROM busybox:musl AS bb

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
# "vmlinuz" / "initrd.img" under /boot. The "vmlinuz" is really an ELF
# vmlinux — CH (PVH) and FC (ELF loader) both content-detect it.
FROM scratch
COPY --from=kbuild /linux/vmlinux /boot/vmlinuz-sandbox
COPY --from=pack /pack/initrd.img-sandbox /boot/initrd.img-sandbox
# Never runs; lets `docker create` work on this scratch image (make extract).
CMD ["/boot"]
