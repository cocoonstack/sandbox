# android sandbox flavor: the cocoon-android lineage (redroid-style Android
# rootfs, /init entrypoint, its own embedded kernel + binder module) plus
# silkd. The userspace has no glibc and no systemd, so this flavor ships the
# musl-static silkd binary and starts it from an Android init .rc.
#
# The base is public on ghcr (anonymous pull), pinned by tag; override for a
# lineage bump.
ARG ANDROID_BASE_IMAGE=ghcr.io/jiaqing-simular/cocoon-android-vnc:0.3.0
# silkd carrier image (built from silkd/Dockerfile); CI passes the
# version-pinned tag.
ARG SILKD_IMAGE=ghcr.io/cocoonstack/sandbox/silkd:0.1.0

FROM ${SILKD_IMAGE} AS silkd

FROM ${ANDROID_BASE_IMAGE}
COPY --chmod=0755 --from=silkd /silkd-static /system/bin/silkd
COPY silkd.rc /system/etc/init/silkd.rc
