# Builds silkd and ships it as a binaries-only carrier image, the same shape
# as the boot artifact: base images COPY --from this image rather than
# rebuilding silkd, so a silkd change rebuilds only this artifact and the
# images gating workflow refolds it into the os-images.
# Two binaries: /silkd is glibc-dynamic (Debian's glibc is older than Ubuntu
# 24.04's, so the binary is forward-compatible into the base image);
# /silkd-static is musl-static for guests without glibc, such as the android
# flavor's non-systemd userspace. Built per-platform (amd64/arm64) on native
# runners; TARGETARCH picks the musl target triple.
FROM rust:1-slim@sha256:8e8cf8f7fd54a2d23d5a743b3a03f56e26b6c774276c33fa0595111704ebb15c AS build
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-musl > /musl-target ;; \
      arm64) echo aarch64-unknown-linux-musl > /musl-target ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    rustup target add "$(cat /musl-target)" && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /silkd
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked && strip target/release/silkd
RUN cargo build --release --locked --target "$(cat /musl-target)" && \
    strip "target/$(cat /musl-target)/release/silkd" && \
    cp "target/$(cat /musl-target)/release/silkd" /silkd-static

FROM scratch
COPY --from=build /silkd/target/release/silkd /silkd
COPY --from=build /silkd-static /silkd-static
