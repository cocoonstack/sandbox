# Builds silkd and ships it as a binaries-only carrier image, the same shape
# as the boot artifact: base images COPY --from this image rather than
# rebuilding silkd, so a silkd change rebuilds only this artifact and the
# images gating workflow refolds it into the os-images.
# Two binaries: /silkd is glibc-dynamic (Debian's glibc is older than Ubuntu
# 24.04's, so the binary is forward-compatible into the base image);
# /silkd-static is musl-static for guests without glibc, such as the android
# flavor's non-systemd userspace. The musl target is x86_64-only, matching
# the amd64 platform pin the carrier is built under.
FROM rust:1-slim AS build
RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /silkd
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && strip target/release/silkd
RUN cargo build --release --target x86_64-unknown-linux-musl && \
    strip target/x86_64-unknown-linux-musl/release/silkd

FROM scratch
COPY --from=build /silkd/target/release/silkd /silkd
COPY --from=build /silkd/target/x86_64-unknown-linux-musl/release/silkd /silkd-static
