# Binaries-only carrier the os-images COPY --from: /silkd is glibc-dynamic
# (Debian's glibc is older than Ubuntu 24.04's), /silkd-static is musl-static
# for guests without glibc, such as android. TARGETARCH picks the musl triple.
FROM rust:1.99.1-slim-bookworm@sha256:24707e70a4f1e23d9a7b25202ec43126d28a52294b96aad5c670df1fd1f1d400 AS build
ARG TARGETARCH
WORKDIR /silkd
# The pin, not the base image's default, is the compiler: copy it before any rustup or cargo call.
COPY rust-toolchain.toml ./
RUN sed -i '/^components/d;/^targets/d' rust-toolchain.toml
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-musl > /musl-target ;; \
      arm64) echo aarch64-unknown-linux-musl > /musl-target ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac && \
    rustup target add "$(cat /musl-target)" && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked
RUN cargo build --release --locked --target "$(cat /musl-target)" && \
    cp "target/$(cat /musl-target)/release/silkd" /silkd-static

FROM scratch
COPY --from=build /silkd/target/release/silkd /silkd
COPY --from=build /silkd-static /silkd-static
