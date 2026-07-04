# Builds the silkd binary and ships it as a single-file carrier image, the
# same shape as the boot artifact: base images COPY --from this image rather
# than rebuilding silkd, so a silkd change rebuilds only this artifact and the
# workflow_run chain refreshes the os-images. Debian's glibc is older than
# Ubuntu 24.04's, so the dynamically linked binary is forward-compatible into
# the base image.
FROM rust:1-slim AS build
WORKDIR /silkd
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && strip target/release/silkd

FROM scratch
COPY --from=build /silkd/target/release/silkd /silkd
