# node sandbox flavor: base + Node.js 22 LTS from the official tarball into
# /usr/local, plus the native-module build toolchain (build-essential,
# python3, unzip) so package installs with node-gyp steps need only the
# packages themselves from the network.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE}
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
# See base: cache-busts the layers below when non-Dockerfile inputs change.
ARG INPUTS_HASH=

ARG NODE_VERSION=22.23.2
ARG NODE_SHA256_AMD64=b294a556e639d64338823920e5866c21c02741742d2e1529ee1a225c1ec9252a
ARG NODE_SHA256_ARM64=013b59cfd2819703a6f4a14ab891fc46fc2a4e3f5bcd92de3fb4929b43e35b30

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python3 python3-setuptools unzip \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) arch=x64; sha="${NODE_SHA256_AMD64}" ;; \
         arm64) arch=arm64; sha="${NODE_SHA256_ARM64}" ;; \
         *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/node.tar.gz \
      "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${arch}.tar.gz" \
    && echo "${sha}  /tmp/node.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/node.tar.gz -C /usr/local --strip-components=1 \
         --exclude CHANGELOG.md --exclude LICENSE --exclude README.md \
    && rm /tmp/node.tar.gz \
    && node -v && npm -v
