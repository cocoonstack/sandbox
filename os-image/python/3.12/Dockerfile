# python sandbox flavor: base + CPython toolchain for agent code execution.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
# See base: cache-busts the layers below when non-Dockerfile inputs change.
ARG INPUTS_HASH=

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv git \
    && rm -rf /var/lib/apt/lists/*

# LSP broker manifest: pylsp is the v1 language server (pure Python on the
# flavor's interpreter, ~15-20MB; ty tracked as the successor). silkd's
# lsp_start reads this file's argv to spawn the server for "python".
RUN pip3 install --no-cache-dir --break-system-packages python-lsp-server \
    && mkdir -p /etc/silkd/lsp.d \
    && printf 'pylsp\n' > /etc/silkd/lsp.d/python
