# browser sandbox flavor: base + headless Chrome for Testing with its CDP
# endpoint on guest loopback 9222, reached through the silkd port relay.
# Ubuntu's chromium apt package is a snap stub that cannot install in a
# Docker build; Chrome for Testing is Google's pinned automation build.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
# See base: cache-busts the layers below when non-Dockerfile inputs change.
ARG INPUTS_HASH=

ARG CHROME_VERSION=150.0.7871.115
ARG CHROME_SHA256=1be2db033133c5e2dd1a4e8664bf67b19a61bcf6ed28d2b00f433b3f0b4f9585

RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    fonts-liberation \
    libasound2t64 \
    libatk-bridge2.0-0t64 \
    libatk1.0-0t64 \
    libcairo2 \
    libcups2t64 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0t64 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    && curl -fsSL -o /tmp/chrome.zip \
      "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip" \
    && echo "${CHROME_SHA256}  /tmp/chrome.zip" | sha256sum -c - \
    && unzip -q /tmp/chrome.zip -d /opt \
    && mv /opt/chrome-linux64 /opt/chrome \
    && rm /tmp/chrome.zip \
    && apt-get purge -y unzip \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/chromium \
    && printf '%s\n' \
      '#!/bin/sh' \
      '# --no-sandbox: the microVM is the isolation boundary; Chrome userns' \
      '# sandboxing cannot function in-guest. --disable-dev-shm-usage: guest' \
      '# /dev/shm is small, Chrome crashes under load without it.' \
      'exec /opt/chrome/chrome \' \
      '  --headless=new \' \
      '  --remote-debugging-port=${CDP_PORT:-9222} \' \
      '  --remote-allow-origins=* \' \
      '  --no-sandbox \' \
      '  --disable-dev-shm-usage \' \
      '  --disable-gpu \' \
      '  --user-data-dir=/var/lib/chromium \' \
      '  --window-size=1280,800 \' \
      '  ${CHROMIUM_FLAGS:-} \' \
      '  about:blank' \
      > /usr/local/bin/chromium-cdp \
    && chmod 0755 /usr/local/bin/chromium-cdp \
    # Product service, not a readiness gate: claim returns on silkd; CDP
    # comes up a beat later and is polled by the workload (see docs).
    && printf '%s\n' \
      '[Unit]' \
      'Description=headless Chromium with CDP on loopback' \
      'After=silkd.service' \
      '' \
      '[Service]' \
      'Type=simple' \
      'ExecStart=/usr/local/bin/chromium-cdp' \
      'Restart=always' \
      'RestartSec=2s' \
      'LimitNOFILE=65536' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/chromium.service \
    && systemctl enable chromium.service
