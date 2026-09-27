# e2b's code interpreter on the e2b data plane: envd, a Jupyter server and the
# code-interpreter API on 49999, both started at boot. The recipe is
# e2b-dev/code-interpreter template/ at CI_RECIPE_SHA, fetched and checksummed
# here, with PATCHES applied; constraints.txt and server-constraints.txt pin the
# full sets it resolves to, so a rebuild of one commit installs the same
# packages. Python kernel only: the recipe's javascript, R and Java kernels
# install from unpinned sources.
#
# amd64 only: e2b publishes no arm64 build of envd.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04
ARG ENVD_VERSION=v0.8.0
ARG ENVD_SHA256=752cfbc7ba66649ec1bffdc9b59529dd3527e7d3899c69351e05bd21dfc0dbf0
ARG CI_RECIPE_SHA=f56a1edf750e20a96f847df57e0063eeeb5e13c3
ARG CI_RECIPE_SHA256=3af064c471e41f72a3c56920f61219c4259ef4aacf466f3e8687b5af30d68c48

FROM ${BASE_IMAGE} AS src
ARG ENVD_VERSION
ARG ENVD_SHA256
ARG CI_RECIPE_SHA
ARG CI_RECIPE_SHA256
ENV DEBIAN_FRONTEND=noninteractive
# [repro] nothing writes a .pyc until compileall writes them all, hash-checked and with fixed set order.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0

COPY constraints.txt server-constraints.txt javascript-context.patch /tmp/ci/
COPY systemd/jupyter.service systemd/code-interpreter.service /etc/systemd/system/

RUN curl -fsSL -o /usr/local/bin/envd \
      "https://storage.googleapis.com/e2b-artifact-binaries/envd/${ENVD_VERSION}/envd" && \
    echo "${ENVD_SHA256}  /usr/local/bin/envd" | sha256sum -c - && \
    chmod 0755 /usr/local/bin/envd && \
    echo "${ENVD_VERSION#v}" > /etc/envd-version && \
    apt-get update && apt-get install -y --no-install-recommends \
      nftables sudo jq util-linux fonts-noto-cjk build-essential patch \
      python3 python3-venv python3-dev && \
    curl -fsSL -o /tmp/ci/recipe.tar.gz \
      "https://github.com/e2b-dev/code-interpreter/archive/${CI_RECIPE_SHA}.tar.gz" && \
    echo "${CI_RECIPE_SHA256}  /tmp/ci/recipe.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/ci/recipe.tar.gz -C /tmp/ci --strip-components=1 \
      "code-interpreter-${CI_RECIPE_SHA}/template" "code-interpreter-${CI_RECIPE_SHA}/LICENSE" && \
    patch -d /tmp/ci -p1 < /tmp/ci/javascript-context.patch && \
    # [envd] loopback only, as in e2b-rt; uvicorn binds the interpreter API to 0.0.0.0 too.
    printf '%s\n' \
      'table inet e2bguard {' \
      '  chain input {' \
      '    type filter hook input priority filter; policy accept;' \
      '    iif != "lo" tcp dport { 49983, 49999, 8888 } drop' \
      '    iif != "lo" ip daddr 169.254.169.254 tcp dport 80 drop' \
      '  }' \
      '}' \
      > /etc/nftables-envd.conf && \
    printf '%s\n' \
      '[Unit]' \
      'Description=drop non-loopback traffic to envd and the code interpreter' \
      'DefaultDependencies=no' \
      'Before=envd.service silkd.service' \
      '' \
      '[Service]' \
      'Type=oneshot' \
      'RemainAfterExit=yes' \
      'ExecStart=/usr/sbin/nft -f /etc/nftables-envd.conf' \
      'ExecStart=/usr/sbin/ip addr replace 169.254.169.254/32 dev lo' \
      '' \
      '[Install]' \
      'WantedBy=sysinit.target' \
      > /etc/systemd/system/envd-guard.service && \
    printf '%s\n' \
      '[Unit]' \
      'Description=envd (e2b data-plane daemon, loopback only)' \
      'Documentation=https://github.com/e2b-dev/infra/tree/main/packages/envd' \
      'DefaultDependencies=no' \
      'After=envd-guard.service' \
      'Requires=envd-guard.service' \
      'Before=basic.target' \
      '' \
      '[Service]' \
      'Type=simple' \
      'ExecStart=/usr/local/bin/envd -no-cgroups' \
      'Restart=always' \
      'RestartSec=2s' \
      'LimitNOFILE=65536' \
      '' \
      '[Install]' \
      'WantedBy=sysinit.target' \
      > /etc/systemd/system/envd.service && \
    useradd -m -s /bin/bash user && \
    echo 'user ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/user && chmod 0440 /etc/sudoers.d/user && \
    python3 -m venv /opt/python && \
    /opt/python/bin/pip install --no-cache-dir --no-compile -r /tmp/ci/template/requirements.txt -c /tmp/ci/constraints.txt && \
    /opt/python/bin/python -m ipykernel install --sys-prefix --name python3 && \
    mv /tmp/ci/template/server /root/.server && \
    python3 -m venv /root/.server/.venv && \
    /root/.server/.venv/bin/pip install --no-cache-dir --no-compile -r /root/.server/requirements.txt -c /tmp/ci/server-constraints.txt && \
    /opt/python/bin/python -m compileall -q -f -j 1 --invalidation-mode unchecked-hash /opt/python/lib /root/.server && \
    install -D -m 0644 /tmp/ci/template/matplotlibrc /root/.config/matplotlib/.matplotlibrc && \
    install -D -m 0755 /tmp/ci/template/jupyter-healthcheck.sh /root/.jupyter/jupyter-healthcheck.sh && \
    install -D -m 0644 /tmp/ci/template/jupyter_server_config.py /root/.jupyter/jupyter_server_config.py && \
    install -D -m 0644 /tmp/ci/template/ipython_kernel_config.py /root/.ipython/profile_default/ipython_kernel_config.py && \
    install -D -m 0644 -t /root/.ipython/profile_default/startup /tmp/ci/template/startup_scripts/*.py && \
    install -D -m 0644 /tmp/ci/LICENSE /usr/share/doc/e2b-code-interpreter/LICENSE && \
    systemctl enable envd-guard.service envd.service jupyter.service code-interpreter.service && \
    rm -rf /tmp/* /var/lib/apt/lists/* /var/log/apt /var/log/dpkg.log /var/log/alternatives.log \
      /var/cache/ldconfig/aux-cache /var/cache/debconf/*-old /root/.cache

FROM scratch
COPY --from=src / /

CMD ["/sbin/init"]
