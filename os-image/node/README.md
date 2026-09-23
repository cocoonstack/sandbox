# node flavor

`base:24.04` plus Node.js 22 LTS (official tarball, sha256-pinned per
architecture, unpacked into `/usr/local`) and the native-module build
toolchain — build-essential, python3, python3-setuptools, unzip — so package installs with
node-gyp steps need only the packages themselves from the network. `node-rt`
runs the same recipe and flattens the result to one layer for
latency-sensitive pools (see `rt`).

Published as `ghcr.io/cocoonstack/sandbox/node:24.04` and
`ghcr.io/cocoonstack/sandbox/node-rt:24.04`, linux/amd64 and linux/arm64.
