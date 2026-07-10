# browser flavor

Headless-browser guest for CDP automation sandboxes: `base:24.04` plus a
pinned [Chrome for Testing](https://googlechromelabs.github.io/chrome-for-testing/)
build launched at boot with its DevTools (CDP) endpoint on guest loopback
`9222`. An agent claims it and connects any CDP client (Playwright
`connectOverCDP`, Puppeteer) through the silkd port relay. The intended
pool shape is `size: large` (4 CPU / 4G) — a persistent Chromium idles at
~150–250 MB RSS.

## Build

- `24.04/Dockerfile` — `FROM base:24.04`; apt the Chromium shared-lib set;
  download Chrome for Testing pinned by version + SHA256 (the
  `install-agent.sh` idiom); bake `/usr/local/bin/chromium-cdp` and
  `chromium.service` (enabled, `multi-user.target`).
- `platforms` — `linux/amd64`; the base lineage is amd64-only.

Why Chrome for Testing: Ubuntu 24.04's `chromium` apt package is a
transitional stub that pulls the snap, and snap cannot install inside a
Docker build. Chrome for Testing is Google's official automation build
with machine-readable pinned download URLs per version.

Version bumps: update `CHROME_VERSION` + `CHROME_SHA256` from
`last-known-good-versions-with-downloads.json`, same operational load as
`COCOON_AGENT_VERSION`.

## CDP on loopback is intentional

Chromium M113+ binds `--remote-debugging-port` to `127.0.0.1` only. In
this stack that is exactly right: silkd's forwarder dials guest loopback,
so CDP is fully reachable through an authorized claim over vsock and never
exposed on a guest NIC — including on the no-network lane.

`chromium.service` is a product service, not a readiness gate: the claim
still returns the moment silkd answers; Chromium comes up a beat later and
is polled by the workload (`e2e/cmd/browsersmoke` shows the pattern).
