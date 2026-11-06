# browser flavor

Headless-browser guest: `base:24.04` plus a pinned Chrome for Testing build
launched at boot. Claim shape, CDP access and behavior are in
[docs/browser.md](../../docs/browser.md); this file holds the build notes.

## Build

- `24.04/Dockerfile` — `FROM base:24.04`; apt the Chromium shared-lib set;
  download Chrome for Testing pinned by version + SHA256 (the
  `install-agent.sh` idiom); bake `/usr/local/bin/chromium-cdp` and
  `chromium.service` (enabled, `multi-user.target`).
- `platforms` — `linux/amd64`; this flavor is amd64-only (Chrome for Testing),
  unlike the multi-arch `base`.

Why Chrome for Testing: Ubuntu 24.04's `chromium` apt package is a
transitional stub that pulls the snap, and snap cannot install inside a
Docker build. Chrome for Testing is Google's official automation build
with machine-readable pinned download URLs per version.

Version bumps: update `CHROME_VERSION` + `CHROME_SHA256` from
`last-known-good-versions-with-downloads.json`, same operational load as
`COCOON_AGENT_VERSION`.
