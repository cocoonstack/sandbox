# Browser sandboxes

The `browser` flavor boots with headless Chromium (Chrome for Testing)
already running and its CDP endpoint on guest loopback `9222`. An agent
claims it and drives it with any CDP client — Playwright `connectOverCDP`,
Puppeteer, Stagehand — through the existing port relay. Two things no
hosted browser service offers: checkpoint/branch of a live browser, and
CDP on the no-network lane over vsock.

```go
sb, err := client.New(ctx, "ghcr.io/cocoonstack/sandbox/browser:24.04",
    sandbox.WithNetwork(sandbox.NetEgress), sandbox.WithSize(sandbox.Large))
```

The claim returns when silkd answers (the usual tiers); Chromium finishes
starting a beat later — poll `/json/version` until it answers:

```go
pc, err := sb.DialPort(ctx, 9222)
// GET /json/version with Host: localhost → {"Browser":"HeadlessChrome/150…"}
```

## Claim shape

- **Lane**: any. `net=egress` to browse the real web; `net=none` (vsock
  only, zero NIC) drives local/bundled content — CDP rides the relay either
  way.
- **Size**: `large` (4 CPU / 4G) — a persistent Chromium idles at
  ~150–250 MB RSS and real pages want headroom; `xlarge` for heavy
  multi-tab work.
- **Template**: `ghcr.io/cocoonstack/sandbox/browser:24.04` —
  `base:24.04` plus a pinned Chrome for Testing launched by a systemd
  unit (`--headless=new`, CDP on `127.0.0.1:9222`).

## CDP access

Chromium M113+ binds the DevTools port to loopback only — here that is a
feature: silkd's forwarder dials guest loopback, so CDP is reachable only
through an authorized claim and never sits on a guest NIC.

- **`ProxyPort` (real CDP clients)** — Chrome's Host-header allowlist
  accepts the IP:

  ```go
  ln, err := sb.ProxyPort(ctx, "127.0.0.1:0", 9222)
  // playwright: chromium.connectOverCDP("http://" + ln.Addr().String())
  ```

- **`DialPort` (raw, works on the no-network lane)** — send HTTP with
  `Host: localhost`; `GET /json/version`, `PUT /json/new?<url>` to open a
  target, then a WebSocket to the returned `webSocketDebuggerUrl`.

## Checkpoint / branch a live browser

`sb.Checkpoint` + `ck.New` fork a warmed browser — in-memory tabs,
cookies, localStorage — in checkpoint-restore time; the branch answers
`/json/version` without relaunching Chrome. Hosted browser services
cold-start a browser per session; here a warmed profile is a template you
branch from.

## What works, what differs

- Everything from the Ubuntu flavors (exec, files, sessions, git, pty)
  plus the running Chromium.
- **Preview URLs are not raw CDP**: the preview proxy rewrites `Host` to
  `sandbox-id:port`, which Chrome's DevTools allowlist rejects. Use
  `ProxyPort`/`DialPort` for CDP; preview URLs serve human-facing HTTP the
  workload chooses to expose (a live-view page, a screenshot server).
- Guest env knobs on the unit: `CDP_PORT` (default 9222),
  `CHROMIUM_FLAGS` (extra flags).
- No stealth build in v1: headless Chromium is fingerprintable; this
  flavor targets automation, not anti-bot evasion.
- One browser per sandbox by design — the VM is the isolation and
  checkpoint unit; open tabs via CDP `Target.createTarget`.
- x86_64 only.

The hardware acceptance is `e2e/cmd/browsersmoke`: claim →
`/json/version` over the relay → open a target via `PUT /json/new` →
checkpoint/branch re-verification.
