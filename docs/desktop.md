# Desktop sandboxes

The `desktop` flavor boots a GNOME session (Ubuntu session on Xvfb, 1920x1080)
with the OSWorld guest server on guest loopback `5000` and the OSWorld app
set (Google Chrome, LibreOffice, GIMP, VLC). A computer-use agent or the
[OSWorld](https://github.com/xlang-ai/OSWorld-V2) harness claims it and
drives the desktop through the same HTTP contract the OSWorld AWS and
docker guests speak — screenshot, AT-SPI accessibility tree, PyAutoGUI
actions — over the existing port relay, so the guest needs no NIC.

```python
sb = client.new("ghcr.io/cocoonstack/sandbox/desktop:24.04", size="2xlarge")
ln = sb.proxy_port("127.0.0.1:0", 5000)
# GET http://127.0.0.1:<port>/screenshot → PNG; /accessibility → AT-SPI XML;
# POST /execute {"command": ["python", "-c", "import pyautogui; ..."]}
```

The claim returns when silkd answers; the session and the guest server come
up a few seconds later — poll `GET /screenshot` until it returns 200.

## Claim shape

- **Lane**: `net=none` — for local tasks (os, office, file work) as it is, and
  with an [egress policy](egress.md) when tasks visit the OSWorld mocked
  websites or the real web; the session's browsers reach the web through the
  relay. Thunderbird's mail policy is pinned to the SOCKS5 door, so a pool that
  runs mail tasks sets `"socks5": true` in its policy. `net=egress` on a
  guarded bridge works the same way: the session's proxy setup waits for the
  host's lane verdict in `/etc/silkd-lane` and follows it, so the locked NIC
  costs the desktop nothing (a CNI-backed lane is told `direct` and routes).
- **Size**: `2xlarge` (8 CPU / 16G) — the t3.xlarge class the OSWorld AWS
  image runs on; the idle session is ~0.5 GB anonymous memory with
  gnome-shell around 290 MB RSS, and the headroom is for the apps.
- **Template**: `ghcr.io/cocoonstack/sandbox/desktop:24.04` — `base:24.04`
  plus GNOME on Xvfb, `osworld-server` at a pinned commit, Chrome from
  Google's apt repo. See [`os-image/desktop/README.md`](../os-image/desktop/README.md)
  for the guest contract.

## Guest ports

| port | service |
|---|---|
| `5000` | osworld-server (`/screenshot`, `/accessibility`, `/execute`, `/setup/*`) |
| `9222` | CDP bridge to the Chrome OSWorld task configs launch with `--remote-debugging-port=1337` |

Both bind guest loopback; reach them with `DialPort`/`ProxyPort`.

## Running the OSWorld harness on sandboxd

OSWorld's `DesktopEnv` drives VMs through a `Provider` whose
`get_ip_address` may return `localhost:<server>:<chromium>:<vnc>:<vlc>`
with per-environment ports — the shape its docker provider uses. A cocoon
provider claims one sandbox per environment, serves the four guest ports on
loopback listeners with `proxy_port`, and implements `revert_to_snapshot` as
release + fresh claim, so a warm pool is the snapshot revert:

```
DesktopEnv(provider_name="cocoon") ── localhost:<p5000> ── sandboxd ── desktop VM :5000
```

Configure it with `SANDBOXD_ADDR`, `SANDBOXD_TOKEN`, `COCOON_TEMPLATE`
(default `desktop:24.04`), `COCOON_SIZE` (default `2xlarge`) and
`COCOON_NET` (default `none`).

## What works, what differs

- Everything from the Ubuntu flavors (exec, files, sessions, git, pty) plus
  the running desktop.
- The desktop user is `user` (uid 1000, passwordless sudo), matching the
  OSWorld AMI; task configs that pipe `CLIENT_PASSWORD` into `sudo -S`
  work with any password.
- Chrome runs `--no-sandbox` (the microVM is the isolation boundary) and a
  fixed `--user-data-dir`, because Chrome 136+ refuses remote debugging on
  the default profile.
- `GTK_USE_PORTAL=0` is a system-wide default: the portal file chooser
  deadlocks the session when Chrome opens one.
- `chromium` is a second launcher onto the same engine, for an agent that
  needs a browser the tasks do not pkill and rebind. It takes the caller's
  profile and debugging port.
- `guest-proxy.service` points the session's browsers and `osworld-server`'s
  commands at silkd's loopback relay on a guest with no device-backed NIC, and
  on a guest whose NIC the host reports as `relay` in `/etc/silkd-lane`; on a
  NIC-bearing guest it holds the session until that verdict lands, up to 300 s,
  longer than the host's own probe and write timeouts, so a verdict that never
  comes means a host that already gave the boot up or an old sandboxd that
  writes none: the unit then fails visibly and the session routes directly. A
  verdict that changes under a running desktop (a node moved from CNI networks
  to a bridge after the claim) reaches silkd's execs at once but the session
  only through a re-claim.
- x86_64 only.
