# desktop flavor

GNOME desktop guest for OSWorld-style computer-use sandboxes: `base:24.04`
plus a GNOME session (Ubuntu session, dock, Yaru) on an Xvfb `:1` display at
1920x1080 with Mesa software GL, the OSWorld guest server
([xlang-ai/osworld-server](https://github.com/xlang-ai/osworld-server),
pinned commit) on guest loopback `5000`, the OSWorld Chrome CDP bridge on
loopback `9222`, and the OSWorld app set: Google Chrome, LibreOffice, GIMP,
VLC, Thunderbird, VS Code, Zotero, Obsidian, Shotcut, FreeCAD, WPS Office,
MuseScore Studio, REAPER, Blender, KiCad, SolveSpace, LabPlot, GeoGebra,
Logisim, OpenBoard and GNOME Calendar.
The intended pool shape is `size: 2xlarge` (8 CPU / 16G); the session idles
at ~0.5 GB anonymous memory with gnome-shell around 290 MB RSS.

An OSWorld harness or the Sai driver claims a sandbox and reaches the guest
server through the silkd port relay (`DialPort`/`ProxyPort` 5000, 9222), so
the `none` lane works for local tasks; tasks that visit the OSWorld mocked
websites or the web add an egress policy, on the `none` lane or a guarded
bridge `egress` lane alike (see `docs/desktop.md`).

## Guest contract

- `user` (uid 1000, password `password`, passwordless sudo) owns the
  session; `/run/user/1000/bus` is the session bus the AT-SPI tree hangs off.
- `/opt/osworld-server/.venv/bin/python` has PyAutoGUI + python-xlib; the
  server's `PATH` puts it first so task configs that run `python -c "import
  pyautogui; ..."` resolve to it.
- `google-chrome` is a wrapper adding `--no-sandbox` (no userns in-guest) and
  a fixed `--user-data-dir`: Chrome 136+ ignores `--remote-debugging-port` on
  the default profile, and OSWorld task configs pass only the port. The
  `cdp-bridge` unit forwards loopback `9222` to Chrome's `1337`.
- dconf defaults turn on `toolkit-accessibility`, and turn off animations,
  idle, screen lock, and the welcome dialog.
- `guest-proxy.service` writes the session's proxy environment, the dconf
  proxy entry and the apt proxy when the guest has no device-backed NIC, or
  when the host's verdict in `/etc/silkd-lane` reads `relay`; on a NIC-bearing
  guest it holds the session for that verdict up to 300 s and fails visibly
  without it. Thunderbird's policy pins mail to the SOCKS5 door on `1080`, so
  a pool that runs mail tasks opts its policy into `socks5`.
- dockerd, once a task installs it, is pinned to the `vfs` storage driver and
  reads the lane-selected `/run/guest-proxy.env` through its systemd drop-in:
  the guest root is overlayfs, which overlay2 cannot stack on.
  `/etc/pip.conf` sets `break-system-packages`, since the task set pip-installs
  into the system interpreter.

## Build

- `24.04/Dockerfile` — `FROM base:24.04`; apt the GNOME/X/AT-SPI/app set,
  Google Chrome from Google's apt repo, `osworld-server` at
  `OSWORLD_SERVER_REF` into a system-site-packages venv; bake the
  `guest-proxy`, `xvfb`, `desktop-session`, `osworld-server` and `cdp-bridge`
  units. A second layer adds the archive- and vendor-pinned applications the
  task set drives.
- `platforms` — `linux/amd64`; Chrome is amd64-only.

Services are product services, not readiness gates: the claim returns on
silkd; the session and `osworld-server` come up a few seconds later and are
polled by the workload (`GET /screenshot` returning 200).
