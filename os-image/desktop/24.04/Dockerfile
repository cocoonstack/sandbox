# desktop sandbox flavor: base + a GNOME session on Xvfb with the OSWorld
# guest server (osworld-server) on guest loopback 5000, the OSWorld Chrome
# CDP bridge on loopback 9222, and the OSWorld app set (Chrome, LibreOffice,
# GIMP, VLC). The microVM has no display device, so the session runs on a
# virtual framebuffer with Mesa software GL; nothing needs a VT or DRM.
ARG BASE_IMAGE=ghcr.io/cocoonstack/sandbox/base:24.04

FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
# See base: cache-busts the layers below when non-Dockerfile inputs change.
ARG INPUTS_HASH=

# xlang-ai/osworld-server pin; the guest half of the OSWorld control contract.
ARG OSWORLD_SERVER_REF=a3cc3f0c64e463f020d1a44780307e9b46cbcab1
# Thunderbird pins the build the task profiles carry, so none of them migrate.
ARG THUNDERBIRD_VERSION=115.10.1
ARG WPS_BUILD=11.1.0.11723
ARG ZOTERO_VERSION=7.0.11
ARG REAPER_BUILD=735
ARG CODE_VERSION=1.137.0-1788902055
ARG SHOTCUT_VERSION=24.02.29+ds-1build1
ARG OBSIDIAN_VERSION=1.13.7
# The task scores were written by MuseScore 4.6.5; noble's 3.x cannot open them.
ARG MUSESCORE_BUILD=4.6.5.253511702
# noble dropped freecad; the project's own archive still builds for it.
ARG FREECAD_VERSION=2:0.21.2+dfsg1~202407140123~ubuntu24.04.1
ARG FREECAD_FINGERPRINT=A728AE60A790F0EB
ARG SCREEN=1920x1080

RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb xauth x11-utils x11-xserver-utils dbus-x11 dbus-user-session \
    libgl1-mesa-dri libglx-mesa0 \
    gnome-session gnome-shell gnome-settings-daemon gnome-control-center \
    ubuntu-session yaru-theme-gnome-shell yaru-theme-gtk yaru-theme-icon \
    gnome-shell-extension-ubuntu-dock gnome-shell-extension-appindicator \
    nautilus gnome-terminal gnome-text-editor gnome-calculator eog evince \
    fonts-ubuntu fonts-dejavu-core fonts-liberation fonts-noto-color-emoji \
    at-spi2-core gir1.2-atspi-2.0 python3-pyatspi gsettings-desktop-schemas dconf-cli \
    gnome-screenshot scrot wmctrl xdotool xclip ffmpeg socat sudo xdg-user-dirs locales \
    python3 python3-venv python3-tk python3-dev python3-gi \
    libreoffice gimp vlc \
    pulseaudio pulseaudio-utils \
    gnupg \
    && install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
      | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
      > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && locale-gen en_US.UTF-8 && update-locale LANG=en_US.UTF-8 \
    # Chrome userns sandboxing cannot function in-guest; the microVM is the
    # isolation boundary. OSWorld launches "google-chrome" by name, so the
    # wrapper shadows the packaged launcher on PATH and in the .desktop entry.
    # Chrome 136+ ignores --remote-debugging-port on the default profile dir,
    # and OSWorld task configs pass that flag without a --user-data-dir.
    # An uninitialized profile opens the terms-of-service window instead of
    # starting, so the debug port never binds; --no-first-run skips it.
    && printf '%s\n' \
      '#!/bin/sh' \
      'exec /opt/google/chrome/google-chrome --user-data-dir=/home/user/.config/google-chrome-osworld --no-sandbox --disable-dev-shm-usage --disable-gpu --test-type --no-first-run --no-default-browser-check "$@"' \
      > /usr/local/bin/google-chrome \
    && chmod 0755 /usr/local/bin/google-chrome \
    # the deb ships two entries and the hidden one wins the mime defaults
    && sed -i 's|^Exec=/usr/bin/google-chrome-stable|Exec=/usr/local/bin/google-chrome|' \
      /usr/share/applications/google-chrome.desktop \
      /usr/share/applications/com.google.Chrome.desktop \
    # An agent driving the desktop needs a browser of its own: OSWorld tasks
    # pkill and rebind the Chrome on 1337/9222 mid-run, so a second engine is
    # what keeps the agent's CDP session alive. Callers launch it as "chromium"
    # (the upstream guest image ships the snap) and pass their own profile and
    # debugging port; a profile they have just created needs the same
    # first-run suppression, or the terms-of-service window takes the port's
    # place.
    && printf '%s\n' \
      '#!/bin/sh' \
      'exec /opt/google/chrome/google-chrome --no-sandbox --disable-dev-shm-usage --disable-gpu --test-type --no-first-run --no-default-browser-check "$@"' \
      > /usr/local/bin/chromium \
    && chmod 0755 /usr/local/bin/chromium \
    # osworld-server: pinned checkout, venv on system site-packages so the
    # apt pyatspi binding is importable; PyAutoGUI/python3-xlib pins mirror
    # its requirements.txt.
    && git clone -q https://github.com/xlang-ai/osworld-server.git /opt/osworld-server \
    && git -C /opt/osworld-server checkout -q "${OSWORLD_SERVER_REF}" \
    && rm -rf /opt/osworld-server/.git \
    && python3 -m venv --system-site-packages /opt/osworld-server/.venv \
    && /opt/osworld-server/.venv/bin/pip install --no-cache-dir \
      "python3-xlib==0.15" "PyAutoGUI==0.9.54" Pillow flask fastapi uvicorn websockets numpy lxml requests \
    # The desktop user is uid 1000 like the OSWorld AMI (units below assume
    # /run/user/1000); the stock ubuntu user gives up the uid.
    && if id ubuntu >/dev/null 2>&1; then userdel -r ubuntu; fi \
    && useradd -m -u 1000 -s /bin/bash -G sudo,audio,video user \
    && echo 'user:password' | chpasswd \
    && echo 'user ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/user \
    && chmod 0440 /etc/sudoers.d/user \
    && install -d -o user -g user /home/user/Desktop /home/user/Documents /home/user/Downloads \
    # python-xlib needs an entry for the display, written once Xvfb is up; a bare file only turns the failure into a stdout warning
    && install -o user -g user -m 0600 /dev/null /home/user/.Xauthority \
    && chown -R user:user /opt/osworld-server \
    && install -d /var/lib/systemd/linger && touch /var/lib/systemd/linger/user \
    # Session defaults: a11y on (AT-SPI tree), no lock/idle/animations.
    && install -d /etc/dconf/profile /etc/dconf/db/local.d \
    && printf 'user-db:user\nsystem-db:local\n' > /etc/dconf/profile/user \
    && printf '%s\n' \
      '[org/gnome/desktop/interface]' \
      'toolkit-accessibility=true' \
      'enable-animations=false' \
      '[org/gnome/desktop/session]' \
      'idle-delay=uint32 0' \
      '[org/gnome/desktop/screensaver]' \
      'lock-enabled=false' \
      'idle-activation-enabled=false' \
      '[org/gnome/settings-daemon/plugins/power]' \
      'sleep-inactive-ac-type='"'"'nothing'"'"'' \
      '[org/gnome/shell]' \
      'welcome-dialog-last-shown-version='"'"'999'"'"'' \
      > /etc/dconf/db/local.d/00-sandbox \
    && dconf update \
    # the GTK file chooser routed through xdg-desktop-portal-gtk deadlocks the
    # session when Chrome opens one; a system-wide default covers both the
    # session and the commands osworld-server spawns.
    && install -d /etc/systemd/system.conf.d \
    && printf '[Manager]\nDefaultEnvironment=GTK_USE_PORTAL=0\n' \
      > /etc/systemd/system.conf.d/10-gtk-portal.conf \
    && printf 'GTK_USE_PORTAL=0\n' >> /etc/environment \
    # Only silkd carries the base image's proxy variables, and only into the
    # execs it spawns, so the session's browsers and the commands
    # osworld-server runs have no way out on the none lane. The NIC test is
    # silkd's own: a lane that routes directly must not be pointed at a relay
    # whose host side is closed.
    && printf '%s\n' \
      'http_proxy=http://127.0.0.1:3128' \
      'https_proxy=http://127.0.0.1:3128' \
      'no_proxy=localhost,127.0.0.1,::1' \
      > /usr/local/share/guest-proxy.env \
    && printf '%s\n' \
      'Acquire::http::Proxy "http://127.0.0.1:3128";' \
      'Acquire::https::Proxy "http://127.0.0.1:3128";' \
      > /usr/local/share/guest-proxy.apt \
    && printf '%s\n' \
      '[system/proxy]' \
      'mode='"'"'manual'"'"'' \
      'ignore-hosts=['"'"'localhost'"'"','"'"'127.0.0.0/8'"'"','"'"'::1'"'"']' \
      '[system/proxy/http]' \
      'host='"'"'127.0.0.1'"'"'' \
      'port=3128' \
      '[system/proxy/https]' \
      'host='"'"'127.0.0.1'"'"'' \
      'port=3128' \
      '[system/proxy/socks]' \
      'host='"'"'127.0.0.1'"'"'' \
      'port=1080' \
      > /usr/local/share/guest-proxy.dconf \
    # a NIC alone says nothing about routing: the host writes /etc/silkd-lane once silkd answers; the wait outlives the host's
    # probe (90 s) and write (120 s) timeouts, so a guest still waiting belongs to a boot the host already gave up on
    && printf '%s\n' \
      '#!/bin/sh' \
      'set -e' \
      ': > /run/guest-proxy.env' \
      'rm -f /usr/lib/thunderbird/distribution/policies.json' \
      'nic=0' \
      'for n in /sys/class/net/*; do if [ -e "$n/device" ]; then nic=1; fi; done' \
      'if [ $nic = 1 ]; then' \
      '  i=0' \
      '  while [ ! -e /etc/silkd-lane ] && [ $i -lt 3000 ]; do sleep 0.1; i=$((i+1)); done' \
      '  if [ ! -e /etc/silkd-lane ]; then echo "guest-proxy: no lane verdict from the host after 300 s; routing directly" >&2; exit 1; fi' \
      '  [ "$(cat /etc/silkd-lane)" = relay ] || exit 0' \
      'fi' \
      'cat /usr/local/share/guest-proxy.env > /run/guest-proxy.env' \
      'cat /usr/local/share/guest-proxy.dconf > /etc/dconf/db/local.d/01-proxy' \
      'dconf update' \
      'cat /usr/local/share/guest-proxy.apt > /etc/apt/apt.conf.d/01-guest-proxy' \
      'cat /usr/local/share/guest-proxy.thunderbird > /usr/lib/thunderbird/distribution/policies.json' \
      > /usr/local/sbin/guest-proxy-setup \
    && chmod 0755 /usr/local/sbin/guest-proxy-setup \
    && printf '%s\n' \
      '[Unit]' \
      'Description=point the desktop at the guest proxy relay' \
      'After=silkd.service' \
      'Before=desktop-session.service osworld-server.service' \
      '' \
      '[Service]' \
      'Type=oneshot' \
      'RemainAfterExit=yes' \
      'ExecStart=/usr/local/sbin/guest-proxy-setup' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/guest-proxy.service \
    && printf '%s\n' \
      '[Unit]' \
      'Description=virtual X display for the desktop session' \
      'After=silkd.service' \
      '' \
      '[Service]' \
      'Type=simple' \
      "ExecStart=/usr/bin/Xvfb :1 -screen 0 ${SCREEN}x24 -ac -noreset -nolisten tcp +extension GLX +extension RANDR +render" \
      'ExecStartPost=/bin/sh -c "for i in $(seq 1 50); do [ -S /tmp/.X11-unix/X1 ] && break; sleep 0.1; done; runuser -u user -- xauth -f /home/user/.Xauthority add :1 . $(mcookie)"' \
      'Restart=always' \
      'RestartSec=1s' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/xvfb.service \
    && printf '%s\n' \
      '[Unit]' \
      'Description=GNOME session on the virtual display' \
      'Requires=xvfb.service user@1000.service' \
      'After=guest-proxy.service' \
      'After=xvfb.service user@1000.service' \
      '' \
      '[Service]' \
      'Type=simple' \
      'User=user' \
      'WorkingDirectory=/home/user' \
      'EnvironmentFile=-/run/guest-proxy.env' \
      'Environment=DISPLAY=:1' \
      'Environment=HOME=/home/user' \
      'Environment=XDG_RUNTIME_DIR=/run/user/1000' \
      'Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus' \
      'Environment=XDG_SESSION_TYPE=x11' \
      'Environment=XDG_SESSION_CLASS=user' \
      'Environment=XDG_CURRENT_DESKTOP=ubuntu:GNOME' \
      'Environment=GNOME_SHELL_SESSION_MODE=ubuntu' \
      'Environment=LIBGL_ALWAYS_SOFTWARE=1' \
      'Environment=GTK_MODULES=gail:atk-bridge' \
      'Environment=QT_ACCESSIBILITY=1' \
      'Environment=NO_AT_BRIDGE=0' \
      'ExecStartPre=/usr/bin/systemctl --user import-environment DISPLAY XDG_SESSION_TYPE XDG_CURRENT_DESKTOP GNOME_SHELL_SESSION_MODE LIBGL_ALWAYS_SOFTWARE GTK_MODULES QT_ACCESSIBILITY NO_AT_BRIDGE' \
      'ExecStart=/usr/bin/gnome-session --session=ubuntu' \
      'Restart=always' \
      'RestartSec=2s' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/desktop-session.service \
    && printf '%s\n' \
      '[Unit]' \
      'Description=OSWorld guest server on loopback 5000' \
      'After=desktop-session.service' \
      'Wants=desktop-session.service' \
      '' \
      '[Service]' \
      'Type=simple' \
      'User=user' \
      'WorkingDirectory=/opt/osworld-server' \
      'EnvironmentFile=-/run/guest-proxy.env' \
      'Environment=DISPLAY=:1' \
      'Environment=HOME=/home/user' \
      'Environment=XDG_RUNTIME_DIR=/run/user/1000' \
      'Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus' \
      'Environment=PATH=/opt/osworld-server/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
      # without it xdg-open here picks a different application than the session
      'Environment=XDG_CURRENT_DESKTOP=ubuntu:GNOME' \
      'Environment=OSWORLD_SERVER_HOST=127.0.0.1' \
      'Environment=OSWORLD_SERVER_PORT=5000' \
      'ExecStart=/opt/osworld-server/.venv/bin/python /opt/osworld-server/main.py' \
      'Restart=always' \
      'RestartSec=2s' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/osworld-server.service \
    # OSWorld task configs start Chrome with --remote-debugging-port=1337 and
    # reach CDP on 9222; the bridge is what the official guest image runs.
    && printf '%s\n' \
      '[Unit]' \
      'Description=OSWorld Chrome CDP bridge 9222 -> 1337' \
      'After=xvfb.service' \
      '' \
      '[Service]' \
      'Type=simple' \
      'ExecStart=/usr/bin/socat TCP-LISTEN:9222,bind=127.0.0.1,fork,reuseaddr TCP:127.0.0.1:1337' \
      'Restart=always' \
      'RestartSec=2s' \
      '' \
      '[Install]' \
      'WantedBy=multi-user.target' \
      > /etc/systemd/system/cdp-bridge.service \
    && systemctl enable guest-proxy.service xvfb.service desktop-session.service osworld-server.service cdp-bridge.service

# The applications and command-line tools the OSWorld task set drives, beyond
# what the flavor already carried. bsdextrautils and xdg-utils: WPS's postinst
# exits 127 without them and wedges apt. libdbus-glib-1-2: Thunderbird and
# Zotero dlopen it, undeclared. WPS blocks on a modal EULA and nags about
# absent symbol fonts. Archive packages take the version the suite froze.
RUN apt-get update && apt-get install -y --no-install-recommends \
    bsdextrautils xdg-utils desktop-file-utils shared-mime-info bzip2 xz-utils \
    libdbus-glib-1-2 tinyproxy shotcut=${SHOTCUT_VERSION} \
    unzip zip poppler-utils imagemagick sqlite3 psmisc \
    python-is-python3 python3-pip nodejs npm \
    blender kicad solvespace labplot geogebra logisim openboard \
    gnome-calendar \
    && install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/code stable main" \
      > /etc/apt/sources.list.d/vscode.list \
    && curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&options=mr&search=0x${FREECAD_FINGERPRINT}" \
      | gpg --dearmor -o /etc/apt/keyrings/freecad.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/freecad.gpg] https://ppa.launchpadcontent.net/freecad-maintainers/freecad-stable/ubuntu noble main" \
      > /etc/apt/sources.list.d/freecad.list \
    && apt-get update && apt-get install -y --no-install-recommends code=${CODE_VERSION} \
      "freecad=${FREECAD_VERSION}" python3-numpy python3-scipy \
    # the task profiles name /usr/lib/thunderbird, one task runs /usr/bin/thunderbird
    && curl -fsSL -o /tmp/thunderbird.tar.bz2 \
      "https://download-installer.cdn.mozilla.net/pub/thunderbird/releases/${THUNDERBIRD_VERSION}/linux-x86_64/en-US/thunderbird-${THUNDERBIRD_VERSION}.tar.bz2" \
    && install -d /usr/lib/thunderbird \
    && tar -xjf /tmp/thunderbird.tar.bz2 -C /usr/lib/thunderbird --strip-components=1 \
    && ln -sf /usr/lib/thunderbird/thunderbird /usr/bin/thunderbird \
    && printf '%s\n' \
      '[Desktop Entry]' \
      'Type=Application' \
      'Name=Thunderbird Mail' \
      'Exec=/usr/bin/thunderbird %u' \
      'Icon=/usr/lib/thunderbird/chrome/icons/default/default128.png' \
      'Terminal=false' \
      'StartupWMClass=thunderbird' \
      'Categories=Network;Email;' \
      'MimeType=x-scheme-handler/mailto;message/rfc822;' \
      > /usr/share/applications/thunderbird.desktop \
    # a task seeds its own profile, so the proxy has to come from policy
    # the task set predates PEP 668 and pip-installs into the system interpreter
    && printf '%s\n' '[global]' 'break-system-packages = true' > /etc/pip.conf \
    && install -d /usr/lib/thunderbird/distribution \
    && printf '%s\n' \
      '{"policies":{"Proxy":{"Mode":"manual","SOCKSProxy":"127.0.0.1:1080",' \
      '"SOCKSVersion":5,"UseProxyForDNS":true,"Locked":true,' \
      '"Passthrough":"localhost, 127.0.0.1"}}}' \
      > /usr/local/share/guest-proxy.thunderbird \
    && curl -fsSL -o /tmp/wps.deb \
      "https://wdl1.pcfg.cache.wpscdn.com/wpsdl/wpsoffice/download/linux/${WPS_BUILD##*.}/wps-office_${WPS_BUILD}.XA_amd64.deb" \
    && apt-get install -y --no-install-recommends /tmp/wps.deb \
    # install -d owns only the operands, so .config has to be named too
    && install -d -o user -g user -m 0755 /home/user/.config /home/user/.config/Kingsoft \
    && printf '%s\n' \
      '[6.0]' \
      'common\AcceptedEULA=true' \
      'common\system_check\no_necessary_symbol_fonts=false' \
      > /home/user/.config/Kingsoft/Office.conf \
    && chown user:user /home/user/.config/Kingsoft/Office.conf \
    && curl -fsSL -o /tmp/zotero.tar.bz2 \
      "https://www.zotero.org/download/client/dl?channel=release&platform=linux-x86_64&version=${ZOTERO_VERSION}" \
    && install -d /opt/zotero \
    && tar -xjf /tmp/zotero.tar.bz2 -C /opt/zotero --strip-components=1 \
    && ln -sf /opt/zotero/zotero /usr/local/bin/zotero \
    && printf '%s\n' \
      '[Desktop Entry]' \
      'Type=Application' \
      'Name=Zotero' \
      'Exec=/opt/zotero/zotero -url %U' \
      'Icon=/opt/zotero/chrome/icons/default/default256.png' \
      'Terminal=false' \
      'StartupWMClass=Zotero' \
      'Categories=Office;' \
      'MimeType=x-scheme-handler/zotero;text/x-bibtex;application/x-bibtex;' \
      > /usr/share/applications/zotero.desktop \
    && install -d /opt/zotero/app/defaults/preferences \
    && printf '%s\n' \
      'pref("extensions.zotero.firstRun2", false);' \
      'pref("extensions.zoteroOpenOfficeIntegration.skipInstallation", true);' \
      > /opt/zotero/app/defaults/preferences/osworld.js \
    && curl -fsSL -o /tmp/musescore.AppImage \
      "https://github.com/musescore/MuseScore/releases/download/v${MUSESCORE_BUILD%.*}/MuseScore-Studio-${MUSESCORE_BUILD}-x86_64.AppImage" \
    && chmod 0755 /tmp/musescore.AppImage \
    && cd /opt && /tmp/musescore.AppImage --appimage-extract >/dev/null \
    && mv /opt/squashfs-root /opt/musescore4 && cd / \
    && printf '%s\n' '#!/bin/sh' 'exec /opt/musescore4/AppRun "$@"' > /usr/local/bin/musescore \
    && chmod 0755 /usr/local/bin/musescore && ln -sf musescore /usr/local/bin/mscore \
    && printf '%s\n' \
      '[Desktop Entry]' \
      'Type=Application' \
      'Name=MuseScore Studio' \
      'Exec=/usr/local/bin/musescore %F' \
      'Icon=/opt/musescore4/.DirIcon' \
      'Terminal=false' \
      'Categories=AudioVideo;Audio;' \
      'MimeType=application/x-musescore;application/x-musescore+xml;' \
      > /usr/share/applications/musescore.desktop \
    # the guest root is overlayfs, which overlay2 cannot stack on; dockerd reads
    # the lane-selected proxy environment once a task installs docker
    && install -d /etc/docker /etc/systemd/system/docker.service.d \
    && printf '%s\n' '{"storage-driver": "vfs", "features": {"containerd-snapshotter": false}}' \
      > /etc/docker/daemon.json \
    && printf '%s\n' '[Service]' \
      'EnvironmentFile=-/run/guest-proxy.env' \
      > /etc/systemd/system/docker.service.d/proxy.conf \
    && curl -fsSL -o /tmp/obsidian.deb \
      "https://github.com/obsidianmd/obsidian-releases/releases/download/v${OBSIDIAN_VERSION}/obsidian_${OBSIDIAN_VERSION}_amd64.deb" \
    && apt-get install -y --no-install-recommends /tmp/obsidian.deb \
    # Electron's sandbox cannot start in-guest, the same as Chrome's
    && printf '%s\n' '#!/bin/sh' 'exec /opt/Obsidian/obsidian --no-sandbox "$@"' > /usr/local/bin/obsidian \
    && chmod 0755 /usr/local/bin/obsidian \
    && sed -i 's|^Exec=/opt/Obsidian/obsidian|Exec=/usr/local/bin/obsidian|' \
      /usr/share/applications/md.obsidian.Obsidian.desktop \
    # both Obsidian tasks use this vault and never register it; unregistered, the app opens on its chooser
    && install -d -o user -g user -m 0755 /home/user/Documents "/home/user/Documents/Obsidian Vault" \
      /home/user/.config/obsidian \
    && printf '%s' '{"vaults":{"6a2f4c1e9b7d3a05":{"path":"/home/user/Documents/Obsidian Vault","ts":1789000000000,"open":true}}}' \
      > /home/user/.config/obsidian/obsidian.json \
    && chown user:user /home/user/.config/obsidian/obsidian.json \
    && curl -fsSL -o /tmp/reaper.tar.xz \
      "https://www.reaper.fm/files/7.x/reaper${REAPER_BUILD}_linux_x86_64.tar.xz" \
    && install -d /tmp/reaper \
    && tar -xJf /tmp/reaper.tar.xz -C /tmp/reaper --strip-components=1 \
    && /tmp/reaper/install-reaper.sh --install /opt --integrate-desktop --quiet \
    && ln -sf /opt/REAPER/reaper /usr/local/bin/reaper \
    # the CAD evaluators are entry-guarded on __main__, which FreeCAD's own
    # launcher does not give a script: it runs one under its file name
    && printf '%s\n' \
      '#!/bin/sh' \
      'exec env PYTHONPATH=/usr/lib/freecad-python3/lib python3 "$@"' \
      > /usr/local/bin/freecadcmd \
    && chmod 0755 /usr/local/bin/freecadcmd \
    # the harness starts its own instance per task against a residential upstream
    && systemctl disable tinyproxy.service \
    && update-desktop-database /usr/share/applications \
    && apt-get update \
    && rm -rf /tmp/thunderbird.tar.bz2 /tmp/wps.deb /tmp/zotero.tar.bz2 /tmp/musescore.AppImage /tmp/obsidian.deb \
      /tmp/reaper /tmp/reaper.tar.xz
