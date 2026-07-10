# android flavor

Android guest for UI-automation sandboxes: the cocoon project's official
redroid image (`ghcr.io/cocoonstack/cocoon/android:15.0`, upstream
redroid 15 rootfs + cocoon boot chain, built from `cocoon/os-image/android/`)
plus silkd, claimable like any other template. Remote access is adb on
port 5555 — no VNC in this lineage. The intended pool shape is
`size: xlarge` (4 CPU / 8G).

## Build

Unlike the Ubuntu flavors, the guest has no glibc and no systemd:

- `15/Dockerfile` — `FROM ${ANDROID_BASE_IMAGE}` (defaults to the pinned
  public base), `COPY --from` the silkd carrier's musl-static
  `/silkd-static` binary to `/system/bin/silkd`, and drop `silkd.rc` into
  `/system/etc/init/`.
- `silkd.rc` — Android init service starting silkd on vsock 2048, mirroring
  the `cocoon-agent.rc` pattern the base lineage already ships.
- `platforms` — `linux/amd64`; the lineage is x86_64-only.

Verified about the base image (15.0): anonymous ghcr pull at docker and
`cocoon image pull` level; 2 layers; ships its own kernel
(`/boot/vmlinuz-6.8.*-generic` + initrd, `binder_linux.ko`) booted via
`/sbin/init` busybox wrapper → Android `/init`; runs `cocoon-agent` from
`/system/etc/init/cocoon-agent.rc`; full framework boot to
`sys.boot_completed=1` with zygote64 + system_server alive (CH egress
lane, 4 CPU / 8G).

Do not revert to the previous `ghcr.io/jiaqing-simular/cocoon-android-vnc`
pin: that build ships a broken dexpreopt boot-image chain that
crash-loops zygote before the framework ever completes.

## Open

- **Boot integration**: the image boots its embedded 6.8 kernel via Android
  `/init`, not the sandbox boot chain (PVH kernel + overlay-root init).
  The claim lane must be CH/egress — the FC no-network lane fails the
  readiness probe (2026-07-07 boot-attempt finding). Snapshot/restore of a
  booted Android is validated by the androidsmoke acceptance round.
- **silkd on Android**: the musl-static binary is built and verified static,
  but exec/session behavior against an Android userspace (`/system/bin/sh`,
  no `/bin/sh`) is validated by the androidsmoke acceptance round.
