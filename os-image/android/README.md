# android flavor (groundwork)

Android guest for UI-automation sandboxes: the cocoon-android lineage
(`ghcr.io/jiaqing-simular/cocoon-android-vnc`, a redroid-style Android 15
rootfs with droidVNC-NG baked in) plus silkd, claimable like any other
template. The intended pool shape is `size: xlarge` (4 CPU / 8G).

## Build

Unlike the Ubuntu flavors, the guest has no glibc and no systemd:

- `15/Dockerfile` — `FROM ${ANDROID_BASE_IMAGE}` (defaults to the pinned
  public base), `COPY --from` the silkd carrier's musl-static
  `/silkd-static` binary to `/system/bin/silkd`, and drop `silkd.rc` into
  `/system/etc/init/`.
- `silkd.rc` — Android init service starting silkd on vsock 2048, mirroring
  the `cocoon-agent.rc` pattern the base lineage already ships.
- `platforms` — `linux/amd64`; the lineage is x86_64-only.

Verified about the base image (0.3.0): pullable at docker level and via
`cocoon image pull`; 4 layers, ~3.7GB on disk; entrypoint
`/init androidboot.hardware=redroid ro.setupwizard.mode=DISABLED`; ships its
own kernel (`/boot/vmlinuz-6.8.0-117-generic` + initrd, with
`binder_linux.ko`) rather than using the sandbox boot artifact; already runs
`cocoon-agent` from `/system/etc/init/cocoon-agent.rc` (class core, root, no
seclabel — the lineage boots SELinux-permissive).

The base is public on ghcr (anonymous pull verified against the manifest
endpoint), so the flavor rides the normal images matrix; the earlier
"CI base-image access" blocker is void.

## Open

- **Boot integration**: the image boots its embedded 6.8 kernel via Android
  `/init`, not the sandbox boot chain (PVH kernel + overlay-root init).
  The claim lane must be CH/egress — the FC no-network lane fails the
  readiness probe (2026-07-07 boot-attempt finding). Snapshot/restore of a
  booted Android is validated by the M6-1 acceptance round.
- **silkd on Android**: the musl-static binary is built and verified static,
  but exec/session behavior against an Android userspace (`/system/bin/sh`,
  no `/bin/sh`) is validated by the M6-1 acceptance round.
