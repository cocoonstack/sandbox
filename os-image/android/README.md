# android flavor (groundwork)

Android guest for UI-automation sandboxes: the cocoon-android lineage
(`ghcr.io/jiaqing-simular/cocoon-android-vnc`, a redroid-style Android 15
rootfs with droidVNC-NG baked in) plus silkd, claimable like any other
template. The intended pool shape is `size: xlarge` (4 CPU / 8G).

## Planned build

Unlike the Ubuntu flavors, the guest has no glibc and no systemd:

- `15/Dockerfile.skeleton` — `FROM ${ANDROID_BASE_IMAGE}` (no default; the
  base is outside the cocoonstack registry), `COPY --from` the silkd
  carrier's musl-static `/silkd-static` binary to `/system/bin/silkd`, and
  drop `silkd.rc` into `/system/etc/init/`.
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

## Blocked / open

- **CI base-image access**: the base lives under an external ghcr org and
  CI credentials for it are not arranged. Until then `ANDROID_BASE_IMAGE`
  has no default and the Dockerfile is parked as `Dockerfile.skeleton` —
  the images workflow matrixes every file named exactly `Dockerfile` under
  `os-image/`, and wiring this in before base access would fail every
  all-image rebuild. Renaming the file is the wiring step.
- **Boot integration**: the image boots its embedded 6.8 kernel via Android
  `/init`, not the sandbox boot chain (PVH kernel + overlay-root init).
  How cocoon boots this shape (and whether snapshot/restore holds) is
  unvalidated — no boot test has been run.
- **silkd on Android**: the musl-static binary is built and verified static,
  but exec/session behavior against an Android userspace (`/system/bin/sh`,
  no `/bin/sh`) is untested until a boot test is possible.
