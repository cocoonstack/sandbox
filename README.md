# sandbox

Fast cold-boot MicroVM system for AI-agent sandboxes, built on
[cocoon](https://github.com/cocoonstack/cocoon).

Phase 1 owns the boot path: a custom all-builtin guest kernel plus a
single-binary micro-initramfs (`sandbox-init`) that assembles the
EROFS + overlay rootfs and hands off to the real init. No distro kernel, no
initramfs-tools, no udev in the boot path. Design + rationale:
[cocoon-specs/design/sandbox-fast-boot.md](https://github.com/cocoonstack/cocoon-specs/blob/main/design/sandbox-fast-boot.md).

```
cloud-hypervisor / firecracker
  → vmlinux (PVH ELF, everything =y, no decompress stage)
  → uncompressed ~1.5MB cpio: /init = sandbox-init (static Rust)
  → resolve virtio-blk serials via sysfs (2ms poll, no udev)
  → mount EROFS layers → overlayfs + ext4 COW → switch_root
  → exec /sbin/init  (systemd, trimmed; cocoon-agent starts at sysinit)
```

## Layout

- `boot/kernel/` — kernel version pin (`VERSION` + matching tarball
  `SHA256`, bump both together) + config fragment (applied over
  `x86_64_defconfig` + `kvm_guest.config`)
- `boot/init/` — `sandbox-init`, the entire initramfs userland (Rust, static
  musl build; `cargo test` runs the logic tests on any host)
- `boot/Dockerfile` — multi-stage build: kernel → init → cpio → scratch image
  with `/boot/vmlinuz-sandbox` + `/boot/initrd.img-sandbox`
- `os-image/` — VM images consuming the boot artifact (`base`, `python`);
  same layout and CI conventions as cocoon's os-image
- `scripts/boot-bench.sh` — boot phase timing harness (Linux + KVM + CH)
- `sdk/` — reserved for phase 2 (`sandbox.New()` Go SDK)

## Build

```bash
make test          # sandbox-init unit tests (any host with cargo)
make boot          # build kernel + initramfs artifact image (docker)
                   #   KERNEL_MIRROR=https://mirrors.tuna.tsinghua.edu.cn/kernel
                   #   if kernel.org tarball paths are unreachable locally
make boot-debug    # same, with busybox + /bin/sh on fatal errors
make extract       # dump /boot artifacts into dist/ for boot-bench.sh
make extract-debug # same, from the boot-debug image
make images        # build base + python images against the local boot image
```

CI: `build-boot.yml` publishes `ghcr.io/cocoonstack/sandbox/boot:<kernel-ver>`
on `boot/**` changes; `build-os-images.yml` builds changed image dirs exactly
like cocoon's workflow. On a fresh repo run build-boot first — the image
builds `FROM` the boot artifact.

## Boot contract (consumed by sandbox-init)

| cmdline key | meaning |
|---|---|
| `cocoon.layers=a,b,…` | EROFS layer disks: virtio-blk serials (CH) or `/dev/vdX` (FC), lowerdir order |
| `cocoon.cow=x` | writable ext4 COW disk (same resolution rules) |
| `cocoon.timeout=10` | per-disk wait budget, seconds |
| `cocoon.hostname=h` | set via `sethostname(2)` before handoff |
| `ip=addr::gw:mask:host:ethN:off[:dns0[:dns1]]` | cocoon CNI static config: persisted as a MAC-matched networkd unit in the new root (not applied in the initramfs); absent → the image's DHCP fallback covers the NIC |
| `sandbox.init=/path` | handoff target, default `/sbin/init` |
| `sandbox.debug=1` | fatal errors drop to `/bin/sh` (debug initramfs) instead of poweroff |

`boot=cocoon-overlay` is ignored. Everything cocoon passes today keeps
working — images built here boot with an unmodified cocoon.
