The local test uses the official Cocoon v0.7.2 x86_64 release from https://github.com/cocoonstack/cocoon/releases/tag/v0.7.2. Archive SHA256 is bd9e9c76f926a5005089ede61a85b1d98c5294c454d3efca0cb4bf4826dca8d4, matching the release checksums. Version reports Git b3b40f3bdedc384c482eb81dcdc60a774dd8d560. Cloud Hypervisor is the existing /opt/composable-local-bfc70d3/bin/cloud-hypervisor v54.0.0. No version output is forged or emulated.

The test's root, runtime sockets, logs, synthetic API token, images, and daemon state are isolated below /tmp/pr339-live. The daemon listens at 127.0.0.1:17777. Cocoon uses network scope p9 and cgroup parent pr339-cocoon.slice. None of the existing composable-local services has been stopped or modified.

Preparation reused the existing guest-rootfs.tar and vmlinuz from /var/lib/composable-local-bfc70d3. The first real VM immediately exited because its old initramfs expected computer.layers while official Cocoon supplies cocoon.layers. Rebuilding the current sandbox boot/init binary fixed that compatibility difference. The first rebuilt binary was dynamically linked and produced a kernel "No working init found" panic inside the small initramfs. Rebuilding it statically fixed that. These were environment preparation failures, not feature failures. The final guest was accepted only after a successful CONNECT 2048 handshake and a live host PID check.

Commands for the working boot rebuild, run from the hardware worktree are below. The host's stable Rust toolchain is recorded in tool-versions.txt. The rootfs layer already contains silkd. It responds to real SDK operations in the baseline smoke.

```sh
RUSTUP_TOOLCHAIN=stable RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --locked --manifest-path boot/init/Cargo.toml
podman create --name pr339-boot localhost/composable-computer/sandbox/boot:6.18.37
podman cp pr339-boot:/boot/initrd.img-sandbox /tmp/pr339-live/boot/initrd.img
cd /tmp/pr339-live/initramfs
cpio -idmu < /tmp/pr339-live/boot/initrd.img
cp /root/eric/composable-computer/.audit/pr339-implementation/hardware/boot/init/target/release/sandbox-init init
find . -print0 | cpio --null -o -H newc > /tmp/pr339-live/boot/initrd.img
cd /tmp/pr339-live/boot
tar -cf /tmp/pr339-live/boot.tar .
. /tmp/pr339-live/env
cocoon image import pr339:local /var/lib/composable-local-bfc70d3/guest-rootfs.tar /tmp/pr339-live/boot.tar
```

The order of the two input layers matters. The boot overlay must come last because the existing rootfs also contains boot files. Input checksums are in guest-input-sha256.txt. The resulting image is sha256:0356e79c7b82914c62e7b5db88b06369594c92f4bb95b4e1de5a721023b1ba91. It uses the real existing 6.18.37 kernel. The guest bootstrap is ordinary sandbox-init and systemd with silkd. The VM pr339-prep was created solely for environment proof. The successful preparation record and PID are in prep-vm-ready.json and prep-vm-current.json.

Dataset fixtures are two 32 MiB ext4 images made with mkfs.ext4 -F -d. Their version files contain v1 and v2 respectively. Catalog names dataset-v1 and dataset-v2 point to those images. Both allow writable attachment to permit the later retirement/backing-reuse assertion. Ordinary smoke attachments request read-only mode.

The baseline daemon was built from 645e49756492e7d8e9337377a958185e615804fc. baseline-smoke.log records real shared dataset reads and EROFS succeeding, followed by the expected failing cwd-busy assertion. The old daemon actually returned HTTP 500 instead of the required HTTP 409 and kept v1 applied with v2 desired. baseline-runtime-commands.jsonl and baseline-daemon.log record its real runtime calls and error. This is a hardware reproduction, not a fake-engine result.

The committed testdata/runtime_observer.py and fault_hook.py are the complete fault injection mechanism. Ordinary commands execute the official binary. Attach interruption occurs only after the real attach succeeds. VM removal failures are explicitly injected exit 73 before the real remove command. The observer logs every attempted command. The test must separately prove final real removal and a later writable attach before claiming backing reuse.
