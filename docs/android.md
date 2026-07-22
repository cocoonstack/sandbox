# Android sandboxes

The `android` flavor runs a full Android system (redroid, Android 15) as a
sandbox: the cocoon project's official redroid image plus silkd, claimable
like any other template. It targets UI automation — drive the device with
adb and any adb-based tooling (scrcpy, Appium, uiautomator2).

```go
sb, err := client.New(ctx, "ghcr.io/cocoonstack/sandbox/android:15",
    sandbox.WithNetwork(sandbox.NetNone), sandbox.WithSize(sandbox.XLarge))
```

The claim returns when silkd answers (seconds); the Android framework
finishes booting shortly after — poll `sys.boot_completed` when the workload
needs the full framework:

```go
out, err := sb.Exec(ctx, "/system/bin/sh", "-c", "getprop sys.boot_completed")
```

## Claim shape

- **Lane**: `net=none`. The image boots its own embedded kernel (with the
  binder module) on Cloud Hypervisor without a NIC, keeping checkpoint and
  branch available.
- **Size**: `xlarge` (4 CPU / 8G) is the intended tier.
- **Template**: `ghcr.io/cocoonstack/sandbox/android:15` — the official
  `ghcr.io/cocoonstack/cocoon/android:15.0` base plus silkd started from an
  Android init service on vsock 2048.

## adb access

adbd listens on `[::]:5555` inside the guest. Reach it through the relay from
anywhere the SDK reaches sandboxd:

```go
ln, err := sb.ProxyPort(ctx, "127.0.0.1:6555", 5555)
// adb connect 127.0.0.1:6555
```

## What works, what differs

- `sb.Exec` runs `/system/bin/sh` — there is no bash and no glibc, so the
  session verbs (persistent bash) are unavailable; prefix commands with
  `PATH=/system/bin:/system/xbin:$PATH` when calling tools directly.
- Files, port relay, and watch verbs work as on the Ubuntu flavors.
- Checkpoint/branch of a booted Android works: a branch answers `ps` and
  adb without rebooting the framework.
- No VNC server in this lineage; screen access goes through adb-based
  tooling such as scrcpy.
- x86_64 only.

The hardware acceptance for all of the above is `e2e/cmd/androidsmoke`:
claim → Android init tree (zygote + system_server) over the relay → ADB
CNXN handshake on 5555 → checkpoint/branch re-verification.
