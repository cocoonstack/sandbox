//! Boot sequence: early mounts → resolve disks → overlay → network persist →
//! switch_root → exec.

use std::fs;
use std::time::{Duration, Instant};

use crate::cfg::{self, BootCfg};
use crate::sys;

const LAYER_DIR: &str = "/l";
const COW_DIR: &str = "/cow";
const NEWROOT: &str = "/newroot";
const POLL_INTERVAL: Duration = Duration::from_millis(2);

pub fn run() -> ! {
    // Best-effort: if devtmpfs fails there is no console either; later
    // failures then power off silently, which is still the right end state.
    let _ = sys::mount(
        "devtmpfs",
        "/dev",
        Some("devtmpfs"),
        libc::MS_NOSUID,
        Some("mode=0755"),
    );
    sys::claim_console();
    let _ = sys::mount("proc", "/proc", Some("proc"), sys::MNT_SECURE, None);
    let _ = sys::mount("sysfs", "/sys", Some("sysfs"), sys::MNT_SECURE, None);

    // Start marker: kernel-relative and visible at production loglevel,
    // where the kernel's own boot lines are suppressed. One console write.
    println!("sandbox-init: start at {}s", uptime());

    let cmdline = fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let cfg = match cfg::parse(&cmdline) {
        Ok(cfg) => cfg,
        Err(err) => sys::fatal(&err, cfg::debug_requested(&cmdline)),
    };
    if let Err(err) = assemble(&cfg) {
        sys::fatal(&err, cfg.debug);
    }

    // Single marker line; boot-bench.sh keys on it. Uptime is kernel-relative,
    // directly comparable with printk timestamps on the serial log.
    println!(
        "sandbox-init: rootfs ready at {}s, handing off to {}",
        uptime(),
        cfg.init
    );
    let err = sys::exec_init(&cfg.init);
    sys::fatal(&format!("exec {}: {err}", cfg.init), cfg.debug)
}

fn assemble(cfg: &BootCfg) -> Result<(), String> {
    let mut lower = Vec::with_capacity(cfg.layers.len());
    for (i, id) in cfg.layers.iter().enumerate() {
        let dev = resolve_disk(id, cfg.timeout)?;
        let mnt = format!("{LAYER_DIR}/{i}");
        mkdir_all(&mnt)?;
        sys::mount(&dev, &mnt, Some("erofs"), libc::MS_RDONLY, None)?;
        lower.push(mnt);
    }

    let cow_dev = resolve_disk(&cfg.cow, cfg.timeout)?;
    mkdir_all(COW_DIR)?;
    sys::mount(&cow_dev, COW_DIR, Some("ext4"), libc::MS_NOATIME, None)?;
    mkdir_all(&format!("{COW_DIR}/upper"))?;
    mkdir_all(&format!("{COW_DIR}/work"))?;

    mkdir_all(NEWROOT)?;
    sys::mount(
        "overlay",
        NEWROOT,
        Some("overlay"),
        0,
        Some(&cfg::overlay_data(&lower, COW_DIR)),
    )?;

    if let Some(hostname) = &cfg.hostname {
        sys::sethostname(hostname)?;
    }
    // Empty machine-id => systemd generates a fresh one per VM (clone identity).
    let _ = fs::write(format!("{NEWROOT}/etc/machine-id"), "");

    persist_network(cfg);

    for dir in ["dev", "proc", "sys", "run"] {
        let _ = fs::create_dir_all(format!("{NEWROOT}/{dir}"));
    }
    for mnt in ["/dev", "/proc", "/sys"] {
        sys::move_mount(mnt, &format!("{NEWROOT}{mnt}"))?;
    }
    sys::switch_root(NEWROOT)
}

/// Materializes kernel ip= params (cocoon CNI static flow) as MAC-matched
/// networkd units in the new root — persistence only, nothing is configured
/// in the initramfs; networkd applies them once the real init is up. A NIC
/// that never shows up degrades that interface to the DHCP fallback instead
/// of failing the boot, matching the old init-bottom hook.
fn persist_network(cfg: &BootCfg) {
    if cfg.ips.is_empty() {
        return;
    }
    let dir = format!("{NEWROOT}/etc/systemd/network");
    if let Err(err) = fs::create_dir_all(&dir) {
        eprintln!("sandbox-init: WARN: mkdir {dir}: {err}");
        return;
    }
    for ip in &cfg.ips {
        let Some(mac) = wait_nic_mac(&ip.device, cfg.timeout) else {
            eprintln!(
                "sandbox-init: WARN: NIC {} not found, static config skipped",
                ip.device
            );
            continue;
        };
        let path = format!("{dir}/10-{}.network", mac.replace(':', ""));
        if let Err(err) = fs::write(&path, cfg::network_unit(ip, &mac)) {
            eprintln!("sandbox-init: WARN: write {path}: {err}");
        }
    }
}

fn wait_nic_mac(device: &str, timeout: Duration) -> Option<String> {
    let path = format!("/sys/class/net/{device}/address");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(mac) = fs::read_to_string(&path) {
            let mac = mac.trim_end().to_string();
            if !mac.is_empty() && mac != "00:00:00:00:00:00" {
                return Some(mac);
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn mkdir_all(path: &str) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|err| format!("mkdir {path}: {err}"))
}

/// CH disks carry a virtio-blk serial; FC has no serials, so cocoon passes
/// /dev/vdX paths there. 2ms polling — virtio probe completes a few ms into
/// boot, so the first or second try normally hits; the old initramfs hook's
/// 1s sleep granularity was a visible cost.
fn resolve_disk(id: &str, timeout: Duration) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(dev) = try_resolve(id) {
            return Ok(dev);
        }
        if Instant::now() >= deadline {
            return Err(format!("disk {id} not found within {timeout:?}"));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn try_resolve(id: &str) -> Option<String> {
    if id.starts_with("/dev/") {
        return sys::is_block_dev(id).then(|| id.to_string());
    }
    for entry in fs::read_dir("/sys/block").ok()?.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.starts_with("vd") {
            continue;
        }
        // The serial attribute location varies by kernel version.
        let paths = [
            format!("/sys/block/{name}/serial"),
            format!("/sys/block/{name}/device/serial"),
        ];
        if paths
            .iter()
            .any(|p| fs::read_to_string(p).is_ok_and(|s| s.trim_end() == id))
        {
            return Some(format!("/dev/{name}"));
        }
    }
    None
}

fn uptime() -> String {
    fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_ascii_whitespace().next().map(String::from))
        .unwrap_or_else(|| "?".into())
}
