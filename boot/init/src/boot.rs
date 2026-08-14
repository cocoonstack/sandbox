//! Boot sequence: early mounts → resolve disks → overlay → network persist →
//! switch_root → exec.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::cfg::{self, BootCfg};
use crate::sys;

const LAYER_DIR: &str = "/l";
const COW_DIR: &str = "/cow";
const NEWROOT: &str = "/newroot";
const POLL_INTERVAL: Duration = Duration::from_millis(2);
// NICs probe in single-digit ms; a missing one must degrade to the DHCP
// fallback, not stall rootfs handoff for the full disk budget (10s).
const NIC_TIMEOUT: Duration = Duration::from_millis(200);

/// Cumulative µs checkpoints since sandbox-init start.
struct Marks {
    start: Instant,
    points: Vec<(&'static str, u128)>,
}

impl Marks {
    fn new() -> Self {
        Marks {
            start: Instant::now(),
            points: Vec::new(),
        }
    }

    fn mark(&mut self, label: &'static str) {
        self.points.push((label, self.start.elapsed().as_micros()));
    }

    fn render(&self) -> String {
        self.points
            .iter()
            .map(|(label, us)| format!(" {label}@{us}us"))
            .collect()
    }
}

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
    let mut marks = Marks::new();
    if let Err(err) = assemble(&cfg, &mut marks) {
        sys::fatal(&err, cfg.debug);
    }

    // One deferred trace line (µs, cumulative since sandbox-init start):
    // per-phase console writes would perturb exactly what they measure.
    if cfg.trace {
        println!("sandbox-init: trace{}", marks.render());
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

fn assemble(cfg: &BootCfg, marks: &mut Marks) -> Result<(), String> {
    let ids: Vec<&str> = cfg
        .layers
        .iter()
        .map(String::as_str)
        .chain([cfg.cow.as_str()])
        .collect();
    let mut devs = resolve_disks(&ids, cfg.timeout)?;
    let cow_dev = devs
        .pop()
        .ok_or("internal: cow disk missing from resolve")?;
    marks.mark("resolve");

    let mut lower = Vec::with_capacity(devs.len());
    for (i, dev) in devs.iter().enumerate() {
        let mnt = format!("{LAYER_DIR}/{i}");
        mkdir_all(&mnt)?;
        sys::mount(dev, &mnt, Some("erofs"), libc::MS_RDONLY, None)?;
        lower.push(mnt);
    }
    marks.mark("erofs");

    mkdir_all(COW_DIR)?;
    sys::mount(&cow_dev, COW_DIR, Some("ext4"), libc::MS_NOATIME, None)?;
    mkdir_all(&format!("{COW_DIR}/upper"))?;
    mkdir_all(&format!("{COW_DIR}/work"))?;
    marks.mark("cow");

    mkdir_all(NEWROOT)?;
    sys::mount(
        "overlay",
        NEWROOT,
        Some("overlay"),
        0,
        Some(&cfg::overlay_data(&lower, COW_DIR)),
    )?;
    marks.mark("overlay");

    if let Some(hostname) = &cfg.hostname {
        sys::sethostname(hostname)?;
    }
    // Empty machine-id => systemd generates a fresh one per VM (clone identity).
    let _ = fs::write(format!("{NEWROOT}/etc/machine-id"), "");

    persist_network(cfg);
    marks.mark("net");

    for dir in ["dev", "proc", "sys", "run"] {
        let _ = fs::create_dir_all(format!("{NEWROOT}/{dir}"));
    }
    for mnt in ["/dev", "/proc", "/sys"] {
        sys::move_mount(mnt, &format!("{NEWROOT}{mnt}"))?;
    }
    sys::switch_root(NEWROOT)?;
    marks.mark("switch");
    Ok(())
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
        let Some(mac) = wait_nic_mac(&ip.device, NIC_TIMEOUT) else {
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

/// Resolves every virtio-blk serial in one sysfs sweep per poll iteration.
fn resolve_disks(ids: &[&str], timeout: Duration) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + timeout;
    let mut found: Vec<Option<String>> = vec![None; ids.len()];
    loop {
        scan_serials(ids, &mut found);
        if found.iter().all(Option::is_some) {
            return Ok(found.into_iter().flatten().collect());
        }
        if Instant::now() >= deadline {
            let missing: Vec<&str> = ids
                .iter()
                .zip(&found)
                .filter(|(_, f)| f.is_none())
                .map(|(id, _)| *id)
                .collect();
            return Err(format!("disks not found within {timeout:?}: {missing:?}"));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn scan_serials(ids: &[&str], found: &mut [Option<String>]) {
    let Ok(entries) = fs::read_dir("/sys/block") else {
        return;
    };
    for entry in entries.flatten() {
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
        for path in paths {
            let Ok(serial) = fs::read_to_string(&path) else {
                continue;
            };
            record_serial(ids, found, serial.trim_end(), &format!("/dev/{name}"));
        }
    }
}

fn record_serial(ids: &[&str], found: &mut [Option<String>], serial: &str, device: &str) {
    for (i, id) in ids.iter().enumerate() {
        if found[i].is_none() && *id == serial && Path::new(device).exists() {
            found[i] = Some(device.into());
        }
    }
}

fn uptime() -> String {
    fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_ascii_whitespace().next().map(String::from))
        .unwrap_or_else(|| "?".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_serial_skips_a_missing_device_node() {
        let ids = ["layer"];
        let mut found = [None];

        record_serial(
            &ids,
            &mut found,
            "layer",
            "/dev/sandbox-init-missing-device",
        );
        assert!(found[0].is_none());

        record_serial(&ids, &mut found, "layer", "/dev/null");
        assert_eq!(found[0].as_deref(), Some("/dev/null"));
    }
}
