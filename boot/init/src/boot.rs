//! Boot sequence: early mounts, resolve disks, overlay, persist network, switch_root, exec.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::cfg::{self, BootCfg};
use crate::sys;

const LAYER_DIR: &str = "/l";
const COW_DIR: &str = "/cow";
const NEWROOT: &str = "/newroot";
const POLL_INTERVAL: Duration = Duration::from_millis(2);
// a missing NIC must degrade to the DHCP fallback, not stall handoff for the disk budget.
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
    let mut marks = Marks::new();
    // best-effort: without devtmpfs there is no console, so a later failure powers off silently.
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

    // start marker, visible at production loglevel where the kernel's own boot lines are suppressed.
    println!("sandbox-init: start at {}s", uptime());

    let cmdline = fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let cfg = match cfg::parse(&cmdline) {
        Ok(cfg) => cfg,
        Err(err) => sys::fatal(&err, cfg::debug_requested(&cmdline)),
    };
    marks.mark("early");
    if let Err(err) = assemble(&cfg, &mut marks) {
        sys::fatal(&err, cfg.debug);
    }

    // one deferred line: per-phase console writes would perturb what they measure.
    if cfg.trace {
        println!("sandbox-init: trace{}", marks.render());
    }
    // boot-bench.sh keys on this line; the uptime is comparable with printk timestamps.
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

/// Persists kernel ip= params as MAC-matched networkd units in the new root; a missing NIC degrades to the DHCP fallback.
fn persist_network(cfg: &BootCfg) {
    if cfg.ips.is_empty() {
        return;
    }
    let dir = format!("{NEWROOT}/etc/systemd/network");
    if let Err(err) = fs::create_dir_all(&dir) {
        eprintln!("sandbox-init: WARN: mkdir {dir}: {err}");
        return;
    }
    let devices: Vec<&str> = cfg.ips.iter().map(|ip| ip.device.as_str()).collect();
    for (ip, mac) in cfg.ips.iter().zip(wait_nic_macs(&devices, NIC_TIMEOUT)) {
        let Some(mac) = mac else {
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

/// Resolves every NIC MAC against one shared deadline, so a missing NIC costs the timeout once.
fn wait_nic_macs(devices: &[&str], timeout: Duration) -> Vec<Option<String>> {
    let deadline = Instant::now() + timeout;
    let mut found: Vec<Option<String>> = vec![None; devices.len()];
    loop {
        for (slot, device) in found.iter_mut().zip(devices) {
            if slot.is_none() {
                *slot = read_nic_mac(device);
            }
        }
        if found.iter().all(Option::is_some) || Instant::now() >= deadline {
            return found;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn read_nic_mac(device: &str) -> Option<String> {
    let mac = fs::read_to_string(format!("/sys/class/net/{device}/address")).ok()?;
    let mac = mac.trim_end();
    (!mac.is_empty() && mac != "00:00:00:00:00:00").then(|| mac.to_string())
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
