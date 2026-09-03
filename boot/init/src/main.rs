//! sandbox-init: assembles the EROFS+overlay rootfs named on the kernel cmdline, then execs the real init.

// compiled dead off Linux so `cargo test` still covers cmdline parsing on a dev host.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod cfg;

#[cfg(target_os = "linux")]
mod boot;
#[cfg(target_os = "linux")]
mod sys;

#[cfg(target_os = "linux")]
fn main() {
    boot::run()
}

/// Keeps `cargo test` runnable on non-Linux dev hosts (cfg logic tests).
#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("sandbox-init is Linux-only");
    std::process::exit(1);
}
