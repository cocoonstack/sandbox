//! sandbox-init: the entire initramfs userland. Assembles the EROFS + overlay
//! rootfs described on the kernel cmdline and hands off to the real init.
//! Design: cocoon-specs/design/sandbox-fast-boot.md.

// Off Linux only cfg's own tests use it; the bin compiles it dead so
// `cargo test` still covers the cmdline parsing on dev hosts.
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
