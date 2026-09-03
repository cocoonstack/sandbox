//! Guest network-lane detection: the egress lane has a device-backed NIC, the none lane only virtual interfaces.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicI8, Ordering};

static LANE_OVERRIDE: AtomicI8 = AtomicI8::new(-1);

/// Reports whether the guest can reach a network; only a `device`-backed interface counts, since the kernel auto-creates virtual tunnels.
pub fn has_egress() -> bool {
    match LANE_OVERRIDE.load(Ordering::Relaxed) {
        0 => return false,
        1 => return true,
        _ => {}
    }
    match silkd_net() {
        0 => return false,
        1 => return true,
        _ => {}
    }
    // Probed once: the NIC set is fixed for the guest's life.
    static DEVICE_BACKED: LazyLock<bool> = LazyLock::new(|| {
        let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
            return true;
        };
        entries.flatten().any(|e| e.path().join("device").exists())
    });
    *DEVICE_BACKED
}

/// Lane override for tests: set_var would race every concurrent getenv.
pub fn override_egress_for_tests(lane: Option<bool>) {
    LANE_OVERRIDE.store(lane.map_or(-1, i8::from), Ordering::Relaxed);
}

/// SILKD_NET decoded once: operator config, fixed at service start.
fn silkd_net() -> i8 {
    static SILKD_NET: LazyLock<i8> =
        LazyLock::new(|| match std::env::var("SILKD_NET").as_deref() {
            Ok("none") => 0,
            Ok("egress") => 1,
            _ => -1,
        });
    *SILKD_NET
}
