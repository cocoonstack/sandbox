//! Guest network-lane detection. The egress lane has a device-backed NIC;
//! the none lane carries only virtual interfaces (lo, plus the tunnels an
//! all-builtin kernel auto-creates). Network verbs (git clone/push/pull) and
//! proxy-env forwarding consult this.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI8, Ordering};

static LANE_OVERRIDE: AtomicI8 = AtomicI8::new(-1);

/// Reports whether the guest can reach a network. `SILKD_NET` overrides the
/// probe (`none` / `egress`) for operators; otherwise an interface under
/// /sys/class/net backed by a real device (a `device` symlink) counts.
/// Name filtering is not enough: the all-builtin sandbox kernel auto-creates
/// virtual tunnels (sit0 and friends) even on the no-NIC lane, and `lo` is
/// virtual too — only a virtio/physical NIC has a device backing. On
/// non-Linux dev hosts (no /sys/class/net) it defaults to true so git verbs
/// are testable.
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
    static DEVICE_BACKED: OnceLock<bool> = OnceLock::new();
    *DEVICE_BACKED.get_or_init(|| {
        let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
            return true;
        };
        entries.flatten().any(|e| e.path().join("device").exists())
    })
}

/// Lane override for tests: set_var would race every concurrent getenv.
pub fn override_egress_for_tests(lane: Option<bool>) {
    LANE_OVERRIDE.store(lane.map_or(-1, i8::from), Ordering::Relaxed);
}

/// SILKD_NET decoded once: operator config, fixed at service start.
fn silkd_net() -> i8 {
    static SILKD_NET: OnceLock<i8> = OnceLock::new();
    *SILKD_NET.get_or_init(|| match std::env::var("SILKD_NET").as_deref() {
        Ok("none") => 0,
        Ok("egress") => 1,
        _ => -1,
    })
}
