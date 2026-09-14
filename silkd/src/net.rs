//! Guest network-lane detection: the egress lane has a device-backed NIC, the none lane only virtual interfaces.

use std::fs::File;
use std::io::Read;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicI8, Ordering};

/// File the host writes with its lane verdict, `relay` or `direct`; on the root filesystem so a guest reboot keeps it.
const LANE_FILE: &str = "/etc/silkd-lane";

static LANE_OVERRIDE: AtomicI8 = AtomicI8::new(-1);
static LANE_RELAY: AtomicBool = AtomicBool::new(false);

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

/// Reports whether execs route directly: a NIC the host did not mark as relayed.
pub fn routes_directly() -> bool {
    if !has_egress() {
        return false;
    }
    // relay latches: the host never unlocks a lane, but a late lock can land after the first exec.
    if LANE_RELAY.load(Ordering::Relaxed) {
        return false;
    }
    let relay = lane_is_relay(LANE_FILE);
    if relay {
        LANE_RELAY.store(true, Ordering::Relaxed);
    }
    !relay
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

fn lane_is_relay(path: &str) -> bool {
    let mut buf = [0u8; 8];
    File::open(path)
        .and_then(|mut f| f.read(&mut buf))
        .is_ok_and(|n| buf[..n].trim_ascii() == b"relay")
}

#[cfg(test)]
mod tests {
    use super::lane_is_relay;

    #[test]
    fn lane_file_decides_relay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lane");
        let text = path.to_str().unwrap();
        assert!(!lane_is_relay(text));
        std::fs::write(&path, "direct\n").unwrap();
        assert!(!lane_is_relay(text));
        std::fs::write(&path, "relay\n").unwrap();
        assert!(lane_is_relay(text));
    }
}
