//! Guest network-lane detection: the egress lane has a device-backed NIC, the none lane only virtual interfaces.

use std::fs::File;
use std::io::Read;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI8, Ordering};

/// File the host writes with its lane verdict, `relay` or `direct`; on the root filesystem so a guest reboot keeps it.
const LANE_FILE: &str = "/etc/silkd-lane";

static LANE_OVERRIDE: AtomicI8 = AtomicI8::new(-1);
/// The lane file's verdict once read: -1 not yet present, 0 direct, 1 relay.
static LANE: AtomicI8 = AtomicI8::new(-1);

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
    // the host writes the file once per claim, so a present verdict latches; only absence re-reads.
    let lane = match LANE.load(Ordering::Relaxed) {
        -1 => match lane_verdict(LANE_FILE) {
            Some(relay) => {
                LANE.store(i8::from(relay), Ordering::Relaxed);
                relay
            }
            None => false,
        },
        v => v == 1,
    };
    !lane
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

/// Reads the lane file: `Some(true)` for relay, `Some(false)` for direct, `None` while the host has not written it.
fn lane_verdict(path: &str) -> Option<bool> {
    let mut buf = [0u8; 8];
    let n = File::open(path).and_then(|mut f| f.read(&mut buf)).ok()?;
    Some(buf[..n].trim_ascii() == b"relay")
}

#[cfg(test)]
mod tests {
    use super::lane_verdict;

    #[test]
    fn lane_file_decides_relay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lane");
        let text = path.to_str().unwrap();
        assert_eq!(lane_verdict(text), None);
        std::fs::write(&path, "direct\n").unwrap();
        assert_eq!(lane_verdict(text), Some(false));
        std::fs::write(&path, "relay\n").unwrap();
        assert_eq!(lane_verdict(text), Some(true));
    }
}
