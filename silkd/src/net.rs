//! Guest network-lane detection. The egress lane has a non-loopback
//! interface; the none lane has only `lo`. Verbs that need the network (git
//! clone/push/pull) consult this so the none lane fails fast with a typed
//! error instead of hanging on an unreachable remote.

/// Reports whether the guest can reach a network. `SILKD_NET` overrides the
/// probe (`none` / `egress`) for operators and tests; otherwise a non-`lo`
/// entry under /sys/class/net means egress. On non-Linux dev hosts (no
/// /sys/class/net) it defaults to true so git verbs are testable.
pub fn has_egress() -> bool {
    match std::env::var("SILKD_NET").as_deref() {
        Ok("none") => return false,
        Ok("egress") => return true,
        _ => {}
    }
    let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
        return true;
    };
    entries
        .flatten()
        .any(|e| e.file_name().to_string_lossy() != "lo")
}
