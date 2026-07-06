//! Kernel cmdline contract, shared with cocoon (hypervisor/utils.go builds
//! the cmdline; this module is the consuming end).

use std::time::Duration;

pub const DEFAULT_INIT: &str = "/sbin/init";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
// Cap: `Instant + Duration` panics on overflow, and a panic in PID 1 with
// panic=abort is a kernel panic — a hung VM instead of fatal()'s poweroff.
const MAX_TIMEOUT_SECS: u64 = 86_400;

#[derive(Debug, PartialEq, Eq)]
pub struct BootCfg {
    /// Layer disk IDs in lowerdir order (first = uppermost lower layer):
    /// virtio-blk serials on CH, /dev/vdX paths on FC.
    pub layers: Vec<String>,
    /// Writable ext4 COW disk ID (same resolution rules as layers).
    pub cow: String,
    /// Per-device wait budget.
    pub timeout: Duration,
    pub hostname: Option<String>,
    /// Static per-NIC config from kernel ip= params (cocoon CNI flow).
    /// Persisted as systemd-networkd units, never applied in the initramfs.
    pub ips: Vec<IpParam>,
    /// Handoff target inside the assembled rootfs.
    pub init: String,
    /// Fatal errors drop to /bin/sh (debug initramfs) instead of poweroff.
    pub debug: bool,
    /// Emit one pre-handoff line with per-phase µs timings (sandbox.trace=1).
    pub trace: bool,
}

/// One `ip=<addr>::<gw>:<mask>:<host>:<dev>:off[:dns0[:dns1]]` param
/// (the shape cocoon's BuildIPParams emits, one per NIC).
#[derive(Debug, PartialEq, Eq)]
pub struct IpParam {
    pub addr: String,
    pub prefix: u8,
    pub gateway: Option<String>,
    pub dns: Vec<String>,
    /// Initramfs-time device name (ethN) — only used to look up the MAC;
    /// the persisted unit matches by MAC so rootfs udev renames don't matter.
    pub device: String,
}

pub fn parse(cmdline: &str) -> Result<BootCfg, String> {
    let mut cfg = BootCfg {
        layers: Vec::new(),
        cow: String::new(),
        timeout: DEFAULT_TIMEOUT,
        hostname: None,
        ips: Vec::new(),
        init: DEFAULT_INIT.to_string(),
        debug: false,
        trace: false,
    };
    for tok in cmdline.split_ascii_whitespace() {
        let (key, val) = tok.split_once('=').unwrap_or((tok, ""));
        match key {
            "cocoon.layers" => {
                cfg.layers = val
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            "cocoon.cow" => cfg.cow = val.to_string(),
            // A junk value keeps the default, matching the old initramfs hook.
            "cocoon.timeout" => {
                if let Ok(secs) = val.parse::<u64>() {
                    cfg.timeout = Duration::from_secs(secs.min(MAX_TIMEOUT_SECS));
                }
            }
            "cocoon.hostname" if !val.is_empty() => cfg.hostname = Some(val.to_string()),
            "sandbox.debug" => cfg.debug = debug_token(val),
            "sandbox.trace" => cfg.trace = debug_token(val),
            "ip" => {
                if let Some(param) = parse_ip_param(val) {
                    cfg.ips.push(param);
                }
            }
            "sandbox.init" if !val.is_empty() => cfg.init = val.to_string(),
            _ => {}
        }
    }
    if cfg.layers.is_empty() {
        return Err("cocoon.layers= not set".into());
    }
    if cfg.cow.is_empty() {
        return Err("cocoon.cow= not set".into());
    }
    Ok(cfg)
}

/// Debug check for the path where parse() itself failed and cfg.debug is
/// unavailable. Token handling mirrors parse() exactly: same value
/// predicate, last occurrence wins.
pub fn debug_requested(cmdline: &str) -> bool {
    let mut debug = false;
    for tok in cmdline.split_ascii_whitespace() {
        let (key, val) = tok.split_once('=').unwrap_or((tok, ""));
        if key == "sandbox.debug" {
            debug = debug_token(val);
        }
    }
    debug
}

/// Overlay mount data. Layer mountpoints are index-based (/l/0, /l/1, …) so
/// arbitrary serial strings can never break lowerdir parsing (':' or ',').
pub fn overlay_data(lower: &[String], cow_dir: &str) -> String {
    format!(
        "lowerdir={},upperdir={cow_dir}/upper,workdir={cow_dir}/work,index=on,redirect_dir=on,metacopy=on,xino=on",
        lower.join(":")
    )
}

/// systemd-networkd unit for one static NIC. MAC-matched (device names may
/// change once rootfs udev renames), named 10-<mac>.network so it sorts
/// before the image's 20-wired.network DHCP fallback.
pub fn network_unit(ip: &IpParam, mac: &str) -> String {
    let mut unit = format!(
        "[Match]\nMACAddress={mac}\n\n[Network]\nAddress={}/{}\n",
        ip.addr, ip.prefix
    );
    if let Some(gw) = &ip.gateway {
        unit.push_str(&format!("Gateway={gw}\n"));
    }
    for dns in &ip.dns {
        unit.push_str(&format!("DNS={dns}\n"));
    }
    unit
}

/// sandbox.debug value semantics, shared by parse() and debug_requested().
fn debug_token(val: &str) -> bool {
    val.is_empty() || val == "1"
}

/// Kernel ip= fields: client:server:gw:netmask:hostname:device:autoconf[:dns0[:dns1]].
/// Shorthand forms (ip=dhcp, ip=off) and malformed params are ignored — the
/// baked DHCP .network fallback then covers the NIC, like the old hook did.
fn parse_ip_param(val: &str) -> Option<IpParam> {
    let f: Vec<&str> = val.split(':').collect();
    if f.len() < 7 || f[0].is_empty() || f[5].is_empty() {
        return None;
    }
    let prefix = mask_to_prefix(f[3])?;
    let gateway = (!f[2].is_empty() && f[2] != "0.0.0.0").then(|| f[2].to_string());
    let dns = f
        .get(7..)
        .unwrap_or(&[])
        .iter()
        .filter(|d| !d.is_empty() && **d != "0.0.0.0")
        .map(|d| d.to_string())
        .collect();
    Some(IpParam {
        addr: f[0].to_string(),
        prefix,
        gateway,
        dns,
        device: f[5].to_string(),
    })
}

fn mask_to_prefix(mask: &str) -> Option<u8> {
    let mut bits: u32 = 0;
    let mut octets = 0;
    for part in mask.split('.') {
        let octet: u32 = part.parse().ok()?;
        if octet > 255 {
            return None;
        }
        bits = (bits << 8) | octet;
        octets += 1;
    }
    if octets != 4 {
        return None;
    }
    let prefix = bits.leading_ones();
    // Reject non-contiguous masks.
    if bits != u32::MAX.checked_shl(32 - prefix).unwrap_or(0) {
        return None;
    }
    Some(prefix as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_cmdline() {
        let cfg = parse(
            "console=hvc0 loglevel=3 boot=cocoon-overlay cocoon.layers=a,b cocoon.cow=cow \
             cocoon.timeout=3 cocoon.hostname=vm1 sandbox.init=/bin/systemd sandbox.debug=1 \
             clocksource=kvm-clock rw",
        )
        .unwrap();
        assert_eq!(cfg.layers, vec!["a", "b"]);
        assert_eq!(cfg.cow, "cow");
        assert_eq!(cfg.timeout, Duration::from_secs(3));
        assert_eq!(cfg.hostname.as_deref(), Some("vm1"));
        assert_eq!(cfg.init, "/bin/systemd");
        assert!(cfg.debug);
        assert!(cfg.ips.is_empty());
    }

    #[test]
    fn parse_defaults() {
        let cfg = parse("cocoon.layers=l0 cocoon.cow=cow").unwrap();
        assert_eq!(cfg.timeout, DEFAULT_TIMEOUT);
        assert_eq!(cfg.hostname, None);
        assert_eq!(cfg.init, DEFAULT_INIT);
        assert!(!cfg.debug);
    }

    #[test]
    fn parse_missing_layers_or_cow() {
        assert!(parse("cocoon.cow=cow")
            .unwrap_err()
            .contains("cocoon.layers"));
        assert!(parse("cocoon.layers=l0")
            .unwrap_err()
            .contains("cocoon.cow"));
        assert!(parse("cocoon.layers= cocoon.cow=cow")
            .unwrap_err()
            .contains("cocoon.layers"));
    }

    #[test]
    fn parse_junk_timeout_keeps_default() {
        let cfg = parse("cocoon.layers=l0 cocoon.cow=cow cocoon.timeout=abc").unwrap();
        assert_eq!(cfg.timeout, DEFAULT_TIMEOUT);
    }

    #[test]
    fn parse_huge_timeout_is_capped() {
        let cfg = parse(&format!(
            "cocoon.layers=l0 cocoon.cow=cow cocoon.timeout={}",
            u64::MAX
        ))
        .unwrap();
        assert_eq!(cfg.timeout, Duration::from_secs(MAX_TIMEOUT_SECS));
    }

    #[test]
    fn debug_requested_exact_tokens() {
        assert!(debug_requested("a sandbox.debug b"));
        assert!(debug_requested("a sandbox.debug=1 b"));
        assert!(debug_requested("a sandbox.debug= b"));
        assert!(!debug_requested("a sandbox.debug=0 b"));
        assert!(!debug_requested("a sandbox.debugger b"));
        assert!(!debug_requested(""));
        // Last occurrence wins, like parse().
        assert!(!debug_requested("sandbox.debug=1 x sandbox.debug=0"));
        assert!(debug_requested("sandbox.debug=0 x sandbox.debug"));
    }

    #[test]
    fn debug_requested_matches_parse() {
        for tail in [
            "",
            "sandbox.debug",
            "sandbox.debug=",
            "sandbox.debug=1",
            "sandbox.debug=0",
            "sandbox.debug=1 sandbox.debug=0",
        ] {
            let cmdline = format!("cocoon.layers=l0 cocoon.cow=cow {tail}");
            assert_eq!(
                parse(&cmdline).unwrap().debug,
                debug_requested(&cmdline),
                "divergence for {tail:?}"
            );
        }
    }

    #[test]
    fn parse_debug_forms() {
        let base = "cocoon.layers=l0 cocoon.cow=cow";
        assert!(parse(&format!("{base} sandbox.debug")).unwrap().debug);
        assert!(parse(&format!("{base} sandbox.debug=1")).unwrap().debug);
        assert!(!parse(&format!("{base} sandbox.debug=0")).unwrap().debug);
    }

    #[test]
    fn parse_trace_forms() {
        let base = "cocoon.layers=l0 cocoon.cow=cow";
        assert!(!parse(base).unwrap().trace);
        assert!(parse(&format!("{base} sandbox.trace=1")).unwrap().trace);
        assert!(parse(&format!("{base} sandbox.trace")).unwrap().trace);
        assert!(!parse(&format!("{base} sandbox.trace=0")).unwrap().trace);
    }

    #[test]
    fn parse_skips_empty_layer_entries() {
        let cfg = parse("cocoon.layers=a,,b, cocoon.cow=cow").unwrap();
        assert_eq!(cfg.layers, vec!["a", "b"]);
    }

    #[test]
    fn parse_ignores_empty_hostname() {
        let cfg = parse("cocoon.layers=l0 cocoon.cow=cow cocoon.hostname=").unwrap();
        assert_eq!(cfg.hostname, None);
    }

    #[test]
    fn parse_cocoon_ip_param() {
        // Exact shape BuildIPParams emits, with both DNS servers.
        let cfg = parse(
            "cocoon.layers=l0 cocoon.cow=cow \
             ip=172.20.100.5::172.20.100.1:255.255.255.0:vm1:eth0:off:8.8.8.8:8.8.4.4",
        )
        .unwrap();
        assert_eq!(
            cfg.ips,
            vec![IpParam {
                addr: "172.20.100.5".into(),
                prefix: 24,
                gateway: Some("172.20.100.1".into()),
                dns: vec!["8.8.8.8".into(), "8.8.4.4".into()],
                device: "eth0".into(),
            }]
        );
    }

    #[test]
    fn parse_ip_param_variants() {
        // No DNS, zero gateway.
        let p = parse_ip_param("10.0.0.2::0.0.0.0:255.255.254.0:vm:eth1:off").unwrap();
        assert_eq!(p.prefix, 23);
        assert_eq!(p.gateway, None);
        assert!(p.dns.is_empty());
        assert_eq!(p.device, "eth1");
        // Zero DNS entries are filtered.
        let p = parse_ip_param("10.0.0.2::10.0.0.1:255.255.255.0:vm:eth0:off:0.0.0.0").unwrap();
        assert!(p.dns.is_empty());
        // Shorthand and malformed forms are ignored.
        assert_eq!(parse_ip_param("dhcp"), None);
        assert_eq!(parse_ip_param("off"), None);
        assert_eq!(parse_ip_param("10.0.0.2::gw:not-a-mask:vm:eth0:off"), None);
        assert_eq!(parse_ip_param(":::255.255.255.0:vm:eth0:off"), None);
    }

    #[test]
    fn mask_to_prefix_cases() {
        assert_eq!(mask_to_prefix("255.255.255.0"), Some(24));
        assert_eq!(mask_to_prefix("255.255.254.0"), Some(23));
        assert_eq!(mask_to_prefix("255.255.255.255"), Some(32));
        assert_eq!(mask_to_prefix("0.0.0.0"), Some(0));
        assert_eq!(mask_to_prefix("255.0.255.0"), None);
        assert_eq!(mask_to_prefix("255.255.255"), None);
        assert_eq!(mask_to_prefix("garbage"), None);
    }

    #[test]
    fn network_unit_layout() {
        let ip = IpParam {
            addr: "172.20.100.5".into(),
            prefix: 24,
            gateway: Some("172.20.100.1".into()),
            dns: vec!["8.8.8.8".into()],
            device: "eth0".into(),
        };
        assert_eq!(
            network_unit(&ip, "aa:bb:cc:dd:ee:ff"),
            "[Match]\nMACAddress=aa:bb:cc:dd:ee:ff\n\n\
             [Network]\nAddress=172.20.100.5/24\nGateway=172.20.100.1\nDNS=8.8.8.8\n"
        );
    }

    #[test]
    fn overlay_data_layout() {
        let lower = vec!["/l/0".to_string(), "/l/1".to_string()];
        assert_eq!(
            overlay_data(&lower, "/cow"),
            "lowerdir=/l/0:/l/1,upperdir=/cow/upper,workdir=/cow/work,\
             index=on,redirect_dir=on,metacopy=on,xino=on"
        );
    }
}
