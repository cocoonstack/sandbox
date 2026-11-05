//! Guest DNS stub: queries to a loopback address go to the host resolver over vsock, one connection per UDP query.

use std::io;
use std::net::Ipv4Addr;
use std::path::Path;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Loopback address of the stub; systemd-resolved owns 127.0.0.53 and 127.0.0.54.
pub const STUB_ADDR: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 153);
/// Host vsock port of the resolver, mapped like `net_egress::HOST_VSOCK_PORT`.
pub const HOST_VSOCK_PORT: u32 = 2052;
pub const PORT: u16 = 53;

#[cfg(target_os = "linux")]
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(target_os = "linux")]
const INFLIGHT: usize = 256;
#[cfg(target_os = "linux")]
const RESOLV_CONF: &str = "/etc/resolv.conf";
#[cfg(target_os = "linux")]
const RESOLVED_DROPIN: &str = "/run/systemd/resolved.conf.d/silkd-dns.conf";
const RESOLVED_STUB_DIR: &str = "/systemd/resolve/";

/// Serves the stub over UDP; the caller relays TCP like any egress door.
#[cfg(target_os = "linux")]
pub async fn serve() -> io::Result<()> {
    use std::sync::Arc;

    use tokio::net::UdpSocket;
    use tokio::sync::Semaphore;

    if !crate::net::has_egress() {
        let systemd = Path::new("/run/systemd/system").exists();
        if let Err(e) = configure(
            Path::new(RESOLV_CONF),
            systemd.then_some(Path::new(RESOLVED_DROPIN)),
        ) {
            eprintln!("silkd dns: configure resolver: {e}");
        }
        if systemd {
            let _ = tokio::process::Command::new("systemctl")
                .args([
                    "--no-block",
                    "try-reload-or-restart",
                    "systemd-resolved.service",
                ])
                .status()
                .await;
        }
    }
    let sock = Arc::new(UdpSocket::bind((STUB_ADDR, PORT)).await?);
    let slots = Arc::new(Semaphore::new(INFLIGHT));
    let mut buf = vec![0u8; usize::from(u16::MAX)];
    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(got) => got,
            Err(e) => {
                eprintln!("silkd dns: recv: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            continue;
        };
        let query = buf[..n].to_vec();
        let sock = sock.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let answer = match tokio::time::timeout(QUERY_TIMEOUT, ask_host(&query)).await {
                Ok(Ok(answer)) => answer,
                _ => match servfail(&query) {
                    Some(answer) => answer,
                    None => return,
                },
            };
            let _ = sock.send_to(&answer, from).await;
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn serve() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "silkd dns requires linux vsock",
    ))
}

#[cfg(target_os = "linux")]
async fn ask_host(query: &[u8]) -> io::Result<Vec<u8>> {
    use tokio_vsock::{VMADDR_CID_HOST, VsockAddr, VsockStream};

    let mut host = VsockStream::connect(VsockAddr::new(VMADDR_CID_HOST, HOST_VSOCK_PORT)).await?;
    exchange(&mut host, query).await
}

/// Sends one query in DNS-over-TCP framing and reads one framed answer.
pub async fn exchange<S>(conn: &mut S, query: &[u8]) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let len =
        u16::try_from(query.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut framed = Vec::with_capacity(2 + query.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(query);
    conn.write_all(&framed).await?;
    let mut size = [0u8; 2];
    conn.read_exact(&mut size).await?;
    let mut answer = vec![0u8; usize::from(u16::from_be_bytes(size))];
    conn.read_exact(&mut answer).await?;
    Ok(answer)
}

/// Echoes the query as a SERVFAIL answer, so a client fails at once instead of on its timeout.
pub fn servfail(query: &[u8]) -> Option<Vec<u8>> {
    if query.len() < 12 {
        return None;
    }
    let mut out = query.to_vec();
    out[2] |= 0x80;
    out[3] = (out[3] & 0xf0) | 2;
    Some(out)
}

/// Points the guest resolver at the stub: systemd-resolved gets it as its only upstream, and a resolv.conf resolved does not own names it directly.
pub fn configure(resolv_conf: &Path, resolved_dropin: Option<&Path>) -> io::Result<()> {
    if let Some(dropin) = resolved_dropin {
        if let Some(dir) = dropin.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(dropin, format!("[Resolve]\nDNS={STUB_ADDR}\nDomains=~.\n"))?;
    }
    let owned_by_resolved = std::fs::read_link(resolv_conf)
        .is_ok_and(|target| target.to_string_lossy().contains(RESOLVED_STUB_DIR));
    if owned_by_resolved {
        return Ok(());
    }
    let tmp = resolv_conf.with_extension("silkd");
    std::fs::write(&tmp, format!("nameserver {STUB_ADDR}\n"))?;
    std::fs::rename(&tmp, resolv_conf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn servfail_keeps_the_id_and_question_and_sets_the_rcode() {
        let query = [
            0x12, 0x34, 0x01, 0x20, 0, 1, 0, 0, 0, 0, 0, 0, 3, b'c', b'o', b'm', 0, 0, 1, 0, 1,
        ];
        let out = servfail(&query).unwrap();
        assert_eq!(&out[..2], &[0x12, 0x34]);
        assert_eq!(out[2], 0x81);
        assert_eq!(out[3], 0x22);
        assert_eq!(&out[4..], &query[4..]);
        assert!(servfail(&query[..11]).is_none());
    }

    #[tokio::test]
    async fn exchange_frames_the_query_and_reads_one_framed_answer() {
        let (mut guest, mut host) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            let mut size = [0u8; 2];
            host.read_exact(&mut size).await.unwrap();
            let mut query = vec![0u8; usize::from(u16::from_be_bytes(size))];
            host.read_exact(&mut query).await.unwrap();
            host.write_all(&[0, 3, b'a', b'n', b's']).await.unwrap();
            query
        });
        let answer = exchange(&mut guest, b"query").await.unwrap();
        assert_eq!(answer, b"ans");
        assert_eq!(server.await.unwrap(), b"query");
    }

    #[test]
    fn configure_rewrites_a_plain_resolv_conf_and_writes_the_resolved_dropin() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join("resolv.conf");
        std::fs::write(&conf, "nameserver 10.0.0.2\n").unwrap();
        let dropin = dir.path().join("resolved.conf.d/silkd-dns.conf");
        configure(&conf, Some(&dropin)).unwrap();
        assert_eq!(
            std::fs::read_to_string(&conf).unwrap(),
            "nameserver 127.0.0.153\n"
        );
        assert_eq!(
            std::fs::read_to_string(&dropin).unwrap(),
            "[Resolve]\nDNS=127.0.0.153\nDomains=~.\n"
        );
    }

    #[test]
    fn configure_leaves_a_resolved_owned_resolv_conf_alone() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("run/systemd/resolve/stub-resolv.conf");
        std::fs::create_dir_all(stub.parent().unwrap()).unwrap();
        std::fs::write(&stub, "nameserver 127.0.0.53\n").unwrap();
        let conf = dir.path().join("resolv.conf");
        std::os::unix::fs::symlink(&stub, &conf).unwrap();
        configure(&conf, Some(&dir.path().join("silkd-dns.conf"))).unwrap();
        assert_eq!(std::fs::read_link(&conf).unwrap(), stub);
        assert_eq!(
            std::fs::read_to_string(&stub).unwrap(),
            "nameserver 127.0.0.53\n"
        );
    }

    #[test]
    fn configure_replaces_a_symlink_resolved_does_not_own() {
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other.conf");
        std::fs::write(&other, "nameserver 10.0.0.2\n").unwrap();
        let conf = dir.path().join("resolv.conf");
        std::os::unix::fs::symlink(&other, &conf).unwrap();
        configure(&conf, None).unwrap();
        assert!(std::fs::read_link(&conf).is_err());
        assert_eq!(
            std::fs::read_to_string(&conf).unwrap(),
            "nameserver 127.0.0.153\n"
        );
        assert_eq!(
            std::fs::read_to_string(&other).unwrap(),
            "nameserver 10.0.0.2\n"
        );
    }
}
