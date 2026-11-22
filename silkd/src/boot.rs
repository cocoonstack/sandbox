//! Boot notice: once per guest boot, before silkd serves, the host learns the boot id and restores what /run lost.

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HOST_VSOCK_PORT: u32 = 2054;
pub const MARKER: &str = "/run/silkd.booted";
pub const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";
pub const TIMEOUT: Duration = Duration::from_secs(2);

pub async fn announce<S, F, Fut>(marker: &Path, boot_id: &Path, dial: F) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce() -> Fut,
    Fut: Future<Output = io::Result<S>>,
{
    if let Err(e) = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(marker)
        .await
    {
        if e.kind() == io::ErrorKind::AlreadyExists {
            return Ok(false);
        }
        return Err(e);
    }
    let id = tokio::fs::read_to_string(boot_id).await?;
    let mut host = dial().await?;
    let line = format!("{}\n", id.trim_end());
    host.write_all(line.as_bytes()).await?;
    let mut ack = [0u8; 1];
    host.read_exact(&mut ack).await?;
    Ok(true)
}

#[cfg(target_os = "linux")]
pub async fn notify_host() {
    let notice = announce(Path::new(MARKER), Path::new(BOOT_ID), || {
        crate::net_egress::dial_host_cloexec(HOST_VSOCK_PORT)
    });
    match tokio::time::timeout(TIMEOUT, notice).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => eprintln!("silkd boot: notice: {e}"),
        Err(_) => eprintln!("silkd boot: notice: no ack within {TIMEOUT:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::announce;

    #[tokio::test]
    async fn announce_sends_one_notice_per_marker_absence() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("silkd.booted");
        let boot_id = dir.path().join("boot_id");
        std::fs::write(&boot_id, "5f1c-boot\n").unwrap();
        let dials = &AtomicUsize::new(0);
        let mut heard = Vec::new();
        for _ in 0..2 {
            let (guest, host) = tokio::io::duplex(256);
            let ack = tokio::spawn(ack_one(host));
            let sent = announce(&marker, &boot_id, move || async move {
                dials.fetch_add(1, Ordering::Relaxed);
                Ok::<DuplexStream, std::io::Error>(guest)
            })
            .await
            .unwrap();
            heard.push((sent, ack.await.unwrap()));
        }
        assert_eq!(
            heard,
            [(true, Some("5f1c-boot".to_string())), (false, None)]
        );
        assert_eq!(dials.load(Ordering::Relaxed), 1);

        std::fs::remove_file(&marker).unwrap();
        let (guest, host) = tokio::io::duplex(256);
        let ack = tokio::spawn(ack_one(host));
        let sent = announce(&marker, &boot_id, move || async move {
            Ok::<DuplexStream, std::io::Error>(guest)
        })
        .await
        .unwrap();
        assert!(sent);
        assert_eq!(ack.await.unwrap().as_deref(), Some("5f1c-boot"));
    }

    #[tokio::test]
    async fn announce_fails_when_the_host_hangs_up_before_the_ack() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("silkd.booted");
        let boot_id = dir.path().join("boot_id");
        std::fs::write(&boot_id, "5f1c-boot\n").unwrap();
        let (guest, host) = tokio::io::duplex(256);
        drop(host);
        let res = announce(&marker, &boot_id, move || async move {
            Ok::<DuplexStream, std::io::Error>(guest)
        })
        .await;
        assert!(res.is_err());
        assert!(marker.exists());
    }

    async fn ack_one(host: DuplexStream) -> Option<String> {
        let mut r = BufReader::new(host);
        let mut line = String::new();
        if r.read_line(&mut line).await.unwrap() == 0 {
            return None;
        }
        r.get_mut().write_all(b"\n").await.unwrap();
        Some(line.trim_end().to_string())
    }
}
