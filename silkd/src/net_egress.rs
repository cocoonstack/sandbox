//! Guest→host egress relay, one vsock connection per proxied connection or datagram; an unwired host refuses the dial (default-deny).

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Loopback port the guest reaches the proxy at (`proxy.internal:3128`).
pub const LOOPBACK_PORT: u16 = 3128;
/// Host vsock port the VMM maps to sandboxd's `<vsock_socket>_<PORT>` UDS.
pub const HOST_VSOCK_PORT: u32 = 2049;
/// Loopback port the guest reaches the SOCKS5 proxy at.
pub const SOCKS_LOOPBACK_PORT: u16 = 1080;
/// Host vsock port of the SOCKS5 proxy, mapped like `HOST_VSOCK_PORT`.
pub const SOCKS_HOST_VSOCK_PORT: u32 = 2050;
/// Loopback address of the DNS relay; systemd-resolved owns 127.0.0.53 and 127.0.0.54.
pub const DNS_ADDR: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 153);
pub const DNS_PORT: u16 = 53;
/// Host vsock port of the DNS resolver's stream door, mapped like `HOST_VSOCK_PORT`.
pub const DNS_HOST_VSOCK_PORT: u32 = 2052;
/// Host vsock port of the DNS resolver's datagram door, so the host keeps a truncated answer truncated.
pub const DNS_DATAGRAM_HOST_VSOCK_PORT: u32 = 2053;
/// Copy buffer per direction; tokio's 8 KiB default costs a syscall pair per 8 KiB in a one-vCPU guest.
#[cfg(target_os = "linux")]
const RELAY_BUF: usize = 64 << 10;
#[cfg(target_os = "linux")]
const DATAGRAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(target_os = "linux")]
const DATAGRAMS_IN_FLIGHT: usize = 256;

#[cfg(target_os = "linux")]
pub async fn serve(listen: SocketAddrV4, host_vsock_port: u32) -> io::Result<()> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(listen).await?;
    loop {
        let mut tcp = match listener.accept().await {
            Ok((conn, _)) => {
                let _ = conn.set_nodelay(true);
                conn
            }
            Err(e) => {
                eprintln!("silkd egress: accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        tokio::spawn(async move {
            let mut host = match dial_host_cloexec(host_vsock_port).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("silkd egress: host dial: {e}");
                    return;
                }
            };
            let _ =
                tokio::io::copy_bidirectional_with_sizes(&mut tcp, &mut host, RELAY_BUF, RELAY_BUF)
                    .await;
        });
    }
}

/// Relays each UDP datagram as one length-framed message on its own vsock connection and sends the one framed reply back; a failed or late exchange drops the datagram.
#[cfg(target_os = "linux")]
pub async fn serve_datagrams(listen: SocketAddrV4, host_vsock_port: u32) -> io::Result<()> {
    use std::sync::Arc;

    use tokio::net::UdpSocket;
    use tokio::sync::Semaphore;

    let sock = Arc::new(UdpSocket::bind(listen).await?);
    let slots = Arc::new(Semaphore::new(DATAGRAMS_IN_FLIGHT));
    let mut buf = vec![0u8; 2 + usize::from(u16::MAX)];
    loop {
        let (n, from) = match sock.recv_from(&mut buf[2..]).await {
            Ok(got) => got,
            Err(e) => {
                eprintln!("silkd egress: recv: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let (Ok(slot), Ok(len)) = (slots.clone().try_acquire_owned(), u16::try_from(n)) else {
            continue;
        };
        buf[..2].copy_from_slice(&len.to_be_bytes());
        let framed = buf[..2 + n].to_vec();
        let sock = sock.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let exchange = async {
                let mut host = dial_host_cloexec(host_vsock_port).await?;
                exchange_frame(&mut host, &framed).await
            };
            if let Ok(Ok(reply)) = tokio::time::timeout(DATAGRAM_TIMEOUT, exchange).await {
                let _ = sock.send_to(&reply, from).await;
            }
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn serve(_listen: SocketAddrV4, _host_vsock_port: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "silkd egress requires linux vsock",
    ))
}

#[cfg(not(target_os = "linux"))]
pub async fn serve_datagrams(_listen: SocketAddrV4, _host_vsock_port: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "silkd egress requires linux vsock",
    ))
}

/// Writes one message already behind its two-byte big-endian length and reads one reply framed the same way.
pub async fn exchange_frame<S>(conn: &mut S, framed: &[u8]) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    conn.write_all(framed).await?;
    let mut size = [0u8; 2];
    conn.read_exact(&mut size).await?;
    let mut reply = vec![0u8; usize::from(u16::from_be_bytes(size))];
    conn.read_exact(&mut reply).await?;
    Ok(reply)
}

#[cfg(target_os = "linux")]
async fn dial_host_cloexec(port: u32) -> io::Result<tokio_vsock::VsockStream> {
    let stream = tokio::task::spawn_blocking(move || {
        let stream = vsock::VsockStream::connect_with_cid_port(libc::VMADDR_CID_HOST, port)?;
        stream.set_nonblocking(true)?;
        Ok::<_, io::Error>(stream)
    })
    .await
    .map_err(io::Error::other)??;
    tokio_vsock::VsockStream::new(stream)
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::exchange_frame;

    #[tokio::test]
    async fn exchange_frame_writes_one_frame_and_reads_one_back() {
        let (mut guest, mut host) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            let mut size = [0u8; 2];
            host.read_exact(&mut size).await.unwrap();
            let mut payload = vec![0u8; usize::from(u16::from_be_bytes(size))];
            host.read_exact(&mut payload).await.unwrap();
            host.write_all(&[0, 3, b'a', b'n', b's']).await.unwrap();
            payload
        });
        assert_eq!(
            exchange_frame(&mut guest, b"\x00\x05query").await.unwrap(),
            b"ans"
        );
        assert_eq!(server.await.unwrap(), b"query");
    }

    #[tokio::test]
    async fn exchange_frame_fails_when_the_host_hangs_up() {
        let (mut guest, host) = tokio::io::duplex(1024);
        drop(host);
        assert!(exchange_frame(&mut guest, b"\x00\x05query").await.is_err());
    }
}
