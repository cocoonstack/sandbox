//! Guest→host egress relay. Binds a loopback proxy port; for each accepted TCP
//! connection it opens one guest→host vsock connection to sandboxd's egress
//! proxy and splices raw bytes. One vsock connection per proxied connection,
//! mirroring the host→guest `port_forward` model — the proxy speaks HTTP on top
//! of this byte pipe, so nothing here frames or inspects the payload. The none
//! lane's only route out; when the host has not wired a proxy, the per-conn
//! vsock dial is refused and the guest client sees a closed proxy (default-deny).

use std::io;

/// Loopback port the guest reaches the proxy at (`proxy.internal:3128`).
pub const LOOPBACK_PORT: u16 = 3128;
/// Host vsock port the VMM maps to sandboxd's `<vsock_socket>_<PORT>` UDS.
pub const HOST_VSOCK_PORT: u32 = 2049;

#[cfg(target_os = "linux")]
pub async fn serve(loopback_port: u16, host_vsock_port: u32) -> io::Result<()> {
    use tokio::net::TcpListener;
    use tokio_vsock::{VMADDR_CID_HOST, VsockAddr, VsockStream};

    let listener = TcpListener::bind(("127.0.0.1", loopback_port)).await?;
    loop {
        let mut tcp = match listener.accept().await {
            Ok((conn, _)) => conn,
            Err(e) => {
                eprintln!("silkd egress: accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        tokio::spawn(async move {
            let addr = VsockAddr::new(VMADDR_CID_HOST, host_vsock_port);
            let mut host = match VsockStream::connect(addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("silkd egress: host dial: {e}");
                    return;
                }
            };
            let _ = tokio::io::copy_bidirectional(&mut tcp, &mut host).await;
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn serve(_loopback_port: u16, _host_vsock_port: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "silkd egress requires linux vsock",
    ))
}
