//! Guest→host egress relay, one vsock connection per proxied connection; an unwired host refuses the dial (default-deny).

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
