//! vsock listener: silkd sees a plain byte stream, the VMM muxer having answered the `CONNECT <port>` handshake.

use std::sync::Arc;

use crate::server::State;

#[cfg(target_os = "linux")]
pub async fn serve(port: u32, state: Arc<State>) -> std::io::Result<()> {
    use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, port))?;
    loop {
        // a transient accept error must not tear down the daemon and lose every session.
        let conn = match listener.accept().await {
            Ok((conn, _)) => conn,
            Err(e) => {
                eprintln!("silkd: accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let (read, write) = tokio::io::split(conn);
            // bulk frames run 43KB-1.3MB, so the 8KB BufReader default costs several reads per frame.
            let reader = tokio::io::BufReader::with_capacity(64 * 1024, read);
            if let Err(e) = state.serve(reader, write).await {
                eprintln!("silkd: connection: {e}");
            }
        });
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn serve(_port: u32, _state: Arc<State>) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "silkd requires linux vsock",
    ))
}
