//! vsock listener. On Linux silkd binds the guest's hybrid-vsock port and
//! serves each connection; the host (sandboxd) reaches it through the VMM
//! muxer's `CONNECT <port>` handshake, which the VMM answers — silkd sees a
//! plain byte stream. Off Linux the crate still builds (host tooling, tests)
//! with a stub so `cargo test` runs everywhere.

use std::sync::Arc;

use crate::server::State;

#[cfg(target_os = "linux")]
pub async fn serve(port: u32, state: Arc<State>) -> std::io::Result<()> {
    use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};

    let listener = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, port))?;
    loop {
        // A transient accept error (e.g. EMFILE under load) must not tear down
        // the daemon and lose the process table and every session — only a bind
        // failure above is fatal.
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
            // Data/stdin frames run 43KB–1.3MB on the wire; the 8KB BufReader
            // default would cost several read syscalls per frame on bulk paths.
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
