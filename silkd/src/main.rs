//! silkd: the in-guest sandbox daemon. Listens on a hybrid-vsock port for
//! newline-JSON RPC frames from the host (relayed by sandboxd) and runs
//! commands with context, tracks processes, and moves files.
//!
//! v1 verbs: exec, info, ps, kill, attach, logs. Sessions, fs, and the v2
//! code-agent verbs land per sandbox-silkd.md.

use std::sync::Arc;

use silkd::server::State;
use silkd::vsock;

const DEFAULT_PORT: u32 = 2048;

#[tokio::main]
async fn main() {
    let port = std::env::var("SILKD_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let state = Arc::new(State::new());
    if let Err(e) = vsock::serve(port, state).await {
        eprintln!("silkd: fatal: {e}");
        std::process::exit(1);
    }
}
