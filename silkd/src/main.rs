//! silkd: the in-guest sandbox daemon. Listens on a hybrid-vsock port for
//! newline-JSON RPC frames from the host (relayed by sandboxd) and runs
//! commands with context, tracks processes, moves files, and holds sessions.
//!
//! Verbs: exec, info, ps, kill, attach, logs, session.{create,list,rm},
//! fs.{write,read,list,stat,mkdir,rm,rename,push,pull,find,replace,watch},
//! pty.{open,resize}, git.{clone,status,add,commit,push,pull,branch}.
//! Design: cocoon-specs/design/sandbox-silkd.md.

use std::sync::Arc;

use silkd::server::State;
use silkd::{net_egress, session, vsock};

const DEFAULT_PORT: u32 = 2048;

#[tokio::main]
async fn main() {
    let port = std::env::var("SILKD_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let state = Arc::new(State::new());
    tokio::spawn(session::reap_loop(
        state.sessions.clone(),
        session::IDLE_TTL,
        session::REAP_INTERVAL,
    ));
    // Loopback→host egress relay: harmless when the host wired no proxy (the
    // per-conn vsock dial is refused), so it needs no lane gate here.
    tokio::spawn(net_egress::serve(
        net_egress::LOOPBACK_PORT,
        net_egress::HOST_VSOCK_PORT,
    ));
    if let Err(e) = vsock::serve(port, state).await {
        eprintln!("silkd: fatal: {e}");
        std::process::exit(1);
    }
}
