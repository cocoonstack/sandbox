//! silkd: the in-guest daemon serving newline-JSON RPC frames over hybrid-vsock; `proto::Request` is the verb list.

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
    // no lane gate needed: an unwired host refuses the per-conn vsock dial.
    tokio::spawn(relay(
        net_egress::LOOPBACK_PORT,
        net_egress::HOST_VSOCK_PORT,
    ));
    tokio::spawn(relay(
        net_egress::SOCKS_LOOPBACK_PORT,
        net_egress::SOCKS_HOST_VSOCK_PORT,
    ));
    if let Err(e) = vsock::serve(port, state).await {
        eprintln!("silkd: fatal: {e}");
        std::process::exit(1);
    }
}

async fn relay(loopback_port: u16, host_vsock_port: u32) {
    if let Err(e) = net_egress::serve(loopback_port, host_vsock_port).await {
        eprintln!("silkd egress: relay on {loopback_port}: {e}");
    }
}
