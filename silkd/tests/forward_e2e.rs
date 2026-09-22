//! port_forward integration against a real TCP echo server; every test runs under a deadline so a relay deadlock fails CI.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;

use serde_json::json;
use silkd::server::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

use common::{DEADLINE, FrameLines, FrameWriter, b64, connect, decode, next_frame, send, type_of};

async fn echo_listener() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((mut conn, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match conn.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if conn.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    port
}

async fn forwarded(state: &Arc<State>, port: u16) -> (FrameWriter, FrameLines) {
    let (mut cw, mut lines, _) = connect(state);
    send(&mut cw, json!({"v":1,"op":"port_forward","port":port})).await;
    let ready = next_frame(&mut lines).await;
    assert_eq!(type_of(&ready), "ready", "got {ready}");
    (cw, lines)
}

#[tokio::test]
async fn forward_round_trips_and_done_on_server_close() {
    timeout(DEADLINE, async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        send(&mut cw, json!({"v":1,"op":"data","data":b64(b"hi")})).await;
        let echoed = next_frame(&mut lines).await;
        assert_eq!(type_of(&echoed), "data", "got {echoed}");
        assert_eq!(decode(&echoed), b"hi");

        send(&mut cw, json!({"v":1,"op":"data_end"})).await;
        let done = next_frame(&mut lines).await;
        assert_eq!(type_of(&done), "done", "got {done}");
    })
    .await
    .expect("test deadline");
}
/// A guest that half-closes must not take the client's write direction with it.
/// What it reads afterwards travels back to the test body: an assertion inside a
/// spawned task is swallowed, so it would pass either way.
async fn half_close_listener() -> (u16, oneshot::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (conn, _) = listener.accept().await.expect("accept");
        let (mut r, mut w) = conn.into_split();
        w.write_all(b"greeting").await.expect("greeting");
        w.shutdown().await.expect("shutdown");
        let mut got = Vec::new();
        let _ = r.read_to_end(&mut got).await;
        let _ = tx.send(got);
    });
    (port, rx)
}

#[tokio::test]
async fn forward_accepts_client_writes_after_the_guest_half_closes() {
    timeout(DEADLINE, async {
        let (port, got) = half_close_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        let greeting = next_frame(&mut lines).await;
        assert_eq!(type_of(&greeting), "data", "got {greeting}");
        assert_eq!(decode(&greeting), b"greeting");

        let done = next_frame(&mut lines).await;
        assert_eq!(type_of(&done), "done", "got {done}");

        send(
            &mut cw,
            json!({"v":1,"op":"data","data":b64(b"after-done")}),
        )
        .await;
        send(&mut cw, json!({"v":1,"op":"data_end"})).await;

        let seen = got.await.expect("guest reported what it read");
        assert_eq!(
            seen, b"after-done",
            "guest lost the client's post-done bytes"
        );
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn forward_bidirectional_bulk_no_deadlock() {
    const CHUNK: usize = 3072;
    const FRAMES: usize = 400;

    timeout(DEADLINE, async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        let frame = json!({"v":1,"op":"data","data":b64(&[b'x'; CHUNK])}).to_string() + "\n";
        let writer = tokio::spawn(async move {
            for _ in 0..FRAMES {
                cw.write_all(frame.as_bytes()).await.expect("write frame");
            }
            send(&mut cw, json!({"v":1,"op":"data_end"})).await;
        });

        let mut got = 0usize;
        while got < CHUNK * FRAMES {
            let frame = next_frame(&mut lines).await;
            if type_of(&frame) == "data" {
                got += decode(&frame).len();
            }
        }
        writer.await.expect("writer");
        assert_eq!(got, CHUNK * FRAMES);
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn forward_refused_port_is_not_found() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = connect(&state);

        send(&mut cw, json!({"v":1,"op":"port_forward","port":1})).await;
        let err = next_frame(&mut lines).await;
        assert_eq!(type_of(&err), "error", "got {err}");
        assert_eq!(err["kind"], "not_found", "got {err}");
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn a_request_during_forward_ends_its_input_and_runs_after_it() {
    timeout(DEADLINE, async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        send(&mut cw, json!({"v":1,"op":"ps"})).await;
        let done = next_frame(&mut lines).await;
        assert_eq!(type_of(&done), "done", "got {done}");
        let procs = next_frame(&mut lines).await;
        assert_eq!(type_of(&procs), "procs", "got {procs}");
    })
    .await
    .expect("test deadline");
}
