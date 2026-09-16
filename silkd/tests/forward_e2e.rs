//! port_forward integration against a real TCP echo server; every test runs under a deadline so a relay deadlock fails CI.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;

use serde_json::json;
use silkd::server::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
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
