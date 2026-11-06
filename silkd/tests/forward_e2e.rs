//! port_forward integration against a real TCP echo server; every test runs under a deadline so a relay deadlock fails CI.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use silkd::server::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use common::{
    FrameLines, FrameWriter, assert_error, b64, bind_local, connect, connect_with, decode,
    expect_frame, next_frame, send, type_of, under_deadline,
};

async fn echo_listener() -> u16 {
    let (listener, port) = bind_local().await;
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
    expect_frame(&mut lines, "ready").await;
    (cw, lines)
}

async fn request_listener(request: usize) -> u16 {
    let (listener, port) = bind_local().await;
    tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; request];
        while conn.read_exact(&mut buf).await.is_ok() {
            if conn.write_all(b"k").await.is_err() {
                return;
            }
        }
    });
    port
}

async fn half_close_listener() -> (u16, oneshot::Receiver<Vec<u8>>) {
    let (listener, port) = bind_local().await;
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
async fn forward_round_trips_and_done_on_server_close() {
    under_deadline(async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        send(&mut cw, json!({"v":1,"op":"data","data":b64(b"hi")})).await;
        let echoed = expect_frame(&mut lines, "data").await;
        assert_eq!(decode(&echoed), b"hi");

        send(&mut cw, json!({"v":1,"op":"data_end"})).await;
        expect_frame(&mut lines, "done").await;
    })
    .await;
}

#[tokio::test]
async fn forward_accepts_client_writes_after_the_guest_half_closes() {
    under_deadline(async {
        let (port, got) = half_close_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        let greeting = expect_frame(&mut lines, "data").await;
        assert_eq!(decode(&greeting), b"greeting");

        expect_frame(&mut lines, "done").await;

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
    .await;
}

#[tokio::test]
async fn forward_bidirectional_bulk_no_deadlock() {
    const CHUNK: usize = 3072;
    const FRAMES: usize = 400;

    under_deadline(async {
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
    .await;
}

#[tokio::test]
async fn forward_refused_port_is_not_found() {
    under_deadline(async {
        let state = Arc::new(State::new());
        let (_cw, mut lines, _) =
            connect_with(&state, json!({"v":1,"op":"port_forward","port":1})).await;
        let err = next_frame(&mut lines).await;
        assert_error(&err, "not_found");
    })
    .await;
}

#[tokio::test]
async fn a_request_during_forward_ends_its_input_and_runs_after_it() {
    under_deadline(async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;

        send(&mut cw, json!({"v":1,"op":"ps"})).await;
        expect_frame(&mut lines, "done").await;
        expect_frame(&mut lines, "procs").await;
    })
    .await;
}

#[tokio::test]
async fn forward_split_requests_do_not_wait_on_delayed_acks() {
    under_deadline(async {
        let port = request_listener(8192 + 1024).await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines) = forwarded(&state, port).await;
        let start = Instant::now();
        for _ in 0..20 {
            send(
                &mut cw,
                json!({"v":1,"op":"data","data":b64(&[b'a'; 8192])}),
            )
            .await;
            send(
                &mut cw,
                json!({"v":1,"op":"data","data":b64(&[b'b'; 1024])}),
            )
            .await;
            let reply = next_frame(&mut lines).await;
            assert_eq!(decode(&reply), b"k", "got {reply}");
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "20 split requests took {elapsed:?}: the guest-side write waits on delayed ACKs"
        );
    })
    .await;
}
