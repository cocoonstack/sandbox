//! port_forward integration: a real TCP echo server behind the verb. Every
//! test runs under a deadline — the regressions this suite exists to catch
//! (relay deadlocks) would otherwise hang CI instead of failing it.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

use silkd::server::State;

const DEADLINE: Duration = Duration::from_secs(30);

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

#[tokio::test]
async fn forward_round_trips_and_done_on_server_close() {
    timeout(DEADLINE, async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = common::connect(&state);

        cw.write_all(format!("{{\"v\":1,\"op\":\"port_forward\",\"port\":{port}}}\n").as_bytes())
            .await
            .expect("send request");
        let ready = lines.next_line().await.expect("read").expect("ready");
        assert!(ready.contains("\"ready\""), "got {ready}");

        cw.write_all(b"{\"v\":1,\"op\":\"data\",\"data\":\"aGk=\"}\n")
            .await
            .expect("send data");
        let echoed = lines.next_line().await.expect("read").expect("data");
        assert!(
            echoed.contains("\"data\"") && echoed.contains("aGk="),
            "got {echoed}"
        );

        cw.write_all(b"{\"v\":1,\"op\":\"data_end\"}\n")
            .await
            .expect("send data_end");
        let done = lines.next_line().await.expect("read").expect("done");
        assert!(done.contains("\"done\""), "got {done}");
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
        let (mut cw, mut lines, _) = common::connect(&state);

        cw.write_all(format!("{{\"v\":1,\"op\":\"port_forward\",\"port\":{port}}}\n").as_bytes())
            .await
            .expect("send request");
        let ready = lines.next_line().await.expect("read").expect("ready");
        assert!(ready.contains("\"ready\""), "got {ready}");

        let payload = STANDARD.encode(vec![b'x'; CHUNK]);
        let frame = format!("{{\"v\":1,\"op\":\"data\",\"data\":\"{payload}\"}}\n");
        let writer = tokio::spawn(async move {
            for _ in 0..FRAMES {
                cw.write_all(frame.as_bytes()).await.expect("write frame");
            }
            cw.write_all(b"{\"v\":1,\"op\":\"data_end\"}\n")
                .await
                .expect("data_end");
        });

        let mut got = 0usize;
        while got < CHUNK * FRAMES {
            let line = lines
                .next_line()
                .await
                .expect("read")
                .expect("stream ended before all bytes echoed");
            if let Some(b64) = data_payload(&line) {
                got += STANDARD.decode(b64).expect("decode").len();
            }
        }
        writer.await.expect("writer");
        assert_eq!(got, CHUNK * FRAMES);
    })
    .await
    .expect("test deadline");
}

/// Extracts the base64 body of a `data` response frame; None for ready/done.
fn data_payload(line: &str) -> Option<&str> {
    let start = line.find("\"data\":\"")? + "\"data\":\"".len();
    let rest = &line[start..];
    Some(&rest[..rest.find('"')?])
}

#[tokio::test]
async fn forward_refused_port_is_not_found() {
    timeout(DEADLINE, async {
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = common::connect(&state);

        cw.write_all(b"{\"v\":1,\"op\":\"port_forward\",\"port\":1}\n")
            .await
            .expect("send request");
        let err = lines.next_line().await.expect("read").expect("error");
        assert!(
            err.contains("\"error\"") && err.contains("not_found"),
            "got {err}"
        );
    })
    .await
    .expect("test deadline");
}

#[tokio::test]
async fn forward_stray_frame_is_bad_request() {
    timeout(DEADLINE, async {
        let port = echo_listener().await;
        let state = Arc::new(State::new());
        let (mut cw, mut lines, _) = common::connect(&state);

        cw.write_all(format!("{{\"v\":1,\"op\":\"port_forward\",\"port\":{port}}}\n").as_bytes())
            .await
            .expect("send request");
        let ready = lines.next_line().await.expect("read").expect("ready");
        assert!(ready.contains("\"ready\""), "got {ready}");

        cw.write_all(b"{\"v\":1,\"op\":\"ps\"}\n")
            .await
            .expect("send stray");
        let err = lines.next_line().await.expect("read").expect("error");
        assert!(
            err.contains("\"error\"") && err.contains("bad_request"),
            "got {err}"
        );
    })
    .await
    .expect("test deadline");
}
