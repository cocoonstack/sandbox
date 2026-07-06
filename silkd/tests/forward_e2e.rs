//! port_forward integration: a real TCP echo server behind the verb.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use silkd::server::State;

mod common;

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
    let port = echo_listener().await;
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (sr, sw) = tokio::io::split(server);
    let state2 = Arc::clone(&state);
    tokio::spawn(async move { state2.serve(BufReader::new(sr), sw).await });
    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();

    cw.write_all(format!("{{\"v\":1,\"op\":\"port_forward\",\"port\":{port}}}\n").as_bytes())
        .await
        .expect("send request");
    let ready = lines.next_line().await.expect("read").expect("ready");
    assert!(ready.contains("\"ready\""), "got {ready}");

    // "hi" → base64 aGk= ; the echo server must send it straight back.
    cw.write_all(b"{\"v\":1,\"op\":\"data\",\"data\":\"aGk=\"}\n")
        .await
        .expect("send data");
    let echoed = lines.next_line().await.expect("read").expect("data");
    assert!(
        echoed.contains("\"data\"") && echoed.contains("aGk="),
        "got {echoed}"
    );

    // Half-close: the echo server sees EOF, closes, and the verb ends with done.
    cw.write_all(b"{\"v\":1,\"op\":\"data_end\"}\n")
        .await
        .expect("send data_end");
    let done = lines.next_line().await.expect("read").expect("done");
    assert!(done.contains("\"done\""), "got {done}");
}

// Bidirectional bulk transfer must not head-of-line deadlock: the client
// streams > socket-buffer worth of input into an echo server that is
// simultaneously streaming it all back, so both directions have to make
// progress concurrently. A single-select relay (write_all stalling the read
// arm) hangs this test.
#[tokio::test]
async fn forward_bidirectional_bulk_no_deadlock() {
    const CHUNK: usize = 3072; // multiple of 3: base64 with no padding
    const FRAMES: usize = 400; // ~1.2 MiB, well past socket + duplex buffers

    let port = echo_listener().await;
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (sr, sw) = tokio::io::split(server);
    tokio::spawn(async move { state.serve(BufReader::new(sr), sw).await });
    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();

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
}

/// Extracts the base64 body of a `data` response frame; None for ready/done.
fn data_payload(line: &str) -> Option<&str> {
    let start = line.find("\"data\":\"")? + "\"data\":\"".len();
    let rest = &line[start..];
    Some(&rest[..rest.find('"')?])
}

#[tokio::test]
async fn forward_refused_port_is_not_found() {
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (sr, sw) = tokio::io::split(server);
    tokio::spawn(async move { state.serve(BufReader::new(sr), sw).await });
    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();

    // Port 1 on loopback: nothing listens there in the test environment.
    cw.write_all(b"{\"v\":1,\"op\":\"port_forward\",\"port\":1}\n")
        .await
        .expect("send request");
    let err = lines.next_line().await.expect("read").expect("error");
    assert!(
        err.contains("\"error\"") && err.contains("not_found"),
        "got {err}"
    );
}
