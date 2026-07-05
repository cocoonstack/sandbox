//! pty.open E2E: a shell under a pseudo-terminal, driven over a duplex.
//! openpty exists on macOS and Linux, so this runs on the dev host.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use silkd::server::{buffer, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::io::{ReadHalf, WriteHalf};

type Client = tokio::io::DuplexStream;

async fn send(cw: &mut WriteHalf<Client>, frame: Value) {
    cw.write_all(frame.to_string().as_bytes()).await.unwrap();
    cw.write_all(b"\n").await.unwrap();
}

/// Reads frames until one satisfies `pred` or 5s elapses.
async fn read_until(
    lines: &mut Lines<BufReader<ReadHalf<Client>>>,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let line = lines.next_line().await.unwrap().expect("stream closed");
            let v: Value = serde_json::from_str(&line).unwrap();
            if pred(&v) {
                return v;
            }
        }
    })
    .await
    .expect("frame never arrived")
}

#[tokio::test]
async fn pty_runs_a_shell_and_echoes() {
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(1 << 20);
    let st = Arc::clone(&state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { st.serve(buffer(sr), sw).await });

    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;

    let started = read_until(&mut lines, |v| v["type"] == "started").await;
    let pid = started["pid"].as_u64().unwrap();
    assert!(pid > 0);

    // A pty echoes input; run a command and look for its output.
    send(
        &mut cw,
        json!({"op":"stdin","data":common::b64(b"echo silk-pty-marker\n")}),
    )
    .await;
    let out = read_until(&mut lines, |v| {
        v["type"] == "stdout"
            && String::from_utf8_lossy(&common::decode(v)).contains("silk-pty-marker")
    })
    .await;
    assert_eq!(out["type"], "stdout");

    // Exit the shell; the stream ends with an exit frame.
    send(&mut cw, json!({"op":"stdin","data":common::b64(b"exit\n")})).await;
    let exit = read_until(&mut lines, |v| v["type"] == "exit").await;
    assert!(exit["code"].is_number());

    drop(cw);
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_appears_in_ps_and_resizes() {
    let state = Arc::new(State::new());
    let (client, server) = tokio::io::duplex(1 << 20);
    let st = Arc::clone(&state);
    let (sr, sw) = tokio::io::split(server);
    tokio::spawn(async move { st.serve(buffer(sr), sw).await });

    let (cr, mut cw) = tokio::io::split(client);
    let mut lines = BufReader::new(cr).lines();
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;
    let pid = read_until(&mut lines, |v| v["type"] == "started").await["pid"]
        .as_u64()
        .unwrap();

    // ps (shared table) sees the pty as a process; resize succeeds on it.
    let ps = common::one(&state, r#"{"op":"ps"}"#).await;
    assert!(
        ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid)),
        "pty not in ps: {ps:?}"
    );
    let resized = common::one(
        &state,
        &json!({"op":"pty_resize","pid":pid,"cols":120,"rows":40}).to_string(),
    )
    .await;
    assert_eq!(common::type_of(&resized[0]), "done");

    drop(cw);
}

#[tokio::test]
async fn resize_unknown_pid_is_not_found() {
    let frames =
        common::exchange(
            &[json!({"op":"pty_resize","pid":999999,"cols":80,"rows":24}).to_string()],
        )
        .await;
    assert_eq!(common::type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}
