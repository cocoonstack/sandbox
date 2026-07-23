//! pty.open E2E: a shell under a pseudo-terminal, driven over a duplex.
//! openpty exists on macOS and Linux, so this runs on the dev host.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code, like #[cfg(test)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{FrameLines, FrameWriter};
use serde_json::{Value, json};
use silkd::server::State;
use tokio::io::AsyncWriteExt;

async fn send(cw: &mut FrameWriter, frame: Value) {
    cw.write_all(frame.to_string().as_bytes()).await.unwrap();
    cw.write_all(b"\n").await.unwrap();
}

/// Reads frames until one satisfies `pred` or 5s elapses.
async fn read_until(lines: &mut FrameLines, pred: impl Fn(&Value) -> bool) -> Value {
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
    let (mut cw, mut lines, handle) = common::connect(&state);
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

    send(&mut cw, json!({"op":"stdin","data":common::b64(b"exit\n")})).await;
    let exit = read_until(&mut lines, |v| v["type"] == "exit").await;
    assert!(exit["code"].is_number());

    drop(cw);
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_appears_in_ps_and_resizes() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = common::connect(&state);
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

#[tokio::test]
async fn pty_disconnect_tears_down_without_spin() {
    // Client disconnects while the shell is idle. The pty must tear down (kill
    // the shell, drop the table entry, end the serve task) instead of
    // busy-spinning on the closed client channel.
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = common::connect(&state);
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;
    let pid = read_until(&mut lines, |v| v["type"] == "started").await["pid"]
        .as_u64()
        .unwrap();

    // Let the shell settle, then disconnect.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(lines);
    drop(cw);

    // The serve task returns promptly (no spin); the final Exit-write to the
    // gone client may error, which is fine — the point is it returns.
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("pty did not tear down on disconnect");
    let ps = common::one(&state, r#"{"op":"ps"}"#).await;
    assert!(
        !ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid)),
        "pty still in table after disconnect: {ps:?}"
    );
}
