//! pty.open E2E: a shell under a pseudo-terminal, driven over a duplex.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use serde_json::{Value, json};
use silkd::proc::Table;
use silkd::proto::PtyReq;
use silkd::pty;
use silkd::server::State;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use common::{FrameLines, b64, connect, decode, exchange, frames_until, one, send, type_of};

struct FailingSink(Vec<u8>);

impl AsyncWrite for FailingSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.0.extend_from_slice(buf);
        Poll::Ready(Err(std::io::Error::other("client gone")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn read_until(lines: &mut FrameLines, pred: impl Fn(&Value) -> bool) -> Value {
    frames_until(lines, pred).await.pop().unwrap()
}

async fn wait_gone(pid: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    // SAFETY: kill(2) with signal 0 only probes whether the pid exists; it takes no pointers.
    while unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "shell {pid} still alive 5 s after the started write failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn pty_runs_a_shell_and_echoes() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect(&state);
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;

    let started = read_until(&mut lines, |v| v["type"] == "started").await;
    let pid = started["pid"].as_u64().unwrap();
    assert!(pid > 0);

    send(
        &mut cw,
        json!({"op":"stdin","data":b64(b"echo silk-pty-marker\n")}),
    )
    .await;
    read_until(&mut lines, |v| {
        v["type"] == "stdout" && String::from_utf8_lossy(&decode(v)).contains("silk-pty-marker")
    })
    .await;

    send(&mut cw, json!({"op":"stdin","data":b64(b"exit\n")})).await;
    let exit = read_until(&mut lines, |v| v["type"] == "exit").await;
    assert_eq!(exit["code"], 0, "{exit:?}");

    cw.shutdown().await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_appears_in_ps_and_resizes() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect(&state);
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;
    let pid = read_until(&mut lines, |v| v["type"] == "started").await["pid"]
        .as_u64()
        .unwrap();

    let ps = one(&state, r#"{"op":"ps"}"#).await;
    assert!(
        ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid)),
        "pty not in ps: {ps:?}"
    );
    let resized = one(
        &state,
        &json!({"op":"pty_resize","pid":pid,"cols":120,"rows":40}).to_string(),
    )
    .await;
    assert_eq!(type_of(&resized[0]), "done");

    drop(cw);
}

#[tokio::test]
async fn resize_unknown_pid_is_not_found() {
    let frames =
        exchange(&[json!({"op":"pty_resize","pid":999999,"cols":80,"rows":24}).to_string()]).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn pty_disconnect_tears_down_without_spin() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect(&state);
    send(&mut cw, json!({"op":"pty_open","cols":80,"rows":24})).await;
    let pid = read_until(&mut lines, |v| v["type"] == "started").await["pid"]
        .as_u64()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(lines);
    drop(cw);

    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("pty did not tear down on disconnect");
    let ps = one(&state, r#"{"op":"ps"}"#).await;
    assert!(
        !ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid)),
        "pty still in table after disconnect: {ps:?}"
    );
}

#[tokio::test]
async fn pty_started_write_failure_kills_the_shell_and_unregisters_it() {
    let table = Table::new();
    let (_client, client_rx) = mpsc::channel(1);
    let req = PtyReq {
        cols: 80,
        rows: 24,
        cwd: None,
        env: HashMap::new(),
        user: None,
    };
    let mut sink = FailingSink(Vec::new());

    let err = pty::open(&table, 0, req, client_rx, &mut sink)
        .await
        .expect_err("open must propagate the started write failure");
    assert_eq!(err.to_string(), "client gone");

    let started: Value = serde_json::from_slice(&sink.0).expect("started frame reached the writer");
    assert_eq!(started["type"], "started");
    let pid = started["pid"].as_u64().unwrap() as u32;
    assert!(
        table.list().is_empty(),
        "pty still registered after the write failure"
    );
    wait_gone(pid).await;
}
