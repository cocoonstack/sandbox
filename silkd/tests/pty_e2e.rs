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

use common::{
    FrameLines, FrameWriter, alive, assert_error, b64, connect_with, decode, frames_until, one,
    proc_entry, request, send, type_of, wait_for_path,
};

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

async fn open_pty(
    state: &Arc<State>,
) -> (
    FrameWriter,
    FrameLines,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    connect_with(state, json!({"op":"pty_open","cols":80,"rows":24})).await
}

async fn wait_gone(pid: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while alive(pid) {
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
    let (mut cw, mut lines, handle) = open_pty(&state).await;

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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_pty_shell_holds_no_master_fd() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = open_pty(&state).await;
    read_until(&mut lines, |v| v["type"] == "started").await;
    send(
        &mut cw,
        json!({"op":"stdin","data":b64(b"echo silk-$(ls -l /proc/$$/fd | grep -c ptmx)-masters\n")}),
    )
    .await;
    let mut out = String::new();
    while !(0..10).any(|n| out.contains(&format!("silk-{n}-masters"))) {
        let frame = read_until(&mut lines, |v| v["type"] == "stdout").await;
        out.push_str(&String::from_utf8_lossy(&decode(&frame)));
    }
    assert!(out.contains("silk-0-masters"), "{out}");
    send(&mut cw, json!({"op":"stdin","data":b64(b"exit\n")})).await;
    read_until(&mut lines, |v| v["type"] == "exit").await;
    cw.shutdown().await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_appears_in_ps_and_resizes() {
    let state = Arc::new(State::new());
    let (cw, mut lines, _) = open_pty(&state).await;
    let pid = read_until(&mut lines, |v| v["type"] == "started").await["pid"]
        .as_u64()
        .unwrap();

    let ps = one(&state, r#"{"op":"ps"}"#).await;
    assert!(proc_entry(&ps, pid).is_some(), "pty not in ps: {ps:?}");
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
    let frames = request(json!({"op":"pty_resize","pid":999999,"cols":80,"rows":24})).await;
    assert_error(&frames[0], "not_found");
}

#[tokio::test]
async fn stdin_nobody_reads_after_the_shell_exits_does_not_wedge_the_next_request() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = open_pty(&state).await;
    read_until(&mut lines, |v| v["type"] == "started").await;
    send(
        &mut cw,
        json!({"op":"stdin","data":b64(b"sleep 20 &\nexit\n")}),
    )
    .await;
    read_until(&mut lines, |v| v["type"] == "exit").await;

    let chunk = b64(&b"unread line\n".repeat(2048));
    let answered = tokio::time::timeout(Duration::from_secs(8), async {
        for _ in 0..64 {
            send(&mut cw, json!({"op":"stdin","data":chunk})).await;
        }
        send(&mut cw, json!({"op":"ps"})).await;
        read_until(&mut lines, |v| v.get("procs").is_some()).await;
    })
    .await;
    assert!(
        answered.is_ok(),
        "the next request is wedged behind stdin nobody reads"
    );

    cw.shutdown().await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn pty_disconnect_tears_down_without_spin() {
    let state = Arc::new(State::new());
    let (cw, mut lines, handle) = open_pty(&state).await;
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
        proc_entry(&ps, pid).is_none(),
        "pty still in table after disconnect: {ps:?}"
    );
}

#[tokio::test]
async fn pty_disconnect_hangs_up_background_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("job");
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = open_pty(&state).await;
    read_until(&mut lines, |v| v["type"] == "started").await;
    let cmd = format!("sleep 300 & echo $! > {}\n", pid_file.display());
    send(&mut cw, json!({"op":"stdin","data":b64(cmd.as_bytes())})).await;
    wait_for_path(&pid_file).await;
    let job: u32 = loop {
        if let Ok(pid) = std::fs::read_to_string(&pid_file).unwrap().trim().parse() {
            break pid;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    drop(lines);
    drop(cw);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("pty did not tear down on disconnect");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while alive(job) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "background job {job} outlived the hang-up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn pty_started_write_failure_kills_the_shell_and_unregisters_it() {
    let table = Table::default();
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

#[tokio::test]
#[ignore = "needs root"]
async fn a_user_pty_shell_owns_its_tty() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, handle) = connect_with(
        &state,
        json!({"op":"pty_open","cols":80,"rows":24,"user":"nobody"}),
    )
    .await;
    read_until(&mut lines, |v| v["type"] == "started").await;
    send(
        &mut cw,
        json!({"op":"stdin","data":b64(b"echo silk-owner-$(stat -c %U \"$(tty)\")-end\n")}),
    )
    .await;
    let mut out = String::new();
    while !["nobody", "root"]
        .iter()
        .any(|u| out.contains(&format!("silk-owner-{u}-end")))
    {
        let frame = read_until(&mut lines, |v| v["type"] == "stdout").await;
        out.push_str(&String::from_utf8_lossy(&decode(&frame)));
    }
    assert!(out.contains("silk-owner-nobody-end"), "{out}");
    send(&mut cw, json!({"op":"stdin","data":b64(b"exit\n")})).await;
    read_until(&mut lines, |v| v["type"] == "exit").await;
    cw.shutdown().await.unwrap();
    handle.await.unwrap().unwrap();
}
