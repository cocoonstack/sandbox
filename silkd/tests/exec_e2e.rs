//! In-process E2E over an in-memory duplex pipe (no vsock needed): drive the
//! server exactly as a relayed host connection would and assert the frame
//! stream. Spawns real processes (echo, sh), so it runs on any Unix — keep it
//! that way so exec-path bugs surface on the dev host, not only on Linux CI.

use std::sync::Arc;

use serde_json::Value;
use silkd::server::{buffer, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn roundtrip(request_line: &str) -> Vec<Value> {
    request_on(&Arc::new(State::new()), request_line).await
}

/// Drives one request line against a shared state, returning the response
/// frames. A shared state lets a test chain calls (detach, then ps/logs).
async fn request_on(state: &Arc<State>, request_line: &str) -> Vec<Value> {
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let state = Arc::clone(state);
    let (sr, sw) = tokio::io::split(server);
    let handle = tokio::spawn(async move { state.serve(buffer(sr), sw).await });

    client
        .write_all(format!("{request_line}\n").as_bytes())
        .await
        .unwrap();
    client.shutdown().await.unwrap();

    let mut frames = Vec::new();
    let mut lines = BufReader::new(client).lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        frames.push(serde_json::from_str(&line).unwrap());
    }
    handle.await.unwrap().unwrap();
    frames
}

fn type_of(frame: &Value) -> &str {
    frame["type"].as_str().unwrap_or("")
}

fn decode(frame: &Value) -> String {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(frame["data"].as_str().unwrap())
        .unwrap();
    String::from_utf8(raw).unwrap()
}

#[tokio::test]
async fn exec_streams_stdout_then_exit() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/echo","-n","hello"]}"#).await;
    assert_eq!(type_of(&frames[0]), "started");
    let body: String = frames
        .iter()
        .filter(|f| type_of(f) == "stdout")
        .map(decode)
        .collect();
    assert_eq!(body, "hello");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn nonzero_exit_is_reported() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","exit 7"]}"#).await;
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 7);
}

#[tokio::test]
async fn empty_argv_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":[]}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn info_reports_version_and_proto() {
    let frames = roundtrip(r#"{"op":"info"}"#).await;
    assert_eq!(type_of(&frames[0]), "info");
    assert_eq!(frames[0]["proto"], 1);
    assert!(frames[0]["version"].as_str().is_some());
}

#[tokio::test]
async fn kill_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"kill","pid":999999}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn daemonizing_child_does_not_wedge_the_exec() {
    // sh forks a process that inherits stdout and outlives it. The exec must
    // still return an exit frame promptly instead of blocking on pipe EOF.
    let frames = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","sleep 30 & exit 0"]}"#),
    )
    .await
    .expect("exec wedged on a daemonizing child");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn kill_of_an_exited_process_is_a_noop_success() {
    let state = Arc::new(State::new());
    let started = request_on(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","x"],"detach":true}"#,
    )
    .await;
    let pid = started[0]["pid"].as_u64().unwrap();
    // Wait for the child to be reaped (exit_code populated).
    for _ in 0..100 {
        let logs = request_on(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let killed = request_on(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(
        type_of(&killed[0]),
        "done",
        "kill of exited pid should be a no-op success"
    );
}

#[tokio::test]
async fn detached_exec_is_listed_then_logs_replay_output_and_exit() {
    let state = Arc::new(State::new());

    let started = request_on(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","detached-hi"],"detach":true}"#,
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    let pid = started[0]["pid"].as_u64().unwrap();

    // The child runs and exits asynchronously; poll ps until it is visible so
    // the test does not race the detached spawn.
    let mut listed = false;
    for _ in 0..50 {
        let ps = request_on(&state, r#"{"op":"ps"}"#).await;
        if ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid) && p["detached"] == true)
        {
            listed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(listed, "detached pid {pid} never appeared in ps");

    // logs replays the ring buffer; retry until the Exit chunk lands.
    for _ in 0..50 {
        let logs = request_on(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        let body: String = logs
            .iter()
            .filter(|f| type_of(f) == "stdout")
            .map(decode)
            .collect();
        if logs.iter().any(|f| type_of(f) == "exit") {
            assert!(
                body.contains("detached-hi"),
                "logs missing output: {body:?}"
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("logs never reported an exit for pid {pid}");
}
