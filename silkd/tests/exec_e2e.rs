//! exec/procs verb E2E over the in-memory duplex, spawning real processes so it runs on any Unix.

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use silkd::server::State;

use common::{
    b64, connect, decode, exchange, next_frame, one, roundtrip, send, stdout_body, type_of,
};

async fn detached_pid(state: &Arc<State>, script: &str) -> u64 {
    let argv = json!(["/bin/sh", "-c", script]);
    let started = one(
        state,
        &json!({"op":"exec","argv":argv,"detach":true}).to_string(),
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    started[0]["pid"].as_u64().unwrap()
}

async fn wait_for_exit(state: &Arc<State>, pid: u64) -> Vec<Value> {
    for _ in 0..250 {
        let logs = one(state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            return logs;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("pid {pid} never exited");
}

async fn wait_listed(state: &Arc<State>, pid: u64) -> Value {
    for _ in 0..250 {
        let ps = one(state, r#"{"op":"ps"}"#).await;
        if let Some(p) = ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["pid"].as_u64() == Some(pid))
        {
            return p.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("pid {pid} never appeared in ps");
}

#[tokio::test]
async fn exec_streams_stdout_then_exit() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/echo","-n","hello"]}"#).await;
    assert_eq!(type_of(&frames[0]), "started");
    assert_eq!(stdout_body(&frames), "hello");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn large_output_is_delivered_without_loss() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","seq 1 20000"]}"#).await;
    let body = stdout_body(&frames);
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(
        lines.len(),
        20000,
        "expected 20000 lines, got {}",
        lines.len()
    );
    assert_eq!(lines.first().copied(), Some("1"));
    assert_eq!(lines.last().copied(), Some("20000"));
    assert_eq!(type_of(frames.last().unwrap()), "exit");
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
    assert_eq!(frames[0]["proto"], 2);
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
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","sleep 30 & exit 0"]}"#),
    )
    .await
    .expect("exec wedged on a daemonizing child");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn daemonizer_exit_is_the_last_frame() {
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","(sleep 5; echo late) & exit 0"]}"#),
    )
    .await
    .expect("exec wedged on a daemonizing child");
    assert_eq!(
        type_of(frames.last().unwrap()),
        "exit",
        "exit must be last: {frames:?}"
    );
    assert!(
        !frames
            .iter()
            .filter(|f| type_of(f) == "stdout")
            .any(|f| String::from_utf8_lossy(&decode(f)).contains("late")),
        "grandchild output must not leak after exit"
    );
}

#[tokio::test]
async fn attach_streams_live_output_then_exit() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 0.3; echo live-chunk").await;
    let frames = tokio::time::timeout(
        Duration::from_secs(8),
        one(&state, &format!(r#"{{"op":"attach","pid":{pid}}}"#)),
    )
    .await
    .expect("attach hung");
    assert!(
        stdout_body(&frames).contains("live-chunk"),
        "attach missed live output: {frames:?}"
    );
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn attach_to_exited_process_returns_exit_immediately() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo done-fast").await;
    wait_for_exit(&state, pid).await;
    let frames = tokio::time::timeout(
        Duration::from_secs(3),
        one(&state, &format!(r#"{{"op":"attach","pid":{pid}}}"#)),
    )
    .await
    .expect("attach to an exited process must not hang");
    assert!(stdout_body(&frames).contains("done-fast"));
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn attach_unknown_pid_is_not_found() {
    let frames = roundtrip(r#"{"op":"attach","pid":999999}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "not_found");
}

#[tokio::test]
async fn kill_actually_terminates_a_live_process() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "sleep 30").await;
    wait_listed(&state, pid).await;
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(type_of(&killed[0]), "done");
    wait_for_exit(&state, pid).await;
}

#[tokio::test]
async fn exec_unknown_user_is_rejected() {
    let frames =
        roundtrip(r#"{"op":"exec","argv":["true"],"user":"definitely-not-a-user-xyz"}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn exec_missing_binary_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/no/such/binary-xyz"]}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
    assert!(frames[0]["message"].as_str().unwrap().contains("spawn"));
}

#[tokio::test]
async fn exec_missing_cwd_is_a_bad_request() {
    let frames = roundtrip(r#"{"op":"exec","argv":["true"],"cwd":"/no/such/dir-xyz"}"#).await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
}

#[tokio::test]
async fn exec_non_executable_file_is_a_bad_request() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let state = Arc::new(State::new());
    let frames = one(
        &state,
        &json!({"op":"exec","argv":[file.path()]}).to_string(),
    )
    .await;
    assert_eq!(type_of(&frames[0]), "error");
    assert_eq!(frames[0]["kind"], "bad_request");
    assert!(frames[0]["message"].as_str().unwrap().contains("spawn"));
}

#[tokio::test]
async fn exec_stdin_is_piped_to_the_child() {
    let frames = exchange(&[
        r#"{"op":"exec","argv":["/bin/cat"]}"#.to_string(),
        json!({"op":"stdin","data":b64(b"piped-in\n")}).to_string(),
        r#"{"op":"stdin_close"}"#.to_string(),
    ])
    .await;
    assert_eq!(stdout_body(&frames), "piped-in\n");
    assert_eq!(type_of(frames.last().unwrap()), "exit");
}

#[tokio::test]
async fn kill_of_an_exited_process_is_a_noop_success() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo x").await;
    wait_for_exit(&state, pid).await;
    let killed = one(&state, &format!(r#"{{"op":"kill","pid":{pid}}}"#)).await;
    assert_eq!(
        type_of(&killed[0]),
        "done",
        "kill of exited pid should be a no-op success"
    );
}

#[tokio::test]
async fn detached_exec_is_listed_then_logs_replay_output_and_exit() {
    let state = Arc::new(State::new());
    let pid = detached_pid(&state, "echo detached-hi").await;
    let listed = wait_listed(&state, pid).await;
    assert_eq!(listed["detached"], true, "{listed:?}");
    let logs = wait_for_exit(&state, pid).await;
    assert_eq!(stdout_body(&logs), "detached-hi\n");
}

#[tokio::test]
async fn disconnect_during_drain_publishes_real_exit_code() {
    let state = Arc::new(State::new());
    let (mut cw, mut lines, _) = connect(&state);
    send(
        &mut cw,
        json!({
            "op": "exec",
            "argv": ["/bin/sh", "-c", "{ sleep 0.5; echo late; sleep 30; } & exit 7"]
        }),
    )
    .await;

    let started = next_frame(&mut lines).await;
    assert_eq!(type_of(&started), "started");
    let pid = started["pid"].as_u64().unwrap();

    let st = Arc::clone(&state);
    let attach =
        tokio::spawn(async move { one(&st, &json!({"op":"attach","pid":pid}).to_string()).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(lines);
    drop(cw);

    let frames = tokio::time::timeout(Duration::from_secs(8), attach)
        .await
        .expect("attach wedged")
        .unwrap();
    let last = frames.last().unwrap();
    assert_eq!(
        type_of(last),
        "exit",
        "attach must end with exit: {frames:?}"
    );
    assert_eq!(last["code"], 7, "real exit code, not a fabricated -1");
}
