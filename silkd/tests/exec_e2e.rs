//! exec/procs verb E2E over the in-memory duplex. Spawns real processes
//! (echo, sh), so it runs on any Unix — keep it that way so exec-path bugs
//! surface on the dev host, not only on Linux CI.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{decode, one, roundtrip, type_of};
use silkd::server::State;

fn body_of(frames: &[serde_json::Value]) -> String {
    let bytes: Vec<u8> = frames
        .iter()
        .filter(|f| type_of(f) == "stdout")
        .flat_map(decode)
        .collect();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn exec_streams_stdout_then_exit() {
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/echo","-n","hello"]}"#).await;
    assert_eq!(type_of(&frames[0]), "started");
    assert_eq!(body_of(&frames), "hello");
    let last = frames.last().unwrap();
    assert_eq!(type_of(last), "exit");
    assert_eq!(last["code"], 0);
}

#[tokio::test]
async fn large_output_is_delivered_without_loss() {
    // Exercises the backpressured foreground mpsc with far more chunks than
    // its buffer; every line must arrive in order, none dropped.
    let frames = roundtrip(r#"{"op":"exec","argv":["/bin/sh","-c","seq 1 20000"]}"#).await;
    let body = body_of(&frames);
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
    // A child that leaves a grandchild holding stdout must still end its frame
    // stream with exit and nothing after it — the post-exit drain aborts the
    // pump, so no stray stdout can arrive once Exit is sent.
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
async fn kill_of_an_exited_process_is_a_noop_success() {
    let state = Arc::new(State::new());
    let started = one(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","x"],"detach":true}"#,
    )
    .await;
    let pid = started[0]["pid"].as_u64().unwrap();
    for _ in 0..100 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    let started = one(
        &state,
        r#"{"op":"exec","argv":["/bin/echo","detached-hi"],"detach":true}"#,
    )
    .await;
    assert_eq!(type_of(&started[0]), "started");
    let pid = started[0]["pid"].as_u64().unwrap();

    let mut listed = false;
    for _ in 0..50 {
        let ps = one(&state, r#"{"op":"ps"}"#).await;
        if ps[0]["procs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pid"].as_u64() == Some(pid) && p["detached"] == true)
        {
            listed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(listed, "detached pid {pid} never appeared in ps");

    for _ in 0..50 {
        let logs = one(&state, &format!(r#"{{"op":"logs","pid":{pid}}}"#)).await;
        if logs.iter().any(|f| type_of(f) == "exit") {
            assert_eq!(body_of(&logs), "detached-hi\n");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("logs never reported an exit for pid {pid}");
}
